# flake8: noqa: E501
"""
PIPELINE_ID: OPTION_TRADES_WITH_ALL_STATS
SCHEMA: OPTION_TRADES-VALUE (AVRO SCHEMA)
INPUT_SOURCE: 3RD PARTY DATA (UNUSUALWHALES.COM)
OUTPUT_TYPE: KAFKA_TOPIC
OUTPUT_NAME: OPTION_TRADES

This pipeline sources option trades data with
the full list of option metrics Tradesignals.io provides.
"""

import asyncio
import json
import logging
import os
import traceback
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional, Union

from dotenv import load_dotenv
from tornado.websocket import WebSocketClosedError, websocket_connect

from tradesignals.common.kafka.producer import AvroProducer
from tradesignals.common.types.core import DecimalT, TimestampT

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

load_dotenv()

def calc_bull_bear_score(price: DecimalT, bid: DecimalT, ask: DecimalT, bias: str) -> DecimalT:
    """
    Calculate the bull-bear score based on the trade price relative to the bid and ask prices.
    The score is a sigmoid function where -1 indicates below the bid, 0 indicates the middle,
    and 1 indicates above the ask. The score increases as the price gets closer to the ask
    and decreases as the price gets closer to the bid.

    Parameters
    ----------
    price : DecimalT
        The trade price.
    bid : DecimalT
        The best bid price.
    ask : DecimalT
        The best ask price.
    bias : str
        The sentiment bias of the trade.

    Returns
    -------
    DecimalT
        The bull-bear score.

    Examples
    --------
    >>> calc_bull_bear_score(DecimalT.from_str("105.0", 21, 9), DecimalT.from_str("100.0", 21, 9), DecimalT.from_str("110.0", 21, 9), 'B')
    DecimalT(0.5)
    >>> calc_bull_bear_score(DecimalT.from_str("95.0", 21, 9), DecimalT.from_str("100.0", 21, 9), DecimalT.from_str("110.0", 21, 9), 'R')
    DecimalT(-1.0)
    >>> calc_bull_bear_score(DecimalT.from_str("115.0", 21, 9), DecimalT.from_str("100.0", 21, 9), DecimalT.from_str("110.0", 21, 9), 'B')
    DecimalT(1.0)
    """
    if price.to_decimal() < bid.to_decimal():
        return DecimalT.from_str("-1.0", 10, 8)
    elif price.to_decimal() > ask.to_decimal():
        return DecimalT.from_str("1.0", 10, 8)
    else:
        mid = (bid.to_decimal() + ask.to_decimal()) / 2
        range_ = ask.to_decimal() - bid.to_decimal()
        if range_ == 0:
            return DecimalT.from_str("0.0", 10, 8)
        normalized_price = (price.to_decimal() - mid) / (range_ / 2)
        # Use Decimal for exponentiation to avoid TypeError
        exp_value = Decimal("2.718281828459045") ** -Decimal(normalized_price)
        score = 2 / (1 + exp_value) - 1
        score = abs(score) if bias != "R" else -abs(score)
    return DecimalT.from_str(str(score), 10, 8)


def add_above_below_tag(price: DecimalT, bid: DecimalT, ask: DecimalT) -> Optional[str]:
    """
    Check if the price is above the ask or below the bid.

    Parameters
    ----------
    price : DecimalT
        The trade price.
    bid : DecimalT
        The best bid price.
    ask : DecimalT
        The best ask price.

    Returns
    -------
    Optional[str]
        "ABOVE_ASK" if price is above ask, "BELOW_BID" if price is below bid, otherwise None.
    """
    if price.to_decimal() > ask.to_decimal():
        return "ABOVE_ASK"
    elif price.to_decimal() < bid.to_decimal():
        return "BELOW_BID"
    return None


class OptionTradesPipeline(AvroProducer):
    """
    External Source for the UnusualWhales Options Websocket API
    """

    def __init__(self, topic: str, debug: bool = False):
        """
        Initialize the pipeline with a topic name.

        Parameters
        ----------
        topic : str
            The topic to produce to.
        debug : bool
            Enable debug mode. Default is False.
        """
        super().__init__(topic, debug)

    def extract_value(self, data: Dict[str, Any], msg_id: Optional[Union[str, int]]) -> Dict[str, Any]:
        """
        Map option data to a dictionary following the AVRO schema.

        Parameters
        ----------
        data : Dict[Any, Any]
            The data to map.

        Returns
        -------
        dict
            The mapped data.

        Raises
        ------
        ValueError
            If any of the required fields are missing or invalid.
        """
        try:
            tags = [tag.upper() for tag in data.get("tags", [])] + [tag.upper() for tag in data.get("report_flags", [])]

            side = "N"
            if "ASK_SIDE" in tags:
                side = "A"
            elif "BID_SIDE" in tags:
                side = "B"

            bias = "N"
            if "BEARISH" in tags:
                bias = "R"
            elif "BULLISH" in tags:
                bias = "B"

            tags = [
                tag
                for tag in tags
                if tag not in ["BEARISH", "BULLISH", "NEUTRAL", "ASK_SIDE", "BID_SIDE", "NO_SIDE", "MID_SIDE"]
            ]

            if Decimal(data.get("premium", 0)) >= Decimal("1000000"):
                tags.append("MILLIONAIRE")

            price = DecimalT.from_str(data.get("price", "0"), 21, 9)
            bid = DecimalT.from_str(data.get("nbbo_bid", "0"), 21, 9)
            ask = DecimalT.from_str(data.get("nbbo_ask", "0"), 21, 9)

            tag = add_above_below_tag(price, bid, ask)
            if tag:
                tags.append(tag)

            executed_ts = int(data.get("executed_at", 0))
            todays_date = date.fromtimestamp(executed_ts / 1000)
            expiry_date = datetime.fromisoformat(data.get("expiry", "1800-01-01")).date()
            days_to_expiry = (expiry_date - todays_date).days

            score = calc_bull_bear_score(price, bid, ask, bias)

            result = {
                "id": str(data.get("id", "")),
                "timestamp_ms": executed_ts,  # Timestamp in milliseconds since epoch
                "symbol": str(data.get("option_symbol", "")),
                "underlying": str(data.get("underlying_symbol", "")),
                "spot": DecimalT.from_str(data.get("underlying_price", "0"), 21, 9).to_bytes(),
                "strike": DecimalT.from_str(data.get("strike", "0"), 21, 9).to_bytes(),
                "exp_date": (datetime.fromisoformat(data.get("expiry", "1800-01-01")).date() - date(1970, 1, 1)).days,
                "dte": int(days_to_expiry),
                "put_call": str(data.get("option_type", "C"))[0].upper(),
                "qty": int(data.get("size", 0) or 0),
                "price": price.to_bytes(),
                "premium": DecimalT.from_str(data.get("premium", "0"), 38, 9).to_bytes(),
                "side": side,
                "bias": bias,
                "score": score.to_bytes(),
                "venue": str(data.get("exchange", "")),
                "condition": str(data.get("trade_code", "")),
                "iv": DecimalT.from_str(data.get("implied_volatility", "0"), 18, 16).to_bytes(),
                "bid": bid.to_bytes(),
                "ask": ask.to_bytes(),
                "theo": DecimalT.from_str(data.get("theo", "0"), 21, 9).to_bytes(),
                "delta": DecimalT.from_str(data.get("delta", "0"), 18, 16).to_bytes(),
                "gamma": DecimalT.from_str(data.get("gamma", "0"), 18, 16).to_bytes(),
                "vega": DecimalT.from_str(data.get("vega", "0"), 18, 16).to_bytes(),
                "theta": DecimalT.from_str(data.get("theta", "0"), 18, 16).to_bytes(),
                "rho": DecimalT.from_str(data.get("rho", "0"), 18, 16).to_bytes(),
                "v_ask": int(data.get("ask_vol", 0)),
                "v_bid": int(data.get("bid_vol", 0)),
                "v_unk": int(data.get("no_side_vol", 0)),
                "v_mid": int(data.get("mid_vol", 0)),
                "v_leg": int(data.get("multi_vol", 0)),
                "v_stk": int(data.get("stock_multi_vol", 0)),
                "v": int(data.get("volume", 0) or 0),
                "oi": int(data.get("open_interest", 0) or 0),
                "tags": tags,
            }
            return result
        except Exception as e:
            logger.error("Error mapping fields at line %s: %s", traceback.extract_tb(e.__traceback__)[-1].lineno, e)
            raise ValueError(f"Error mapping fields: {e}")

    def extract_key(self, data: Dict[str, Any], msg_id: Optional[Union[str, int]]) -> Optional[Union[str, int]]:
        return data["option_symbol"]

    def extract_ts(self, data: Dict[str, Any]) -> Optional[int]:
        ts = data.get("executed_at")
        if ts and len(str(ts)) == 19:
            return ts // 1_000_000
        elif ts and len(str(ts)) == 13:
            return ts
        elif ts and len(str(ts)) == 10:
            return ts * 1_000
        return None

    def extract_headers(self, data: Dict[str, Any], msg_id: Optional[Union[str, int]]) -> Optional[Dict[str, Any]]:
        return None

    async def run(self) -> None:
        """
        Run the websocket connection and process messages.
        """
        while self.running:
            try:
                ws = await websocket_connect(os.environ["UNUSUALWHALES_WS_URI"])
                subscribe_msg = json.dumps(
                    {"channel": "option_trades", "msg_type": "join"}
                )
                await ws.write_message(subscribe_msg)
                while self.running:
                    msg = await ws.read_message()
                    if msg is None:
                        break
                    if self.debug:
                        logger.debug("Received message: %s", msg)
                    try:
                        data = json.loads(msg)
                        for item in data[1:]:
                            if item.get("price"):
                                await self.produce_message(item)
                    except json.JSONDecodeError as e:
                        logger.error(
                            "Error decoding JSON message at line %s: %s",
                            traceback.extract_tb(e.__traceback__)[-1].lineno,
                            e
                        )
                    except ValueError as e:
                        logger.error(
                            "Error mapping fields at line %s: %s",
                            traceback.extract_tb(e.__traceback__)[-1].lineno,
                            e
                        )
                    except Exception as e:
                        logger.error(
                            "Error processing message at line %s: %s",
                            traceback.extract_tb(e.__traceback__)[-1].lineno,
                            e
                        )
            except WebSocketClosedError as e:
                logger.error(
                    "Connection closed with error at line %s: %s. Reconnecting...",
                    traceback.extract_tb(e.__traceback__)[-1].lineno,
                    e
                )
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(
                    "Unexpected error at line %s: %s. Reconnecting...",
                    traceback.extract_tb(e.__traceback__)[-1].lineno,
                    e
                )
                await asyncio.sleep(5)

if __name__ == "__main__":
    from tornado.ioloop import IOLoop
    from tornado.platform.asyncio import AsyncIOMainLoop

    AsyncIOMainLoop().install()
    pipeline = OptionTradesPipeline("option_trades", debug=False)
    IOLoop.current().run_sync(pipeline.start)
