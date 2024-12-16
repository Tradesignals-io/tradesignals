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

import json
import logging
import os
import traceback
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Dict, Optional, Union

from dotenv import load_dotenv
from tornado.websocket import WebSocketClosedError, websocket_connect

import pipelines.schemas as avro_schemas  # type: ignore
from core.utils import get_pyproject_root  # type: ignore
from pipelines.kafka.producer import AvroProducer  # type: ignore

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

project_root = get_pyproject_root()
print(f"project_root: {project_root}")
log_file_path = os.path.join(project_root, "logs", f"option_trades-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.log")
print(f"log_file_path: {log_file_path}")
logger.addHandler(logging.FileHandler(log_file_path))

load_dotenv()

def calc_bull_bear_score(price: Decimal, bid: Decimal, ask: Decimal, bias: str) -> Decimal:
    """
    Calculate the bull-bear score based on the trade price relative to the bid and ask prices.
    The score is a sigmoid function where -1 indicates below the bid, 0 indicates the middle,
    and 1 indicates above the ask. The score increases as the price gets closer to the ask
    and decreases as the price gets closer to the bid.

    Args:
        price : Decimal
            The trade price.
        bid : Decimal
            The best bid price.
        ask : Decimal
            The best ask price.
        bias : str
            The sentiment bias of the trade.

    Returns:
        Decimal
            The bull-bear score.

    Examples:
        >>> calc_bull_bear_score(Decimal("105.0"), Decimal("100.0"), Decimal("110.0"), 'B')
        Decimal(0.5)
        >>> calc_bull_bear_score(Decimal("95.0"), Decimal("100.0"), Decimal("110.0"), 'R')
        Decimal(-1.0)
        >>> calc_bull_bear_score(Decimal("115.0"), Decimal("100.0"), Decimal("110.0"), 'B')
        Decimal(1.0)
    """
    if price < bid:
        return Decimal("-1.0")
    elif price > ask:
        return Decimal("1.0")
    else:
        mid = (bid + ask) / 2
        range_ = ask - bid
        if range_ == 0:
            return Decimal("0.0")
        normalized_price = (price - mid) / (range_ / 2)
        exp_value = Decimal("2.718281828459045") ** -Decimal(normalized_price)
        score = 2 / (1 + exp_value) - 1
        score = abs(score) if bias != "R" else -abs(score)
    return Decimal(str(score))


def add_above_below_tag(price: Decimal, bid: Decimal, ask: Decimal) -> Optional[str]:
    """
    Check if the price is above the ask or below the bid.

    Args:
        price : Decimal
            The trade price.
        bid : Decimal
            The best bid price.
        ask : Decimal
            The best ask price.

    Returns:
        Optional[str]
            "ABOVE_ASK" if price is above ask, "BELOW_BID" if price is below bid, otherwise None.
    """
    if price > ask:
        return "ABOVE_ASK"
    elif price < bid:
        return "BELOW_BID"
    return None


class OptionTradesPipeline(AvroProducer):
    """
    External Source for the UnusualWhales Options Websocket API.
    """

    def __init__(
        self,
        topic: str,
        debug: bool = True,
        schema_str: str | None = None,
        on_delivery: Callable[[Any], None] | None = None,
        auto_register_schemas: bool = True,
        **kwargs
    ):
        """
        Initialize the pipeline with a topic name.

        Args:
            topic : str
                The topic to produce to.
            debug : bool
                Enable debug mode. Default is False.
            schema_str : str
                The schema to use for the producer.
        """
        super().__init__(topic, debug, schema_str, on_delivery, **kwargs)
        self.schema_str = schema_str

    def extract_value(self, data: Dict[str, Any], msg_id: Optional[Union[str, int]]) -> Dict[str, Any]:
        """
        Map option data to a dictionary following the AVRO schema.

        Args:
            data : Dict[Any, Any]
                The data to map.

        Returns:
            dict
                The mapped data.

        Raises:
            ValueError
                If any of the required fields are missing or invalid.
        """
        try:
            tags = [tag.upper() for tag in data.get("tags", [])] \
                    + [tag.upper() for tag in data.get("report_flags", [])]
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
            price = Decimal(data.get("price", "0"))
            bid = Decimal(data.get("nbbo_bid", "0"))
            ask = Decimal(data.get("nbbo_ask", "0"))
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
                "timestamp": executed_ts,  # Timestamp in milliseconds since epoch
                "symbol": str(data.get("option_symbol", "")),
                "underlying": str(data.get("underlying_symbol", "")),
                "spot": Decimal(data.get("underlying_price", "0")),
                "strike": Decimal(data.get("strike", "0")),
                "exp_date": (datetime.fromisoformat(data.get("expiry", "1970-01-01")).date() - date(1970, 1, 1)).days,
                "dte": int(days_to_expiry),
                "put_call": str(data.get("option_type", "C"))[0].upper(),
                "qty": int(data.get("size", 0) or 0),
                "price": price or Decimal("0.00000001"),
                "premium": Decimal(data.get("premium", "0.00000001")),
                "side": side,
                "bias": bias,
                "score": score or Decimal("0.00000001"),
                "venue": str(data.get("exchange", "")),
                "trade_code": str(data.get("trade_code", "")),
                "iv": Decimal(data.get("implied_volatility", "0.00000001")),
                "bid_px": bid,
                "ask_px": ask,
                "theo": Decimal(data.get("theo", "0.00000001")),
                "delta": Decimal(data.get("delta", "0.00000001")),
                "gamma": Decimal(data.get("gamma", "0.00000001")),
                "vega": Decimal(data.get("vega", "0.00000001")),
                "theta": Decimal(data.get("theta", "0.00000001")),
                "rho": Decimal(data.get("rho", "0.00000001")),
                "vol_ask": int(data.get("ask_vol", 0)),
                "vol_bid": int(data.get("bid_vol", 0)),
                "vol_unk": int(data.get("no_side_vol", 0)),
                "vol_mid": int(data.get("mid_vol", 0)),
                "vol_leg": int(data.get("multi_vol", 0)),
                "vol_stk": int(data.get("stock_multi_vol", 0)),
                "vol": int(data.get("volume", 0) or 0),
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

    def set_message_value(self, data: Union[Dict[str, Any], Any], message_id: Union[int, str, None]) -> Dict[str, Any]:
        return self.extract_value(data, message_id)

    def set_message_key(self, data: Union[Dict[str, Any], Any], message_id: Union[int, str, None]) -> Union[int, str, None]:
        return self.extract_key(data, message_id)

    def set_message_timestamp(self, data: Union[Dict[str, Any], Any]) -> Union[int, None]:
        return self.extract_ts(data)

    def set_message_headers(self, data: Union[Dict[str, Any], Any], message_id: Union[int, str, None]) -> Union[Dict[str, Any], None]:
        return self.extract_headers(data, message_id)

def ack(msg_id: Optional[Union[str, int]]) -> None:
    logger.info("Message %s delivered", msg_id)


def run():
    """Run the pipeline."""

    from tornado.ioloop import IOLoop
    from tornado.platform.asyncio import AsyncIOMainLoop

    logger.info("Starting pipeline...")

    AsyncIOMainLoop().make_current()
    pipeline = OptionTradesPipeline(
        topic="option_trades",
        schema_str=getattr(avro_schemas, "option_trades_value"),
        on_delivery=ack,
        debug=True,
        source_id="unusualwhales",
        dataset_id="websocket.option_trades",
    )
    IOLoop.current().run_sync(pipeline.start)

if __name__ == "__main__":
    run()


__all__ = ["OptionTradesPipeline"]
