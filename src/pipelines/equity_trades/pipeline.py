# flake8: noqa
"""Equity TBBO pipeline."""
import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict

import databento as db
from dotenv import load_dotenv
from numpy.random.tests import data

from tradesignals.common.calcs import calc_bull_bear_score, calc_trade_side
from tradesignals.common.kafka import AvroProducer
from tradesignals.common.kafka.avro_utils import decimal_to_bytes
from tradesignals.common.publishers import Venue
from tradesignals.common.types import DecimalT, TimestampNanos, TimestampT

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class EquityTradesPipeline(AvroProducer):
    """Equity TBBO producer."""

    def __init__(self, topic: str, debug: bool = False, replay_last24: bool = False) -> None:
        """
        Initialize the producer with topic and debug options.

        Parameters
        ----------
        topic : str
            Kafka topic to produce messages to.
        debug : bool
            Enable debug logging.
        replay_last24 : bool
            Replay data from the last day.

        Returns
        -------
        None

        """
        super().__init__(topic, debug)
        # @todo: remove this once confirmed that publisher_id is consistently mapping to venue
        # self._replay_last_day = replay_last24
        self._replay_last_day = True
        self.client = None

    @property
    def replay_last_day(self) -> bool:
        """Return the replay last day flag."""
        return self._replay_last_day

    def extract_value(self, data: Dict[str, Any] | Any, msg_id: int | str | None) -> Dict[str, Any]:
        """
        Extract the message value from a MBP1Msg data.

        Parameters
        ----------
        data : Dict[str, Any] | Any
            The message data.
        msg_id : int | str | None
            The message unique identifier.

        Returns
        -------
        Dict[str, Any]
            The extracted message value.

        Raises
        ------
        ValueError
            If the instrument_id does not resolve to a symbol.
            If the publisher_id does not resolve to a venue.
            If the size is zero or negative.
            If the price is zero or negative.
        """

        try:
            # @todo: remove this once confirmed that publisher_id is consistently mapping to venue
            # venue = self._publisher_dict.get(data.hd.publisher_id, "NONE")
            try:
                venue = Venue.from_int(data.hd.publisher_id)
            except ValueError as e:
                logger.error("Error extracting venue for publisher_id: %s", data.hd.publisher_id, exc_info=True)
                venue = "NONE"

            # default to no side
            trade_side = "N"
            # only calculate trade side if no side is provided
            if data.side == "N":
                trade_side = calc_trade_side(
                    DecimalT.from_int(data.levels[0].bid_px),
                    DecimalT.from_int(data.levels[0].ask_px),
                    DecimalT.from_int(data.price)
                )
            # default to no bias
            bias = "N"
            if trade_side == "B":
                bias = "R"
            elif trade_side == "A":
                bias = "B"

            # calculate bull bear score with bias
            bull_bear = calc_bull_bear_score(
                DecimalT.from_int(data.levels[0].bid_px),
                DecimalT.from_int(data.levels[0].ask_px),
                DecimalT.from_int(data.price),
                bias,
            )

            # adjust bull bear score if bias is bear
            if bias == "R":
                bull_bear = DecimalT(str(-abs(bull_bear.to_decimal())))
            else:
                bull_bear = DecimalT(str(abs(bull_bear.to_decimal())))

            raw_symbol = self.client.symbology_map.get(data.hd.instrument_id, None)

            if raw_symbol is None:
                raise ValueError(f"No symbol found for instrument_id: {data.hd.instrument_id}")

            symbol = raw_symbol.split(".")[0]

            if data.size <= 0:
                raise ValueError(f"Invalid size: {data.size}")
            if data.price <= 0:
                raise ValueError(f"Invalid price: {data.price}")

            # Convert decimal values to bytes for Avro decimal logical type
            price_decimal = DecimalT.from_int(data.price / 1_000_000_000).to_decimal()
            amount_decimal = DecimalT.from_int(data.price * data.size / 1_000_000_000).to_decimal()
            score_decimal = bull_bear.to_decimal()
            bid_decimal = DecimalT.from_int(data.levels[0].bid_px / 1_000_000_000).to_decimal()
            ask_decimal = DecimalT.from_int(data.levels[0].ask_px / 1_000_000_000).to_decimal()

            # Convert to bytes with proper scale and precision for Avro decimal logical type
            price_bytes = decimal_to_bytes(price_decimal, 18, 6)
            amount_bytes = decimal_to_bytes(amount_decimal, 38, 10)
            score_bytes = decimal_to_bytes(score_decimal, 8, 6)
            bid_bytes = decimal_to_bytes(bid_decimal, 18, 6)
            ask_bytes = decimal_to_bytes(ask_decimal, 18, 6)
            tags = []

            # Check if the trade price is above the ask price
            if price_decimal > ask_decimal:
                tags.append("ABOVE_ASK")

            # Check if the trade price is below the bid price
            if price_decimal < bid_decimal:
                tags.append("BELOW_BID")

            # Check if the trade amount exceeds 1,000,000 dollars
            if amount_decimal > Decimal('1000000'):
                tags.append("MILLIONAIRE")

            # Convert nanosecond timestamps to milliseconds for Flink compatibility
            ts_event = data.hd.ts_event // 1_000_000  # Convert nanos to millis

            # return the message value matching the tbbo_equity_trade schema
            return {
                "id": msg_id,  # Unique identifier for the trade
                "symbol": symbol,  # The symbol of the instrument
                "timestamp_ms": ts_event,  # Timestamp of trade/book update in nanos
                "qty": data.size,  # Trade quantity
                "price": price_bytes,  # Trade price as bytes with 4 decimal places
                "amount": amount_bytes,  # Trade amount as bytes with 4 decimal places
                "side": trade_side,  # Book side of trade (A=Ask, B=Bid, N=None)
                "bias": bias,  # Trade sentiment (B=Bullish, R=Bearish, N=None)
                "venue": venue,  # The name of the exchange
                "score": score_bytes,  # Trade quality score as bytes with 6 decimal places
                "bid": bid_bytes,  # Top bid price as bytes with 4 decimal places
                "ask": ask_bytes,  # Top ask price as bytes with 4 decimal places
                "bid_qty": data.levels[0].bid_sz,  # Top bid size
                "ask_qty": data.levels[0].ask_sz,  # Top ask size
                "bid_ct": data.levels[0].bid_ct,  # Number of bid orders at top
                "ask_ct": data.levels[0].ask_ct,  # Number of ask orders at top
                "tags": [],  # List of flags for the trade
            }
        except Exception as e:
            logger.error("Error extracting value: %s", e, exc_info=True)
            return {}

    def extract_ts(self, data: Dict[str, Any] | Any) -> int:
        """
        Extract the timestamp from MBP1Msg data.

        Parameters
        ----------
        data : Dict[str, Any] | Any
            The message data.

        Returns
        -------
        int
            The extracted timestamp in milliseconds.
        """
        return TimestampNanos.from_int(data.hd.ts_event).to_timestamp_millis()

    def extract_key(self, data: Dict[str, Any] | Any, msg_id: int | str | None) -> int | str | None:
        """
        Extract the message key from MBP1Msg data.

        Parameters
        ----------
        data : Dict[str, Any] | Any
            The message data.
        msg_id : int | str | None
            The message unique identifier.

        Returns
        -------
        int | str | None
            The extracted message key.
        """
        return str(uuid.uuid4())

    def extract_headers(self, data: Dict[str, Any] | Any, msg_id: int | str | None) -> Dict[str, Any] | None:
        """
        Extract the message headers from MBP1Msg data.

        Parameters
        ----------
        data : Dict[str, Any] | Any
            The message data.
        msg_id : int | str | None
            The message unique identifier.

        Returns
        -------
        Dict[str, Any] | None
            The extracted message headers.
        """
        return None

    async def run(self):
        """Run the pipeline to process and produce messages."""
        try:
            db.enable_logging(logging.DEBUG)
            self.client = db.Live(
                key=os.environ["DATABENTO_API_KEY"],
                ts_out=True
            )
            self.client.subscribe(
                dataset="DBEQ.BASIC",
                schema="tbbo",
                stype_in="raw_symbol",
                symbols='ALL_SYMBOLS',
                start=datetime.now(timezone.utc) - timedelta(hours=23, minutes=59, seconds=59) if self._replay_last_day else None
            )
            # start the subscription
            async for record in self.client:
                if isinstance(record, db.MBP1Msg):
                    if self.debug:
                        logger.debug("Received MBP1Msg: %s", record)
                    try:
                        await self.produce_message(record)
                    except Exception as e:
                        logger.error("Failed to deserialize Avro record: %s", e, exc_info=True)
                        if "Invalid int encoding" in str(e):
                            logger.error("Suppressed: Invalid int encoding")
                        elif "Malformed data. Length is negative: -1" in str(e):
                            logger.error("Suppressed: Malformed data. Length is negative: -1")
            await self.client.wait_for_close()
        except db.BentoError as e:
            logger.error("BentoError: %s", e, exc_info=True)
        except Exception as e:
            logger.error("Unexpected error: %s", e, exc_info=True)


__all__ = ["EquityTradesPipeline"]

if __name__ == "__main__":
    import asyncio
    pipeline = EquityTradesPipeline("equity_trades", debug=True, replay_last24=True)
    asyncio.run(pipeline.run())
