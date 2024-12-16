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
import pipelines.schemas as avro_schemas
from dotenv import load_dotenv
from pipelines.kafka.producer import AvroProducer  # type: ignore
from core import get_pyproject_root  # type: ignore

PUBLISHER_FROM_INT = {
    1: "GLBX.MDP3.GLBX",
    2: "XNAS.ITCH.XNAS",
    3: "XBOS.ITCH.XBOS",
    4: "XPSX.ITCH.XPSX",
    5: "BATS.PITCH.BATS",
    6: "BATY.PITCH.BATY",
    7: "EDGA.PITCH.EDGA",
    8: "EDGX.PITCH.EDGX",
    9: "XNYS.PILLAR.XNYS",
    10: "XCIS.PILLAR.XCIS",
    11: "XASE.PILLAR.XASE",
    12: "XCHI.PILLAR.XCHI",
    13: "XCIS.BBO.XCIS",
    14: "XCIS.TRADES.XCIS",
    15: "MEMX.MEMOIR.MEMX",
    16: "EPRL.DOM.EPRL",
    17: "FINN.NLS.FINN",
    18: "FINN.NLS.FINC",
    19: "FINY.TRADES.FINY",
    20: "OPRA.PILLAR.AMXO",
    21: "OPRA.PILLAR.XBOX",
    22: "OPRA.PILLAR.XCBO",
    23: "OPRA.PILLAR.EMLD",
    24: "OPRA.PILLAR.EDGO",
    25: "OPRA.PILLAR.GMNI",
    26: "OPRA.PILLAR.XISX",
    27: "OPRA.PILLAR.MCRY",
    28: "OPRA.PILLAR.XMIO",
    29: "OPRA.PILLAR.ARCO",
    30: "OPRA.PILLAR.OPRA",
    31: "OPRA.PILLAR.MPRL",
    32: "OPRA.PILLAR.XNDQ",
    33: "OPRA.PILLAR.XBXO",
    34: "OPRA.PILLAR.C2OX",
    35: "OPRA.PILLAR.XPHL",
    36: "OPRA.PILLAR.BATO",
    37: "OPRA.PILLAR.MXOP",
    38: "IEXG.TOPS.IEXG",
    39: "DBEQ.BASIC.XCHI",
    40: "DBEQ.BASIC.XCIS",
    41: "DBEQ.BASIC.IEXG",
    42: "DBEQ.BASIC.EPRL",
    43: "ARCX.PILLAR.ARCX",
    44: "XNYS.BBO.XNYS",
    45: "XNYS.TRADES.XNYS",
    46: "XNAS.QBBO.XNAS",
    47: "XNAS.NLS.XNAS",
    48: "DBEQ.PLUS.XCHI",
    49: "DBEQ.PLUS.XCIS",
    50: "DBEQ.PLUS.IEXG",
    51: "DBEQ.PLUS.EPRL",
    52: "DBEQ.PLUS.XNAS",
    53: "DBEQ.PLUS.XNYS",
    54: "DBEQ.PLUS.FINN",
    55: "DBEQ.PLUS.FINY",
    56: "DBEQ.PLUS.FINC",
    57: "IFEU.IMPACT.IFEU",
    58: "NDEX.IMPACT.NDEX",
    59: "DBEQ.BASIC.DBEQ",
    60: "DBEQ.PLUS.DBEQ",
    61: "OPRA.PILLAR.SPHR",
}


load_dotenv()


databento_logger = logging.getLogger("databento")
databento_logger.setLevel(level=logging.DEBUG)
databento_logger.addHandler(logging.StreamHandler())
databento_log_file_path = os.path.join(
    get_pyproject_root(),
    "logs/databento",
    f"databento-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.log"
)
databento_logger.addHandler(logging.FileHandler(databento_log_file_path))

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

log_file_path = os.path.join(get_pyproject_root(), "logs", f"equity_trades-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.log")
print(f"log_file_path: {log_file_path}")
logger.addHandler(logging.FileHandler(log_file_path))

from pipelines.kafka.producer import DeliveryGuarantee

class EquityTradesPipeline(AvroProducer):
    """Equity TBBO producer."""

    def on_delivery_cb(self, err: str | None, msg: Any) -> None:
        """Handle the delivery of a message.

        Args:
            err: str | None
                The error message.
            msg: Any
                The message.
        """
        if err:
            logger.error("Message delivery failed: %s", err)
        else:
            logger.info("Message delivered to %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset())

    def __init__(self, topic: str, debug: bool = True, replay_last24: bool = True) -> None:
        """
        Initialize the producer with topic and debug options.

        Args:
            topic : str
                Kafka topic to produce messages to.
            debug : bool
                Enable debug logging.
            replay_last24 : bool
                Replay data from the last day.

        Returns:
            None
        """
        print(f"EquityTradesPipeline.__init__")
        print(f"EquityTradesPipeline.__init___{avro_schemas.equity_tbbo_value =}")
        print(f"EquityTradesPipeline.__init___{topic =}")
        print(f"EquityTradesPipeline.__init___{debug =}")
        print(f"EquityTradesPipeline.__init___{replay_last24 =}")
        super().__init__(
            topic,
            debug,
            schema_str=avro_schemas.equity_tbbo_value,  # type: ignore
            on_delivery=self.on_delivery_cb,
            delivery_guarantee=DeliveryGuarantee.EXACTLY_ONCE
        )
        print(f"EquityTradesPipeline.__init__")
        self.replay_last24 = replay_last24
        self.client = None
        self.logger = logger
        self.debug = debug

    def set_message_value(self, data: db.MBP1Msg, message_id: int | str | None) -> Dict[str, Any]:
        """
        Extract the message value from a MBP1Msg data.

        Args:
            data : db.MBP1Msg
                The message data.
            message_id : int | str | None
                The message unique identifier.

        Returns:
            Dict[str, Any]
                The extracted message value.

        Raises:
            ValueError
                If the instrument_id does not resolve to a symbol.
                If the size is zero or negative.
                If the price is zero or negative.
                If the venue is not found.
                If the symbol is not found.
        """
        try:
            venue = PUBLISHER_FROM_INT[data.hd.publisher_id].split(".")[2]
            if venue is None:
                raise ValueError(f"No venue found for publisher_id: {data.hd.publisher_id}")

            symbol = self.client.symbology_map.get(data.hd.instrument_id, None)  # type: ignore
            if symbol is None:
                raise ValueError(f"No symbol found for instrument_id: {data.hd.instrument_id}")

            price_decimal = Decimal(str(data.price / 1_000_000_000))
            bid_decimal = Decimal(str(data.levels[0].bid_px / 1_000_000_000))
            ask_decimal = Decimal(str(data.levels[0].ask_px / 1_000_000_000))
            tags = []

            if price_decimal > ask_decimal:
                tags.append("ABOVE_ASK")
            if price_decimal < bid_decimal:
                tags.append("BELOW_BID")
            if data.price * data.size > Decimal('1000000'):
                tags.append("WHALE")


            ts_event = data.hd.ts_event // 1_000_000
            ts_out = data.hd.ts_out // 1_000_000
            ts_recv = data.hd.ts_recv // 1_000_000

            return {
                "received_ts": ts_recv,
                "out_ts": ts_out,
                "timestamp": ts_event,
                # "source_id": "databento",
                # "dataset_id": "DBEQ.BASIC",
                "symbol": symbol or "NONE",
                "qty": data.size,
                "price": price_decimal,
                "side": data.side,
                "venue": venue,
                "sequence": data.sequence,
                "flags": data.flags,
                "depth": data.depth,
                "bid_px_00": bid_decimal,
                "ask_px_00": ask_decimal,
                "bid_qty_00": data.levels[0].bid_sz,
                "ask_qty_00": data.levels[0].ask_sz,
                "bid_ct_00": data.levels[0].bid_ct,
                "ask_ct_00": data.levels[0].ask_ct,
                "tags": tags,
            }
        except Exception as e:
            logger.error("Error extracting value: %s", e, exc_info=True)
            return {}

    def set_message_timestamp(self, data: db.MBP1Msg) -> int:
        """
        Extract the timestamp from MBP1Msg data.

        Args:
            data : db.MBP1Msg
                The message data.

        Returns:
            int
                The extracted timestamp in milliseconds.
        """
        return data.hd.ts_event // 1_000_000

    def set_message_key(self, data: db.MBP1Msg, message_id: int | str | None) -> int | str | None:
        """
        Extract the message key from MBP1Msg data.

        Args:
            data : db.MBP1Msg
                The message data.
            message_id : int | str | None
                The message unique identifier.

        Returns:
            int | str | None
                The extracted message key.
        """
        return str(uuid.uuid4())

    def set_message_headers(self, data: db.MBP1Msg, message_id: int | str | None) -> Dict[str, Any] | None:
        """
        Extract the message headers from MBP1Msg data.

        Args:
            data : db.MBP1Msg
                The message data.
            message_id : int | str | None
                The message unique identifier.

        Returns:
            Dict[str, Any] | None
                The extracted message headers.
        """
        return None

    async def run(self) -> None:
        """Run the pipeline to process and produce messages.

        Returns:
            None
        """
        while True:
            try:
                logger.info("Starting client")
                self.client = db.Live(
                    key=os.environ["DATABENTO_API_KEY"],
                    ts_out=True
                )
                logger.info("Subscribing to DBEQ.BASIC")
                self.client.subscribe(  # type: ignore
                    dataset="DBEQ.BASIC",
                    schema="tbbo",
                    stype_in="raw_symbol",
                    symbols='ALL_SYMBOLS',
                    start=datetime.now(timezone.utc) - timedelta(hours=23, minutes=59, seconds=59) if self.replay_last24 else None
                )
                logger.info("Subscribed to DBEQ.BASIC")
                async for record in self.client:  # type: ignore
                    if isinstance(record, db.MBP1Msg):
                        logger.info("Received MBP1Msg")
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
                await self.client.wait_for_close()  # type: ignore
            except db.BentoError as e:
                logger.error("BentoError: %s", e, exc_info=True)
            except Exception as e:
                logger.error("Unexpected error: %s", e, exc_info=True)

def main() -> None:
    """
    Main function to run the EquityTradesPipeline.

    Returns:
        None
    """
    pipeline = EquityTradesPipeline(
        topic="equity_trades",
        debug=True,
        replay_last24=True
    )
    asyncio.run(pipeline.run())

__all__ = ["EquityTradesPipeline", "main"]

if __name__ == "__main__":
    main()
