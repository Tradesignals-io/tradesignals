from decimal import Decimal
from typing import Any, Dict

import databento as db
from dotenv import load_dotenv

import pipelines.schemas as avro_schemas
from pipelines.kafka.x import DataPipeline

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


class EquityTradesPipeline(DataPipeline):
    """Equity TBBO producer pipeline."""

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize the EquityTradesPipeline class.

        Args:
            config : Dict[str, Any]
                Configuration dictionary for the job.
        """
        super().__init__(config)
        self.replay_last24 = config.get('replay_last24', True)
        self.client = None

    def get_key(self, data: db.MBP1Msg, message_id: int | str | None) -> str:
        """Get the key for the message.

        Args:
            data : db.MBP1Msg
                The message data.
            message_id : int | str | None
                The message unique identifier.

        Returns:
            str
                The key for the message.
        """
        return f"{data.hd.publisher_id}.{data.hd.instrument_id}.{message_id}"

    def get_timestamp(self, data: db.MBP1Msg) -> int:
        """Get the timestamp for the message.

        Args:
            data : db.MBP1Msg
                The message data.

        Returns:
            int
                The timestamp for the message.
        """
        return data.hd.ts_event // 1_000_000

    def get_value(self, data: db.MBP1Msg, message_id: int | str | None) -> Dict[str, Any]:
        """Extract the message value from a MBP1Msg data.

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
            if ts_event is None:
                raise ValueError("ts_event is None")
            ts_out = data.hd.ts_out // 1_000_000
            if ts_out is None:
                raise ValueError("ts_out is None")
            ts_recv = data.hd.ts_recv // 1_000_000
            if ts_recv is None:
                raise ValueError("ts_recv is None")

            message_value = {
                "received_ts": ts_recv,
                "out_ts": ts_out,
                "timestamp": ts_event,
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

            # Produce the record to Kafka
            avro_bytes = self.serializer.encode_record_with_schema(
                self.schema_subject,
                avro_schemas.EquityTrade,
                message_value
            )
            self.produce_record(avro_bytes)

            return message_value
        except Exception as e:
            logger.error("Error extracting value: %s", e, exc_info=True)
            raise

    def produce_record(self, avro_bytes: bytes) -> None:
        """
        Produce the record to Kafka.

        Args:
            avro_bytes : bytes
                The serialized record in Avro format.
        """
        if self.pipeline_manager and self.pipeline_manager.producer:
            self.producer.produce(
                self.pipeline_manager.output_topic,
                value=avro_bytes
            )
            self.producer.poll(0)

    async def run_after_lazy_load(self) -> None:
        """Lazy initialize the message serializer."""
        self.pipeline_manager = PipelineManager(
            output_topic=self.config['output_topic'],
            debug=self.config.get('debug', False)
        )
        self.producer = self.pipeline_manager.producer
        self.schema_registry_client = self.pipeline_manager.schema_registry_client
        self.serializer = self.pipeline_manager.message_serializer
        self.pipeline_manager.create_topics_if_not_exist([self.config['output_topic']])
        if not self.avro_schema:
            self.avro_schema = self.pipeline_manager.producer_value_schema_str
        try:
            # register schema
            self.schema_registry_client.register_schema(
                subject_name=self.schema_subject,
                schema=self.avro_schema,
                normalize_schemas=True
            )
            logger.info("Schema registered successfully!")
        except Exception as e:
            logger.error("Error registering schema: %s", e, exc_info=True)
            raise SchemaRegistryError(
                http_status_code=400, error_code=-1, error_message="Error registering schema"
            )
        self.running = True
        self.initialized = True

