# flake8: noqa
"""
Kafka Producer with Avro schema support.
"""
import asyncio
import json
import logging
import os
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from threading import Lock
from typing import Any, Dict, TypeVar

from tradesignals.common.types.extended_types import TradePriceT, TradeAmountT, TradeTimestampT
from tradesignals.common.types.core import DecimalT, TimestampT
from decimal import Decimal
from datetime import datetime, date, timezone

import requests
from confluent_kafka import Producer
from confluent_kafka.avro.cached_schema_registry_client import \
    CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import \
    MessageSerializer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

class AvroProducer(ABC):
    """
    Abstract base class for Kafka producers with Avro schema support.
    This abstract class provides a framework for producing Avro-encoded messages to Kafka.
    It assumes that the schema registry uses basic authentication (i.e., <username>:<password>).

    Environment Variables:
    - SCHEMA_REGISTRY_URL: URL of the schema registry.
    - SCHEMA_REGISTRY_AUTH_INFO: Authentication information for the schema registry.
    - KAFKA_BOOTSTRAP_SERVERS: Kafka bootstrap servers.
    - KAFKA_SASL_MECHANISM: SASL mechanism for Kafka.
    - KAFKA_SECURITY_PROTOCOL: Security protocol for Kafka.
    - KAFKA_SASL_USERNAME: SASL username for Kafka.
    - KAFKA_SASL_PASSWORD: SASL password for Kafka.

    Examples
    --------
    >>> producer = AvroProducer("example_topic", debug=True)
    >>> await producer.start()
    >>> await producer.stop()
    >>> await producer.close()
    >>> await producer.produce_message(record)
    >>> await producer.flush()

    """

    def __init__(self, topic: str, debug: bool = False) -> None:
        """
        Initialize the Kafka producer with Avro schema support.

        Parameters
        ----------
        topic : str
            The Kafka topic to produce to.
        debug : bool
            Enable debug logging.
        """
        logger.info("************************************************************")
        logger.info("Initializing Avro producer for topic: %s with debug: %s", topic, debug)
        logger.info("************************************************************")
        self.running = False
        self.messages_produced = 0
        self.last_log_time = time.time()
        self.topic = topic
        self.schema_subject = f"{self.topic}-value"
        self.debug = debug
        self.avro_schema = None
        self.producer = None  # Lazy initialization
        self.schema_registry_client = None
        self.serializer = None
        self.initialized = False
        if self.debug:
            logger.info("Setting the log level to debug. Schema registry client will not be initialized.")
            logger.setLevel(logging.DEBUG)


    async def run_after_lazy_load(self) -> None:
        """
        Lazy initialize the message serializer.
        """

        producer_config = {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "sasl.mechanism": os.environ["KAFKA_SASL_MECHANISM"],
            "security.protocol": os.environ["KAFKA_SECURITY_PROTOCOL"],
            "sasl.username": os.environ["KAFKA_SASL_USERNAME"],
            "sasl.password": os.environ["KAFKA_SASL_PASSWORD"],
            "acks": "1",
            "queue.buffering.max.messages": 1000000,
            "client.id": f"{self.topic}-producer"
        }
        if self.debug:
            producer_config["debug"] = "all"
        self.producer = Producer(producer_config)
        self.schema_registry_client = CachedSchemaRegistryClient({
            "url": os.environ["SCHEMA_REGISTRY_URL"],
            "basic.auth.user.info": os.environ["SCHEMA_REGISTRY_AUTH_INFO"],
            "basic.auth.credentials.source": "USER_INFO",
            "auto.register.schemas": False
        })
        self.serializer = MessageSerializer(self.schema_registry_client)
        self.avro_schema = self.get_latest_schema()
        self.running = True
        self.initialized = True


    def decimal_to_bytes(self, value: Decimal | DecimalT, precision: int = 26, scale: int = 4) -> bytes:
        """
        Convert a Python Decimal to Avro's binary format for the decimal logical type.

        Parameters
        ----------
        value : Decimal | DecimalT
            Decimal value to encode.
        precision : int
            Maximum number of digits in the decimal.
        scale : int
            Number of digits to the right of the decimal point.

        Returns
        -------
        bytes
            Encoded bytes.
        """
        if isinstance(value, DecimalT):
            value = value.to_decimal()
        # Scale the decimal value
        scaled_value = int(value * (10**scale))

        # Calculate the minimum number of bytes needed for the value
        num_bytes = (scaled_value.bit_length() + 8) // 8  # Convert bits to bytes, rounded up
        # Encode the value in big-endian two's-complement
        encoded = scaled_value.to_bytes(num_bytes, byteorder="big", signed=True)
        return encoded


    def bytes_to_decimal(self, encoded: bytes, scale: int) -> Decimal:
        """
        Decode Avro's binary format for the decimal logical type to a Python Decimal.

        Parameters
        ----------
        encoded : bytes
            Encoded bytes from Avro.
        scale : int
            Number of digits to the right of the decimal point.

        Returns
        -------
        Decimal
            Decoded Decimal value.
        """
        # Decode the value from big-endian two's-complement
        scaled_value = int.from_bytes(encoded, byteorder="big", signed=True)

        # Convert back to decimal by applying the scale
        return Decimal(scaled_value) / (10**scale)

    def get_latest_schema(self) -> str | None:
        """
        Retrieve the latest Avro schema for the Kafka topic.

        Returns
        -------
        str
            The latest Avro schema as a string.
        """
        url = f"{os.environ['SCHEMA_REGISTRY_URL']}/subjects/{self.schema_subject}/versions/latest"
        auth = tuple(os.environ["SCHEMA_REGISTRY_AUTH_INFO"].split(":"))
        try:
            response = requests.get(url, auth=auth)
            response.raise_for_status()
            logger.info(
                "Latest schema for topic: %s",
                json.dumps(response.json(), sort_keys=True, indent=2)
            )
            return response.json()["schema"]
        except requests.exceptions.RequestException as e:
            logger.error("Error fetching the latest schema: %s", e, exc_info=True)

    async def produce_message(self, record: Dict[str, Any]) -> None:
        """
        Produce a message to Kafka.

        Parameters
        ----------
        record : Dict[str, Any]
            The record to produce.
        """
        try:
            if not self.initialized:
                logger.info("Initializing producer after lazy load")
                await self.run_after_lazy_load()

            logger.debug("Producing message to topic: %s", self.topic)
            msg_id = str(uuid.uuid4())
            value = self.extract_value(record, msg_id=msg_id)
            encoded_value = self.serializer.encode_record_with_schema(
                self.topic,
                self.avro_schema,
                value,
                is_key=False
            )
            headers = self.extract_headers(record, msg_id=msg_id) or {}
            encoded_headers = [(key, value.encode()) for key, value in headers.items()]
            key = self.extract_key(record, msg_id=msg_id)
            if key is None:
                key = msg_id
            ts = self.extract_ts(record)
            if ts is None:
                ts = int(datetime.now().timestamp() * 1000)
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=encoded_value,
                headers=encoded_headers,
                timestamp=ts,

            )
            self.producer.poll(0)
            self.messages_produced += 1
            if time.time() - self.last_log_time >= 5:
                logger.info("%s messages produced: %d", self.topic, self.messages_produced)
                self.last_log_time = time.time()
        except BufferError as e:
            logger.error("Error producing message: Local: Queue full: %s", e, exc_info=True)
            self.producer.poll(1)  # Wait for the queue to clear
        except ValueError as e:
            logger.error("Error producing message: Expecting value: %s", e, exc_info=True)
            self.producer.poll(1)
        except Exception as e:
            logger.error("Error producing message: %s", e, exc_info=True)
            self.producer.poll(1)

    @abstractmethod
    def extract_value(self, data: Dict[str, Any] | Any, msg_id: int | str | None) -> Dict[str, Any]:
        """
        Abstract method to map incoming data to the desired format.

        Parameters
        ----------
        data : Dict[str, Any] | Any
            The incoming data.
        msg_id : str | int | None, optional
            The message ID, by default None

        Returns
        -------
        Dict[str, Any]
            The mapped data.
        """
        pass

    @abstractmethod
    def extract_key(self, data: Dict[str, Any] | Any, msg_id: int | str | None) -> int | str | None:
        """
        Abstract method to extract the message key.

        Parameters
        ----------
        data : Dict[str, Any] | Any
            The incoming data.
        msg_id : str | int | None, optional
            The message ID, by default None

        Returns
        -------
        str | int | None
            The message key.
        """
        pass

    @abstractmethod
    def extract_ts(self, data: Dict[str, Any] | Any) -> int | None:
        """
        Abstract method to extract the message timestamp.

        Parameters
        ----------
        data : Dict[str, Any] | Any
            The incoming data.

        Returns
        -------
        int | None
            The message timestamp.
        """
        pass

    @abstractmethod
    def extract_headers(self, data: Dict[str, Any] | Any, msg_id: int | str | None) -> Dict[str, Any] | None:
        """
        Abstract method to set the message headers.

        Parameters
        ----------
        data : Dict[str, Any] | Any
            The incoming data.
        msg_id : str | int | None, optional
            The message ID, by default None

        Returns
        -------
        Dict[str, Any] | None
            The message headers.
        """
        pass

    @abstractmethod
    async def run(self) -> None:
        """
        Abstract method to run the producer.
        This method should contain the logic to establish and maintain a websocket connection.

        Returns
        -------
        None
        """
        pass

    async def start(self) -> None:
        """
        Start the producer and attempt to run forever..

        Returns
        -------
        None
        """
        logger.info("Starting pipeline for the producer for %s", self.topic)
        self.running = True
        while self.running:
            try:
                await self.run()
            except RuntimeError as e:
                if self.producer:
                    await self.producer.flush()
                if "can't register atexit after shutdown" in str(e):
                    logger.error("Error in run method: %s. Reconnecting...", e, exc_info=True)
                    await asyncio.sleep(5)  # wait before retrying
                elif "cannot schedule new futures after interpreter shutdown" in str(e):
                    logger.error("Unexpected error: %s. Reconnecting...", e, exc_info=True)
                    await asyncio.sleep(5)  # wait before retrying
                else:
                    if self.producer:
                        await self.producer.flush()
                    raise
            except Exception as e:
                if self.producer:
                    await self.producer.flush()
                logger.error("Error in run method: %s", e, exc_info=True)
                await asyncio.sleep(5)  # wait before retrying

    async def stop(self) -> None:
        """
        Stop the producer.

        Returns
        -------
        None
        """
        self.running = False
        if self.producer:
            if self.producer.buffer:
                await self.producer.buffer.flush()
            await self.producer.flush()
        self.producer.close()


__all__ = ["AvroProducer"]
