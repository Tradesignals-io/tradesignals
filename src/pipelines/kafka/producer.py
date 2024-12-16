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
from typing import Any, Dict, TypeVar, Union, Callable

from threading import Lock

from decimal import Decimal
from datetime import datetime, date, timezone


import requests

from confluent_kafka import Producer
from confluent_kafka.error import KafkaError
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.avro import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from pipelines.schemas import *
from pipelines.kafka.pipeline_manager import PipelineManager, PipelineSettings
from pipelines.kafka.avro_utils import make_avro_ready

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
from enum import IntEnum

class DeliveryGuarantee(IntEnum):
    """
    Enum for the producer delivery promise.
    """
    EXACTLY_ONCE = 1
    AT_LEAST_ONCE = 2


class AvroProducer(ABC):
    """
    Abstract base class for Kafka producers with Avro schema support.
    This abstract class provides a framework for producing Avro-encoded messages to Kafka.
    It assumes that the schema registry uses basic authentication (i.e., <username>:<password>).

    Required Environment Variables:
    - SCHEMA_REGISTRY_URL: URL of the schema registry.
    - SCHEMA_REGISTRY_AUTH_INFO: Authentication information for the schema registry.
    - BOOTSTRAP_SERVERS: Kafka bootstrap servers.
    - SASL_MECHANISM: SASL mechanism for Kafka.
    - SECURITY_PROTOCOL: Security protocol for Kafka.
    - SASL_USERNAME: SASL username for Kafka.
    - SASL_PASSWORD: SASL password for Kafka.

    """

    def __init__(
        self,
        topic: str,
        debug: bool = False,
        schema_str: str | None = None,
        on_delivery: Callable[[Any], None] | None = None,
        consumer_group_id: str = "default-consumer-group",
        source_id: str = "",
        dataset_id: str = "",
        **kwargs
    ) -> None:
        """Initialize the Kafka producer with optional schema for serialization.

            Args:
                topic : str
                    The Kafka topic to produce to.
                debug : bool
                    Enable debug logging.
                schema_str : str | None
                    The Avro schema as a string.
                on_delivery : Callable[[Any], None] | None
                    The callback function to call when the message is delivered.
                consumer_group_id : str
                    The consumer group ID.
                source_id : str
                    The source ID.
                dataset_id : str
                    The dataset ID.

        Examples:
            >>> producer = AvroProducer(
                topic="example_topic",
                on_delivery=acks(),
                consumer_group_id="consumer_group_x",
                source_id="provider_x",
                dataset_id="dataset_y",
                debug=True
            )
            >>> await producer.start()
            >>> await producer.stop()
            >>> await producer.close()
            >>> await producer.produce_message(record)
            >>> await producer.flush()

        """
        logger.info("************************************************************")
        logger.info(f"--    Initializing Producer for topic: {topic.upper()}   --")
        logger.info("************************************************************")
        self.running = False
        self.on_delivery = on_delivery
        self.source_id = source_id
        self.dataset_id = dataset_id
        self.last_log_time = time.time()
        self.consumer_group_id = consumer_group_id
        self.topic = topic
        self.schema_subject = f"{self.topic}-value"
        self.debug = debug
        self.avro_schema = schema_str
        self.producer = None  # Lazy initialization
        self.schema_registry_client = None  # Lazy initialization
        self.serializer = None  # Lazy initialization
        self.initialized = False  # Lazy initialization
        self.all_time_messages_produced = 0
        self.messages_produced = 0
        self.pipeline_manager = None
        if self.debug:
            logger.info("Setting the log level to debug. Schema registry client will not be initialized.")
            logger.setLevel(logging.DEBUG)

    async def run_after_lazy_load(self) -> None:
        """Lazy initialize the message serializer."""
        self.pipeline_manager = PipelineManager(
            output_topic=self.topic,
            debug=self.debug
        )
        self.producer = self.pipeline_manager.producer
        self.schema_registry_client = self.pipeline_manager.schema_registry_client
        self.serializer = self.pipeline_manager.message_serializer
        self.pipeline_manager.create_topics_if_not_exist([self.topic])
        if not self.avro_schema:
            self.avro_schema = self.pipeline_manager.producer_value_schema_str
        try:
            # register schema
            self.schema_registry_client.register_schema(
                subject_name=self.schema_subject,
                schema=json.loads(self.avro_schema),
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


    def register_schema(self) -> None:
        """
        Register the provided Avro schema string with the schema registry.

        Args:
            schema_str : str
                The Avro schema string to register.

        Raises:
            SchemaRegistryError
                If there is an error registering the schema.
            ValueError
                If the schema registry client is not initialized.
            ValueError
                If the Avro schema is not initialized.
        """
        if not self.schema_registry_client:
            raise ValueError("Schema registry client not initialized")
        if not self.avro_schema:
            raise ValueError("Avro schema not initialized")
        try:
            self.schema_registry_client.register_schema(
                subject_name=self.schema_subject,
                schema=self.avro_schema,
                normalize_schemas=True
            )
            logger.info("Schema registered successfully!")
        except Exception as e:
            logger.error("Error registering schema: %s", e, exc_info=True)
            raise SchemaRegistryError(
                http_status_code=400,
                error_code=-1,
                error_message="Error registering schema"
            )

    async def produce_message(
        self,
        record: Dict[str, Any]
    ) -> None:
        """
        Produce a message to Kafka.

        Args:
            record : Dict[str, Any]
                The record to produce.

        Raises:
            RuntimeError
                If the producer is not initialized, or the serializer is not initialized.
            BufferError
                If the local queue is full.
            ValueError
                If there is an error with the value.
        """
        try:
            if not self.initialized:
                logger.info("Initializing producer after lazy load")
                await self.run_after_lazy_load()
            logger.info("Producing message to topic: %s", self.topic)
            message_id = str(uuid.uuid4())
            value = self.set_message_value(record, message_id=message_id)
            key = self.set_message_key(record, message_id=message_id)
            headers = self.set_message_headers(record, message_id=message_id) or {}
            timestamp = self.set_message_timestamp(record)
            if key is None:
                key = message_id
            if timestamp is None:
                timestamp = int(datetime.now().timestamp() * 1000)
            record = self.pipeline_manager.encode_record(
                record_value=value,
                record_key=key,
                timestamp=timestamp,
                schema_version=1,
                source_id="",
                dataset_id="",
            )
            self.producer.produce(
                topic=self.topic,
                key=record.key,
                value=record.value,
                headers=record.headers,
                timestamp=record.timestamp,
                on_delivery=self.on_delivery,
            )
            self.producer.poll(0.01)
            self.messages_produced += 1
            if time.time() - self.last_log_time >= 15:
                with open('producer.log', 'a') as log_file:
                    log_file.write(f"{self.topic} messages produced in 15 seconds: {self.messages_produced}\n")
                self.last_log_time = time.time()
                self.all_time_messages_produced += self.messages_produced
                self.messages_produced = 0
        except BufferError as e:
            logger.error("Error producing message: Local: Queue full: %s", e, exc_info=True)
            self.producer.poll(1)  # Wait for the queue to clear
        except ValueError as e:
            logger.error("Error producing message: Expecting value: %s", e, exc_info=True)
            self.producer.poll(1)
        except Exception as e:
            logger.error("Error producing message: %s", e, exc_info=True)
            self.producer.poll(1)

    def serialize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Serialize the record using the Avro schema.
        """
        return self.pipeline_manager.encode_record(
            record_value=record,
            record_key=record.key,
            timestamp=record.timestamp,
            schema_version=1,
            source_id="",
            dataset_id="",
        )

    @abstractmethod
    def set_message_value(
        self,
        data: Union[Dict[str, Any], Any],
        message_id: Union[int, str, None]
    ) -> Dict[str, Any]:
        """
        Abstract method to map incoming data to the desired format.

        Parameters
        ----------
        data : Dict[str, Any] | Any
            The incoming data.
        message_id : str | int | None, optional
            The message ID, by default None

        Returns
        -------
        Dict[str, Any]
            The mapped data.
        """
        pass

    @abstractmethod
    def set_message_key(
        self,
        data: Union[Dict[str, Any], Any],
        message_id: Union[int, str, None]
    ) -> Union[int, str, None]:
        """
        Abstract method to extract the message key.

        Parameters
        ----------
        data : Dict[str, Any] | Any
            The incoming data.
        message_id : str | int | None, optional
            The message ID, by default None

        Returns
        -------
        str | int | None
            The message key.
        """
        pass

    @abstractmethod
    def set_message_timestamp(
        self,
        data: Union[Dict[str, Any], Any]
    ) -> Union[int, None]:
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
    def set_message_headers(
        self,
        data: Union[Dict[str, Any], Any],
        message_id: Union[int, str, None]
    ) -> Union[Dict[str, Any], None]:
        """
        Abstract method to set the message headers.

        Parameters
        ----------
        data : Dict[str, Any] | Any
            The incoming data.
        message_id : str | int | None, optional
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
        This method should contain the logic to establish and
        maintain a websocket connection.

        Returns
        -------
        None
        """
        pass

    async def start(self) -> None:
        """
        Start the producer and attempt to run forever.

        Returns
        -------
        None
        """
        logger.info("Starting pipeline for the producer for %s", self.topic)
        self.running = True
        while self.running:
            try:
                print(f"self.producer = {self.producer}")
                await self.run()
            except RuntimeError as e:
                if self.producer:
                    self.producer.flush()
                if "can't register atexit after shutdown" in str(e):
                    logger.error("Error in run method: %s. Reconnecting...", e, exc_info=True)
                    await asyncio.sleep(5)  # wait before retrying
                elif "cannot schedule new futures after interpreter shutdown" in str(e):
                    logger.error("Unexpected error: %s. Reconnecting...", e, exc_info=True)
                    await asyncio.sleep(5)  # wait before retrying
                else:
                    if self.producer:
                        self.producer.flush()
                    raise
            except Exception as e:
                if self.producer:
                    self.producer.flush()
                logger.error("Error in run method: %s", e, exc_info=True)
                await asyncio.sleep(5)  # wait before retrying

    async def stop(self) -> None:
        """Stop the producer."""
        self.running = False
        if self.producer:
            self.producer.flush()
        self.producer.close()
