"""Manage kafka clients and topics."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro.serializer import MessageSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationError
from dotenv_derive import dotenv

import pipelines.schemas as pipelines_schemas  # type: ignore
from pipelines.kafka.types import KafkaRecord

# Set up logging
logger = logging.getLogger('PipelineManager-Logger')
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler('kafka.log')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.addHandler(file_handler)
logger.propagate = True
logger.info("***************************************************************")
logger.info("                Initializing PipelineManager...                ")
logger.info("***************************************************************")


# Load environment variables from a .env file
@dotenv(dotenv_file=".env", traverse=True, coerce_values=True)
class PipelineSettings:
    """Settings for the Kafka client with detailed exception handling.

    Raises:
        ValueError: If the configurations are not set properly.
    """

    bootstrap_servers: str
    sasl_mechanism: str
    security_protocol: str
    sasl_username: str
    sasl_password: str
    schema_registry_url: str
    schema_registry_auth_info: str
    developer_environment: bool
    consumer_group_id: str = 'default-consumer-group'
    auto_offset_reset: str = "earliest"
    debug: str = "none"

    def __init__(
        self,
        consumer_group_id: str = 'default-consumer-group',
        auto_offset_reset: str = "earliest",
        debug: str = "none"
    ) -> None:
        """Initialize the PipelineSettings class.

        Args:
            consumer_group_id: The consumer group ID.
            auto_offset_reset: The auto offset reset.
            debug: The debug level.
        """
        self.consumer_group_id = consumer_group_id
        self.auto_offset_reset = auto_offset_reset
        self.debug = debug

    @property
    def base_kafka_config(self) -> Dict[str, str]:
        """Base configuration dictionary.

        Returns:
            Dict[str, str]: Base configuration dictionary.
        """
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "sasl.mechanism": self.sasl_mechanism,
            "security.protocol": self.security_protocol,
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
        }

    @property
    def consumer_config(self) -> Dict[str, str]:
        """Consumer configuration dictionary.

        Returns:
            Dict[str, str]: Consumer configuration dictionary.
        """
        config = self.base_kafka_config
        config.update(self._consumer_config)

        config["group.id"] = self.consumer_group_id
        config["auto.offset.reset"] = self.auto_offset_reset
        config["debug"] = self.debug
        return config

    @property
    def producer_config(self) -> Dict[str, str]:
        """Kafka configuration dictionary.

        Returns:
            Dict[str, str]: Kafka configuration dictionary.
        """
        config = self.base_kafka_config
        config["debug"] = self.debug
        return config

    @property
    def schema_registry_config(self) -> Dict[str, str]:
        """Schema registry configuration dictionary.

        Returns:
            Dict[str, str]: Schema registry configuration dictionary.
        """
        return {
            "url": self.schema_registry_url,
            "basic.auth.user.info": self.schema_registry_auth_info
        }


class PipelineManager:
    """Kafka class to initialize Kafka clients."""

    def __init__(
        self,
        output_topic: str,
        input_topic: Optional[str] = None,
        consumer_group_id: str = "default-consumer-group",
        auto_offset_reset: str = "earliest",
        debug: bool = False,
        producer_config: Optional[Dict[str, Any]] = None,
        consumer_config: Optional[Dict[str, Any]] = None,
        schema_registry_config: Optional[Dict[str, Any]] = None,
        source_id: str = "",
        dataset_id: str = "",
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the PipelineManager class with detailed configurations.

        Raises:
            ValueError: If any configuration is invalid.
        """
        self.logger = logger or logging.getLogger('PipelineManager-Logger')
        self._debug = 'all' if debug else 'none'
        self._consumer_group_id = consumer_group_id
        self._auto_offset_reset = auto_offset_reset
        self._producer_config = producer_config or {}
        self._consumer_config = consumer_config or {}
        self._schema_registry_config = schema_registry_config or {}
        self._output_topic = output_topic
        self._input_topic = input_topic
        self._topics = [self._output_topic, self._input_topic] if self._input_topic else [self._output_topic]
        self._pipeline_settings = None
        self._admin = None
        self._schema_registry_client = None
        self._message_serializer = None
        self._producer = None
        self._consumer = None

    @property
    def logger(self) -> logging.Logger:
        """The logger to use.

        Returns:
            logging.Logger: The logger to use.
        """
        if not self._logger:
            self._logger = logger
        self._logger.info("Logger initialized for PipelineManager.")
        return self._logger

    @property
    def producer_key_schema_str(self) -> str:
        """The key schema string for the producer.

        Returns:
            str: The key schema string for the producer.

        Raises:
            AttributeError: If the schema is not found in pipelines_schemas.
        """
        schema_name = f"{self.output_topic}_key"
        try:
            return getattr(pipelines_schemas, schema_name)
        except AttributeError as error:
            logger.error("AttributeError: %s", error)
            raise AttributeError(f"Schema {schema_name} not found in pipelines_schemas.") from error

    @property
    def producer_value_schema_str(self) -> str:
        """The value schema string for the producer.

        Returns:
            str: The value schema string for the producer.
        """
        schema_name = f"{self.output_topic}_value"
        try:
            return getattr(pipelines_schemas, schema_name)
        except AttributeError as error:
            logger.error("AttributeError: %s", error)
            raise AttributeError(f"Schema {schema_name} not found in pipelines_schemas.") from error

    @property
    def consumer_key_schema_str(self) -> str:
        """The key schema string for the consumer.

        Returns:
            str: The key schema string for the consumer.

        Raises:
            AttributeError: If the schema is not found in pipelines_schemas.
        """
        schema_name = f"{self.input_topic}_key"
        try:
            return getattr(pipelines_schemas, schema_name)
        except AttributeError as error:
            logger.error("AttributeError: %s", error)
            raise AttributeError(f"Schema {schema_name} not found in pipelines_schemas.") from error

    @property
    def consumer_value_schema_str(self) -> str:
        """The value schema string for the consumer.

        Returns:
            str: The value schema string for the consumer.

        Raises:
            AttributeError: If the schema is not found in pipelines_schemas.
        """
        schema_name = f"{self.input_topic}_value"
        try:
            return getattr(pipelines_schemas, schema_name)
        except AttributeError as error:
            logger.error("AttributeError: %s", error)
            raise AttributeError(f"Schema {schema_name} not found in pipelines_schemas.") from error

    @property
    def output_topic(self) -> str:
        """The output topic for the PipelineManager.

        Returns:
            str: The output topic for the PipelineManager.

        Raises:
            ValueError: If the output topic is not set.
        """
        if not self._output_topic:
            raise ValueError("Output topic is not set.")
        return self._output_topic

    @property
    def input_topic(self) -> Optional[str]:
        """The input topic for the PipelineManager.

        Returns:
            Optional[str]: The input topic for the PipelineManager.
        """
        return self._input_topic

    @property
    def topics(self) -> List[str]:
        """The topics for the PipelineManager.

        Returns:
            List[str]: The topics for the PipelineManager.
        """
        self._topics = [
            self._output_topic,
            self._input_topic
        ] if self._input_topic else [self._output_topic]
        return self._topics

    @property
    def producer(self) -> Producer:
        """The producer for the PipelineManager.

        Returns:
            Producer: The producer for the PipelineManager.
        """
        producer_config = self.pipeline_settings.producer_config
        if producer_config is None:
            raise ValueError("Default configurations not found for Producer.")
        if not self._producer:
            producer_config["debug"] = self._debug
            producer_config.update(self._producer_config)
            self._producer = Producer(producer_config)
        else:
            self._producer = Producer(self._producer_config)
        return self._producer

    @property
    def consumer(self) -> Consumer:
        """The consumer for the PipelineManager.

        Returns:
            Consumer: The consumer for the PipelineManager.
        """
        if not self._consumer:
            consumer_config = self.pipeline_settings.consumer_config
            consumer_config["debug"] = self._debug
            consumer_config.update(self._consumer_config)
            self._consumer = Consumer(consumer_config)
        return self._consumer

    @property
    def debug(self) -> bool:
        """The debug level for the PipelineManager.

        Returns:
            bool: The debug level for the PipelineManager.
        """
        return self._debug

    @property
    def auto_offset_reset(self) -> str:
        """The auto offset reset for the PipelineManager.

        Returns:
            str: The auto offset reset for the PipelineManager.
        """
        return self._auto_offset_reset

    @property
    def consumer_group_id(self) -> str:
        """The consumer group ID for the PipelineManager.

        Returns:
            str: The consumer group ID for the PipelineManager.
        """
        return self._consumer_group_id

    @property
    def pipeline_settings(self) -> PipelineSettings:
        """Get the settings for the PipelineManager.

        Returns:
            PipelineSettings: The settings for the PipelineManager.
        """
        if not self._pipeline_settings:
            self._pipeline_settings = PipelineSettings(debug=self._debug)
        return self._pipeline_settings

    @property
    def admin(self) -> AdminClient:
        """Get the admin client for the PipelineManager.

        Returns:
            AdminClient: The admin client for the PipelineManager.
        """
        if not self._admin:
            self._admin = AdminClient(self.pipeline_settings.base_kafka_config)
            # if self.developer_environment:
            #     logger.info("Deleting and recreating topics in developer mode.")
            #     self.delete_and_recreate_topics(self.topics)
            # else:
            #     logger.info("Creating topics if they do not exist.")
            #     self.create_topics_if_not_exist(self.topics)
        return self._admin

    @property
    def schema_registry_client(self) -> SchemaRegistryClient:
        """Get the schema registry client for the PipelineManager.

        Returns:
            SchemaRegistryClient: The schema registry client for the PipelineManager.
        """
        if not self._schema_registry_client:
            self._schema_registry_client = SchemaRegistryClient(
                self.pipeline_settings.schema_registry_config,
                self
            )
        return self._schema_registry_client

    @property
    def message_serializer(self) -> MessageSerializer:
        """Get the message serializer for the PipelineManager.

        Returns:
            MessageSerializer: The message serializer for the PipelineManager.
        """
        if not self._message_serializer:
            self._message_serializer = MessageSerializer(
                self.schema_registry_client,
                self.pipeline_settings.schema_registry_config
            )
        return self._message_serializer

    @property
    def developer_environment(self) -> bool:
        """Whether the PipelineManager is in developer mode.

        Returns:
            bool: Whether the PipelineManager is in developer mode.
        """
        return self.pipeline_settings.developer_environment

    def delete_and_recreate_topics(self, topics: List[str]) -> None:
        """Delete and recreate topics.

        Args:
            topics: List of topics to delete and recreate.
        """
        # Delete existing topics
        self.admin.delete_topics(topics)
        logger.debug("Deleted topics: %s", topics)

        # Recreate topics
        self.create_topics_if_not_exist(topics)

    def create_topics_if_not_exist(self, topics: List[str]) -> None:
        """Create Kafka topics if they do not exist.

        Args:
            topics (List[str]): List of topic names to create.

        Raises:
            KafkaError: If the topic creation fails.
        """
        for topic in topics:
            new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
            try:
                self.admin.create_topics([new_topic])
                logger.debug("Created topic: %s", topic)
            except Exception as e:
                logger.error("Failed to create topic %s: %s", topic, e)
                raise

    def _coerce_decimal(
        self,
        value: Decimal,
        precision: int,
        scale: int
    ) -> Decimal:
        """Coerce a decimal value to match the specified scale and precision.

        Args:
            value: The decimal value to coerce.
            precision: The precision to coerce to.
            scale: The scale to coerce to.

        Returns:
            Decimal: The coerced decimal value.

        Raises:
            ValueError: If the value exceeds the specified precision.
        """
        quantize_exp = Decimal(f"1e-{scale}")
        coerced_value = value.quantize(quantize_exp)
        if coerced_value.adjusted() >= precision:
            raise ValueError(f"Value {value} exceeds precision {precision}")
        return coerced_value

    def _coerce_values_for_avro(
        self,
        record: Dict[str, Any],
        schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process a record to ensure all Decimal fields comply with the Avro schema.

        Args:
            record: The record to process.
            schema: The Avro schema to use.

        Returns:
            Dict[str, Any]: The processed record.
        """
        for field in schema["fields"]:
            if field["type"] == "bytes" and "logicalType" \
                    in field and field["logicalType"] == "decimal":
                precision = field["precision"]
                scale = field["scale"]
                field_name = field["name"]
                if field_name in record:
                    record[field_name] = self._coerce_decimal(
                        record[field_name],
                        precision,
                        scale
                    )
        return record

    def encode_record(
        self,
        record_value: Dict[str, Any],
        record_key: Optional[Union[Dict[str, Any], str, int]] = None,
        timestamp: Optional[int] = None,
        source_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        schema_version: Optional[str] = None,
    ) -> KafkaRecord:
        """Encode a record using the Avro schema.

        Args:
            record_value: The record value to encode.
            record_key: The record key to encode.
            timestamp: The timestamp to encode.
            source_id: The source ID to encode.
            dataset_id: The dataset ID to encode.
            schema_version: The schema version to encode.

        Returns:
            KafkaRecord: The encoded Kafka record containing
                header, value, key, and timestamp.
        """
        encoded_value = self.encode_field(
            record=record_value,
            schema=self.producer_value_schema_str,
            topic=self.output_topic,
            is_key=False
        )

        encoded_key = self.encode_field(
            record=record_key,
            schema=self.producer_key_schema_str,
            topic=self.output_topic,
            is_key=True
        ) if record_key is not None else None

        record = KafkaRecord(
            value=encoded_value,
            key=encoded_key,
            timestamp=timestamp if timestamp is not None else datetime.now().timestamp() * 1000,
            topic=self.output_topic,
        )
        record.add_header(key="source_id", value=source_id)
        record.add_header(key="dataset_id", value=dataset_id)
        record.add_header(key="schema_version", value=schema_version)
        return record

    def encode_field(
        self,
        record: Dict[str, Any],
        schema: Dict[str, Any],
        topic: Optional[str] = None,
        is_key: bool = False
    ) -> bytes:
        """Encode a field for a message.

        Args:
            record: The record to encode.
            schema: The schema to use for encoding.
            topic: The topic to use for encoding.
            is_key: Whether the field is a key.

        Returns:
            bytes: The encoded field.

        Raises:
            Exception: If there is an unexpected error encoding the record.
        """
        try:
            encoded_field = self.message_serializer.encode_record_with_schema(
                topic,
                schema,
                record,
                is_key=is_key
            )
        except SerializationError as e:
            logger.error("Error encoding record: %s", e)
        except Exception as e:
            logger.error("Unexpected error encoding record: %s", e)
            raise Exception("Unexpected error encoding record.") from e
        return encoded_field

    def decode_field(
        self,
        encoded_field: bytes,
        schema: Dict[str, Any],
        topic: Optional[str] = None,
        is_key: bool = False
    ) -> Dict[str, Any]:
        """Decode a field for a message.

        Args:
            encoded_field: The encoded field to decode.
            schema: The schema to use for decoding.
            topic: The topic to use for decoding.
            is_key: Whether the field is a key.

        Returns:
            Dict[str, Any]: The decoded field.

        Raises:
            Exception: If there is an unexpected error decoding the record.
        """
        try:
            decoded_field = self.message_serializer.decode_message(
                message=encoded_field,
                is_key=is_key
            )
        except SerializationError as e:
            logger.error("Error decoding record: %s", e)
        except Exception as e:
            logger.error("Unexpected error decoding record: %s", e)
            raise Exception("Unexpected error decoding record.") from e
        return decoded_field


__all__ = [
    "PipelineManager",
    "PipelineSettings",
]
