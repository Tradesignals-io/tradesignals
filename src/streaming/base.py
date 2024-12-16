import io
import json
import threading
import time
from enum import StrEnum
from typing import Any, Dict, List, Literal

from attrs import define
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from fastavro import writer
from fastavro.schema import load_schema
from websocket import WebSocketApp


@define
class StreamingEnvironment:
    """Base streaming environment object.

    Attributes:
        name: The name of the environment.
    """

    name: str
    is_production: bool = False
    cluster: 'StreamingCluster'
    topics: List['StreamingTopic'] = []
    schema_registry: 'SchemaRegistryClient'
    producer: 'StreamingProducer'
    consumer: 'StreamingConsumer'


@define
class StreamingCluster:
    """Base streaming cluster object.

    Attributes:
        name: The name of the cluster.
    """

    name: str


@define
class KeyField:
    """Base key field object."""
    name: str
    serializer: Literal["avro", "string", "int", "fixed", "bytes", "none"]
    schema: 'FieldSchema'



@define
class ValueField:
    """Base value field object."""
    name: str
    serializer: Literal["avro", "string", "int", "fixed", "bytes", "none"]
    schema: 'FieldSchema'


@define
class StreamingTopic:
    """Base streaming topic object.

    Attributes:
        name: The name of the topic.
        num_partitions: The number of partitions in the topic.
        replication_factor: The replication factor for the topic.
        config: The configuration for the topic.
        cleanup_policy: The cleanup policy for the topic.
        retention_ms: The retention time for the topic in milliseconds.
        retention_bytes: The retention bytes for the topic.
        min_insync_replicas: The minimum number of in-sync replicas for the topic.
        message_timestamp_type: The type of message timestamp for the topic.
    """

    name: str
    num_partitions: int
    replication_factor: int
    config: Dict[str, Any]
    cleanup_policy: Literal["delete", "compact"]
    retention_ms: int = 30 * 24 * 60 * 60 * 1000
    retention_bytes: int = -1
    min_insync_replicas: int = 2
    message_timestamp_type: Literal["CreateTime", "LogAppendTime"] = "CreateTime"
    key_field: 'MessageFieldConfig'
    value_field: 'MessageFieldConfig'

    def __init__(
        self,
        name: str,
        num_partitions: int = 1,
        replication_factor: int = 3,
        cleanup_policy: Literal["delete", "compact"] = "delete",
        retention_ms: int = 30 * 24 * 60 * 60 * 1000,
        retention_bytes: int = -1,
        min_insync_replicas: int = 2,
        message_timestamp_type: Literal["CreateTime", "LogAppendTime"] = "CreateTime",
        key_field: "MessageFieldConfig" = MessageFieldConfig(name="key", serializer="string", schema=None),
        value_field: "MessageFieldConfig" = MessageFieldConfig(name="value", serializer="avro", schema=None),
    ):
        """Initialize the StreamingTopic object.

        Args:
            name: The name of the topic.
            num_partitions: The number of partitions in the topic.
            replication_factor: The replication factor for the topic.
            cleanup_policy: The cleanup policy for the topic.
            retention_ms: The retention time for the topic in milliseconds.
            retention_bytes: The retention bytes for the topic.
            min_insync_replicas: The minimum number of in-sync replicas for the topic.
            message_timestamp_type: The type of message timestamp for the topic.
        """
        self.name = name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.cleanup_policy = cleanup_policy
        self.retention_ms = retention_ms
        self.retention_bytes = retention_bytes
        self.min_insync_replicas = min_insync_replicas
        self.message_timestamp_type = message_timestamp_type

    def make_config_dict(self):
        """Make the configuration for the topic.

        Returns:
            Dict[str, Any]: The configuration for the topic.
        """
        return {
            "cleanup.policy": self.cleanup_policy,
            "retention.ms": self.retention_ms,
            "retention.bytes": self.retention_bytes,
            "min.insync.replicas": self.min_insync_replicas,
            "message.timestamp.type": self.message_timestamp_type,
        }

    def create_topic(self, admin_client: AdminClient):
        """Create a new topic if it does not already exist.

        Args:
            admin_client: The admin client to use for creating the topic.

        Raises:
            KafkaException: If there is an error creating the topic.
        """
        new_topic = NewTopic(
            topic=self.name,
            num_partitions=self.num_partitions,
            replication_factor=self.replication_factor,
            config=self.make_config_dict(),
        )
        try:
            # Check if the topic already exists
            existing_topics = admin_client.list_topics(timeout=10).topics
            if self.name in existing_topics:
                print(f"Topic {self.name} already exists.")
                return
            # Create the topic
            future = admin_client.create_topics([new_topic])
            # Wait for the operation to complete
            future[self.name].result()
        except Exception as error:
            raise KafkaException(
                f"Failed to create topic {self.name}: {error}"
            ) from error

    def delete_topic(self, admin_client: AdminClient):
        """Delete the topic if it exists.

        Args:
            admin_client: The admin client to use for deleting the topic.
        """
        admin_client.delete_topics([self.name])


class FieldSerializer(StrEnum):
    """Enumeration of types of field serializers.

    Attributes:
        AVRO: The Avro serializer.
        JSON: The JSON serializer.
        PROTOBUF: The Protobuf serializer.
        STRING: The String serializer.
        INT: The Integer serializer.
        FLOAT: The Float serializer.
        DOUBLE: The Double serializer.
        FIXED: The Fixed serializer.
        BYTES: The Bytes serializer.
        NONE: The None serializer.
    """

    AVRO = "avro"
    JSON = "json"
    PROTOBUF = "protobuf"
    STRING = "string"
    INT = "int"
    FLOAT = "float"
    DOUBLE = "double"
    FIXED = "fixed"
    BYTES = "bytes"
    NONE = "none"


@define
class MessageKey:
    """Base message key object.

    Attributes:
        name: The name of the field.
        type: The type of the field.
    """

    name = "key"
    serializer: Literal["avro", "string", "int", "fixed", "bytes", "none"]
    schema: 'FieldSchema'


@define
class MessageFieldConfig:
    """Base message field config object.

    Attributes:
        name: The name of the field.
        serializer: The serializer for the field.
        schema: The schema for the field.
    """

    name: Literal["key", "value"]
    serializer: Literal["avro", "string", "int", "fixed", "bytes", "none"]
    schema: "FieldSchema"

    def __init__(
        self,
        name: Literal["key", "value"],
        serializer: Literal["avro", "string", "int", "fixed", "bytes", "none"],
        schema: "FieldSchema" | None = None
    ):
        """Initialize the MessageField object.

        Args:
            name: The name of the field.
            serializer: The serializer for the field.
            schema: The schema for the field.
        """
        self.name = name
        self.serializer = serializer
        self.schema = schema

@define
class MessageTopic:
    """Base message topic object.

    Attributes:
        name: The name of the topic.
        num_partitions: The number of partitions in the topic.
        replication_factor: The replication factor for the topic.
        time_to_live_days: The time to live for the topic.
        delete_policy: The delete policy for the topic.
        key_schema: The key schema for the topic.
        value_schema: The value schema for the topic.
    """
    name: str
    num_partitions: int
    replication_factor: int
    time_to_live_millis: int
    delete_policy: Literal["compact", "delete"]
    key: MessageField(name="key", serializer="string", schema=None)
    value: MessageField(name="value", serializer="avro", schema=None)


    def set_ttl_by_days(
        self,
        days: int,
        update_registry: bool = False,
        admin_client: AdminClient | None = None
    ) -> 'MessageTopic':
        """Set the time to live for the topic in days.

        Args:
            days: The number of days to set the time to live for.
            update_registry: Flag to indicate if the registry should be updated.
            schema_registry_client: The client to use for updating the schema registry.

        Returns:
            MessageTopic: The updated topic object.

        Raises:
            ValueError: If update_registry is True and schema_registry_client is None.
        """
        self.time_to_live_millis = days * 24 * 60 * 60 * 1000

        if update_registry:
            if schema_registry_client is None:
                raise ValueError("Schema registry client must be provided if update_registry is True.")
            try:
                # Assuming update_schema_registry is a method of schema_registry_client
                schema_registry_client.update_schema_registry(self)
            except Exception as e:
                print(f"Failed to update schema registry: {e}")

        return self

    @staticmethod
    def create(name: str, partitions: int, replication_factor: int, time_to_live: int, time_to_live_type: Literal["days", "millis"]) -> 'MessageTopic':
        """Create a topic object using the provided parameters.

        Args:
            name: The name of the topic.
            partitions: The number of partitions in the topic.
            replication_factor: The replication factor for the topic.
            time_to_live_millis: Whether the time to live value is in days.
        Returns:
            MessageTopic: The created topic object.
        """
        if time_to_live_type == "days":
            time_to_live_millis = time_to_live * 24 * 60 * 60 * 1000
        elif time_to_live_type == "millis":
            time_to_live_millis = time_to_live
        else:
            raise ValueError("Invalid time_to_live_type. Must be 'days' or 'millis'.")

        py_topic = {
            "name": name,
            "num_partitions": partitions,
            "replication_factor": replication_factor,
            "time_to_live_millis": time_to_live_millis,
            "delete_policy": "delete",
            "key_schema": None,  # Placeholder, should be replaced with actual schema
            "value_schema": None  # Placeholder, should be replaced with actual schema
        }

        return MessageTopic(**py_topic)

@define
class BaseSchema:
    """Base schema object."""
    field: Literal["key", "value"]
    topic: MessageTopic

    def __init__(self, field: Literal["key", "value"], topic: MessageTopic) -> None:
        """Initialize the base schema object.

        :param field: The field to which the schema applies.
        :param topic: The topic of the schema.
        """
        self.field = field
        self.topic = topic
        self.subject = self.topic.name + "-" + self.field


@define
class FieldSchema:
    """Base schema object.

    Attributes:
        field: The field to which the schema applies.
        subject: The subject of the schema.
        topic: The topic of the schema.
    """

    field: Literal["key", "value"]
    topic: MessageTopic
    subject: str
    subject_strategy: Literal["default", "topic_name", "record_name"]

    def __init__(
        self,
        field: Literal["key", "value"],
        topic: str
    ) -> None:
        """
        Initialize the field schema object.

        Args:
            field : Literal["key", "value"]
                The field to which the schema applies.
            topic : str
                The topic of the schema.
            subject_strategy : Literal["default", "topic_name", "record_name"]
                The strategy to use for the subject name.
        """
        self.field = field
        self.topic = topic
        # TODO: Add support for Topic Name Strategy with Topic Name
        # TODO: Add support for Record Name Strategy with Record Name
        self.subject = self.topic + "-" + self.field
        self.subject_strategy = "default"


@define
class AvroFieldSchema(FieldSchema):
    """Represents an Avro schema object."""
    field: Literal["value"]
    type: Literal["avro"]
    schema: str


@define
class SchemaVersion:
    """Represents a schema object.

    Schema is used to define the schema of a message field.

    Attributes:
        field: The field to which the schema applies.
        subject: The subject of the schema.
        type: The type of the schema.
        str: The schema string.
    """

    field: Literal["key", "value"]
    subject: str
    topic: str
    type: Literal[
        "avro",
        "json",
        "protobuf",
        "string",
        "int",
        "float",
        "double",
        "bytes",
        "fixed",
    ] = "avro"
    str: str
    version: int | None = None

    def __init__(
        self,
        field: Literal["key", "value"],
        subject: str,
        topic: str,
        type: Literal[
            "avro",
            "json",
            "protobuf",
            "string",
            "int",
            "float",
            "double",
            "bytes",
            "fixed",
        ],
        str: str,
        version: int | None = None,
    ):
        self.field = field
        self.subject = subject
        self.topic = topic
        self.type = type
        self.str = str
        self.version = version
        # At this time only the default subject name convention is supported.
        # TODO: Add support for custom subject name convention
        # The subject is the topic + "-" + the field
        self.subject = self.topic + "-" + self.field

    @classmethod
    def register(
        cls,
        schema_registry_client: SchemaRegistryClient,
        schema: 'SchemaVersion',
    ):
        """Register the schema in the schema registry.

        :param schema_registry_client: The schema registry client.
        :param schema: The schema to register.
        :return: The registered schema.
        """
        schema_registry_client.register_schema(
            schema.subject, schema.str, schema.type, schema.version
        )

    @classmethod
    def from_py(cls, json_schema: Dict[str, Any]):
        """Create a schema object from a JSON schema.

        :param json_schema: The JSON schema.
        :return: The schema object.
        """
        return cls(**json_schema)

    def __str__(self) -> str:
        """Stringify the schema object.

        Returns:
            str: String representation of the schema.
        """
        return f"""
        Schema(
            field={self.field},
            subject={self.subject},
            topic={self.topic},
            type={self.type},
            str={self.str},
            version={self.version}
        )
        """

    def __repr__(self) -> str:
        """Return a string representation of the schema object.

        Returns:
            str: String representation of the schema.
        """
        return self.__str__()


@define
class TopicSchema:
    """Pipeline topic object."""
    key_subject: str
    key_type: str
    key_schema: str
    value_subject: str
    value_type: str
    value_schema: str

@define
class PipelineConfig:
    """Pipeline configuration object."""
    ws_url: str
    kafka_config: Dict[str, Any]
    topic_name: str
    avro_schema_path: str

    def __init__(self, ws_url, kafka_config, topic_name, avro_schema_path):
        self.ws_url = ws_url
        self.kafka_config = kafka_config
        self.topic_name = topic_name
        self.avro_schema_path = avro_schema_path


class Pipeline:
    def __init__(self, ws_url, kafka_config, topic_name, avro_schema_path):
        """
        Initializes the WebSocket to Kafka pipeline.

        :param ws_url: WebSocket URL.
        :param kafka_config: Dictionary of Kafka configuration (e.g., Confluent Cloud configs).
        :param topic_name: Name of the Kafka topic.
        :param avro_schema_path: Path to the Avro schema file.
        """
        self.ws_url = ws_url
        self.kafka_config = kafka_config
        self.topic_name = topic_name
        self.schema = load_schema(avro_schema_path)
        self.producer = Producer(self.kafka_config)
        self.stop_event = threading.Event()

    def on_message(self, ws, message):
        """
        Callback for processing incoming WebSocket messages.
        Encodes the message using Avro and sends it to Kafka.

        :param ws: WebSocket connection object.
        :param message: Raw message received from the WebSocket.
        """
        try:
            # Parse the incoming message
            data = json.loads(message)

            # Serialize the message using Avro
            avro_bytes = self._serialize_to_avro(data)

            # Send the serialized message to Kafka
            self.producer.produce(self.topic_name, value=avro_bytes)
            self.producer.flush()

            print(f"Message sent to Kafka: {data}")
        except Exception as e:
            print(f"Error processing message: {e}")

    def on_error(self, ws, error):
        """
        Callback for handling WebSocket errors.

        :param ws: WebSocket connection object.
        :param error: Error message.
        """
        print(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """
        Callback for handling WebSocket closure.

        :param ws: WebSocket connection object.
        :param close_status_code: Closure status code.
        :param close_msg: Closure message.
        """
        print(f"WebSocket closed: {close_status_code} {close_msg}")

    def on_open(self, ws):
        """
        Callback for WebSocket open event.
        """
        print("WebSocket connection established.")

    def start(self):
        """
        Starts the WebSocket connection and begins streaming data to Kafka.
        """
        self.ws = WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.ws.on_open = self.on_open

        # Run the WebSocket in a separate thread
        self.thread = threading.Thread(target=self.ws.run_forever)
        self.thread.start()

        try:
            while not self.stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """
        Stops the WebSocket connection and pipeline.
        """
        self.stop_event.set()
        self.ws.close()
        self.thread.join()
        print("Pipeline stopped.")

    def _serialize_to_avro(self, data):
        """
        Serializes a Python dictionary to Avro format.

        :param data: Dictionary to serialize.
        :return: Serialized Avro bytes.
        """
        with io.BytesIO() as buffer:
            writer(buffer, self.schema, [data])
            return buffer.getvalue()


# Example usage
if __name__ == "__main__":
    ws_url = "wss://example.com/websocket"
    kafka_config = {
        "bootstrap.servers": "your-bootstrap-servers",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "your-api-key",
        "sasl.password": "your-api-secret",
    }
    topic_name = "your-topic-name"
    avro_schema_path = "path/to/your/avro/schema.avsc"

    pipeline = WebSocketToKafkaPipeline(ws_url, kafka_config, topic_name, avro_schema_path)
    pipeline.start()