
import logging
from typing import Dict, Literal

from attr import define
from dotenv_derive import dotenv


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
        self, days: int, update_registry: bool = False, admin_client: AdminClient | None = None
    ) -> "MessageTopic":
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
    def create(
        name: str, partitions: int, replication_factor: int, time_to_live: int, time_to_live_type: Literal["days", "millis"]
    ) -> "MessageTopic":
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
            "value_schema": None,  # Placeholder, should be replaced with actual schema
        }

        return MessageTopic(**py_topic)


@schema_registry
@admin_client
@define
class StreamingTopic:
    """Streaming topic configuration."""
    name: str
    num_partitions: int = 1
    replication_factor: int = 1
    num_insync_replicas: int = 1

@define
class PipelineInputConfig:
    """Pipeline input/output configuration."""
    type: Literal["kafka", "file", "db", "s3"]
    name: str
    namespace: str
    root_key: str
    group_id: str

@define
class PipelineOutputConfig:
    """Pipeline output configuration.

    Args:
        name (str): The name of the output topic if type is kafka.
        output_type (Literal["kafka"]): The type of the output, default is "kafka".
        num_partitions (int): The number of partitions, default is 1.
        replication_factor (int): The replication factor, default is 1.
        num_insync_replicas (int): The number of in-sync replicas, default is 1.
    """
    name: str
    output_type: Literal["kafka"] = "kafka"
    output_topic: StreamingTopic

@define
class PipelineConfig:
    """Pipeline configuration."""
    pipeline_id: str
    logging_level: logging.Level
    output_config: PipelineIOConfig
    input_config: PipelineIOConfig

@define
class PipelineInputConfig:
    """Input configuration."""
    name: str
    type: str
    source_id: str
    dataset_id: str
    root_key: str
    group_id: str

@define
class KafkaConfig:
    """Kafka configuration."""
    topic: str
    key_schema: str  # Key schemas must be strings
    value_schema: str  # Assuming value schema is also a string for simplicity
    key_serializer: str
    value_serializer: str

@define
class PipelineOutputConfig:
    """Output configuration."""
    type: Literal['flink', 'kafka', 'file', 'db', 's3']
    name: str
    namespace: str

@define
@dotenv(dotenv_file="config/kafka.env")
class KafkaSecurityConfig:
    """Kafka security configuration."""
    bootstrap_servers: str
    security_protocol: str
    sasl_mechanisms: str
    sasl_username: str
    sasl_password: str

@define
class OutputConfig:
    """Output configuration."""
    output_topic: str
    kafka_config: Dict[str, str]
    topic_name: str
    avro_schema_path: str  # Support only Avro schemas for output topic values
    message_root: str
    input_type: str
    input_name: str
    input_key_schema: str  # Key schemas must be strings
    input_key_serializer: str
    input_value_schema: str
    input_value_serializer: str
    output_key_schema: str  # Key schemas must be strings
    output_value_schema: str  # Assuming value schema is also a string for simplicity
