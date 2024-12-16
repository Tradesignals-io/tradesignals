import abc
import asyncio
from typing import Any, Dict, Literal, Optional, TypeVar

import websocket
import websockets
from attr import define

from streaming.types import TopicSchema

WebSocketApp = TypeVar("WebSocketApp", bound=websocket.WebSocketApp)


class WebSocketClient:
    """WebSocket client wrapper for handling connections and messaging."""

    def __init__(self, url: str):
        """Initialize the WebSocket client.

        Args:
            url (str): The WebSocket URL to connect to.
        """
        self.url = url
        self._ws = None

    async def connect(self):
        """Establish a connection to the WebSocket server."""
        self._ws = await websockets.connect(self.url)

    async def send(self, message: str):
        """Send a message to the WebSocket server.

        Args:
            message (str): The message to send.
        """
        await self._ws.send(message)

    async def receive(self) -> str:
        """Receive a message from the WebSocket server.

        Returns:
            str: The received message.
        """
        return await self._ws.recv()

    async def close(self):
        """Close the WebSocket connection."""
        await self._ws.close()

@define
class PipelineIOConfig:
    """Configuration for pipeline input or output."""
    name: str
    io_type: Literal["kafka", "websocket", "file", "database", "console"] = "kafka"
    partitions: int = 4
    replication_factor: int = 3
    min_insync_replicas: int = 2
    schema: TopicSchema

class StreamingPipeline(abc.ABC):
    """Abstract base class for streaming pipelines."""

    @property
    @abc.abstractmethod
    def stream_output(self) -> PipelineIOConfig:
        """Abstract property to get output configuration."""
        pass

    @property
    @abc.abstractmethod
    def stream_input(self) -> PipelineIOConfig:
        """Abstract property to get input configuration."""
        pass

    @property
    @abc.abstractmethod
    def websocket_client(self) -> Optional[WebSocketClient]:
        """Abstract property to get WebSocket client."""
        return None

    @property
    @abc.abstractmethod
    def websocket_connect_message(self) -> str:
        """Abstract property to get WebSocket connect message."""
        pass

    @abc.abstractmethod
    def validate_message(self, raw_message: Dict[str, Any]) -> Dict[str, Any] | None:
        """Abstract method to validate the received message.

        Args:
            raw_message (Dict[str, Any]): The raw message received from the WebSocket.

        Returns:
            Dict[str, Any] | None: The validated message or None if invalid.
        """
        pass

    @abc.abstractmethod
    def extract_data(self, raw_message: Dict[str, Any]) -> Dict[str, Any]:
        """Abstract method to extract data from the message.

        Args:
            raw_message (Dict[str, Any]): The raw message received from the WebSocket.

        Returns:
            Dict[str, Any]: The extracted data.
        """
        pass

    @abc.abstractmethod
    def set_key(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Abstract method to set the key for the message.

        Args:
            extracted_data (Dict[str, Any]): The extracted data.

        Returns:
            Dict[str, Any]: The key for the message.
        """
        pass

    @abc.abstractmethod
    def set_value(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Abstract method to set the value for the message.

        Args:
            extracted_data (Dict[str, Any]): The extracted data.

        Returns:
            Dict[str, Any]: The value for the message.
        """
        pass

    @abc.abstractmethod
    def set_timestamp(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Abstract method to set the timestamp for the message.

        Args:
            extracted_data (Dict[str, Any]): The extracted data.

        Returns:
            Dict[str, Any]: The timestamp for the message.
        """
        pass

    @abc.abstractmethod
    def set_headers(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Abstract method to set the headers for the message.

        Args:
            extracted_data (Dict[str, Any]): The extracted data.

        Returns:
            Dict[str, Any]: The headers for the message.
        """
        pass

    @abc.abstractmethod
    def coerce_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Abstract method to coerce data according to Avro schema.

        Args:
            data (Dict[str, Any]): The data to be coerced.

        Returns:
            Dict[str, Any]: The coerced data.
        """
        pass

    @abc.abstractmethod
    def produce_to_kafka(self, key: Dict[str, Any], value: Dict[str, Any], timestamp: Dict[str, Any], headers: Dict[str, Any]):
        """Abstract method to produce message to Kafka.

        Args:
            key (Dict[str, Any]): The key for the message.
            value (Dict[str, Any]): The value for the message.
            timestamp (Dict[str, Any]): The timestamp for the message.
            headers (Dict[str, Any]): The headers for the message.
        """
        pass

class WebSocketToKafkaPipeline(StreamingPipeline):
    """WebSocket to Kafka Pipeline.

    This class extends the StreamingPipeline class to implement a specific pipeline
    that streams data from a WebSocket to a Kafka topic.

    Args:
        ws_url (str): WebSocket URL.
        kafka_config (Dict[str, Any]): Dict[str, Any]ionary of Kafka configuration (e.g., Confluent Cloud configs).
        topic_name (str): Name of the Kafka topic.
        avro_schema_path (str): Path to the Avro schema file.

    Kwargs:
        None

    Returns:
        None

    Raises:
        None
    """

    def __init__(self, ws_url: str, kafka_config: Dict[str, Any], topic_name: str, avro_schema_path: str):
        """Initialize the WebSocket to Kafka pipeline.

        Args:
            ws_url (str): WebSocket URL.
            kafka_config (Dict[str, Any]): Kafka configuration.
            topic_name (str): Kafka topic name.
            avro_schema_path (str): Avro schema file path.
        """
        self._ws_client = None
        self.ws_url = ws_url
        self.kafka_config = kafka_config
        self.topic_name = topic_name
        self.avro_schema_path = avro_schema_path

    @property
    def _ws_client_instance(self):
        """Lazily initialize the WebSocketClient instance.

        Returns:
            WebSocketClient: The WebSocketClient instance.
        """
        if self._ws_client is None:
            self._ws_client = WebSocketClient(self.ws_url)
        return self._ws_client

    @property
    def output_config(self) -> PipelineIOConfig:
        """Returns the output configuration.

        Returns:
            PipelineIOConfig: The output configuration.
        """
        return PipelineIOConfig(
            name="output",
            io_type="kafka",
            partitions=8,
            replication_factor=3,
            min_insync_replicas=2,
            schema=TopicSchema(
                key_schema="output_key_schema",
                value_schema="output_value_schema",
                key_serializer="output_key_serializer",
                value_serializer="output_value_serializer",
            )
        )

    @property
    def input_config(self) -> PipelineIOConfig:
        """Returns the input configuration.

        Returns:
            PipelineIOConfig: The input configuration.
        """
        return PipelineIOConfig(
            name="input",
            io_type="websocket",
            partitions=8,
            replication_factor=3,
            min_insync_replicas=2,
            schema=TopicSchema(
                key_schema="input_key_schema",
                value_schema="input_value_schema",
                key_serializer="input_key_serializer",
                value_serializer="input_value_serializer",
            )
        )

    @property
    def websocket_client(self) -> WebSocketClient:
        """Returns the WebSocket client.

        Returns:
            WebSocketClient: The WebSocket client.
        """
        return self._ws_client_instance

    @property
    def websocket_connect_message(self) -> str:
        """Returns the WebSocket connect message.

        Returns:
            str: The WebSocket connect message.
        """
        return {"type": "connect"}

    def validate_message(self, raw_message: Dict[str, Any]) -> Dict[str, Any] | None:
        """Validates the message from the WebSocket.

        Args:
            raw_message (Dict[str, Any]): The raw message received from the WebSocket.

        Returns:
            Dict[str, Any] | None: The validated message or None if invalid.
        """
        return raw_message if raw_message.get("type") == "connect" else None

    def extract_data(self, raw_message: Dict[str, Any]) -> Dict[str, Any]:
        """Extracts the data from the message.

        Args:
            raw_message (Dict[str, Any]): The raw message received from the WebSocket.

        Returns:
            Dict[str, Any]: The extracted data.
        """
        return raw_message.get("data", {})

    def set_key(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Sets the key for the message.

        Args:
            extracted_data (Dict[str, Any]): The extracted data.

        Returns:
            Dict[str, Any]: The key for the message.
        """
        return extracted_data.get("key", {})

    def set_value(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Sets the value for the message.

        Args:
            extracted_data (Dict[str, Any]): The extracted data.

        Returns:
            Dict[str, Any]: The value for the message.
        """
        return extracted_data.get("value", {})

    def set_timestamp(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Sets the timestamp for the message.

        Args:
            extracted_data (Dict[str, Any]): The extracted data.

        Returns:
            Dict[str, Any]: The timestamp for the message.
        """
        return extracted_data.get("timestamp", {})

    def set_headers(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Sets the headers for the message.

        Args:
            extracted_data (Dict[str, Any]): The extracted data.

        Returns:
            Dict[str, Any]: The headers for the message.
        """
        return extracted_data.get("headers", {})

    def coerce_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Coerces data according to Avro schema.

        Args:
            data (Dict[str, Any]): The data to be coerced.

        Returns:
            Dict[str, Any]: The coerced data.
        """
        # Implement the logic to coerce data according to Avro schema
        return data

    def produce_to_kafka(self, key: Dict[str, Any], value: Dict[str, Any], timestamp: Dict[str, Any], headers: Dict[str, Any]):
        """Produces message to Kafka.

        Args:
            key (Dict[str, Any]): The key for the message.
            value (Dict[str, Any]): The value for the message.
            timestamp (Dict[str, Any]): The timestamp for the message.
            headers (Dict[str, Any]): The headers for the message.
        """
        # Implement the logic to produce message to Kafka
        pass

    async def start(self):
        """Start the WebSocket to Kafka pipeline."""
        await self.websocket_client.connect()
        try:
            while True:
                raw_message = await self.websocket_client.receive()
                validated_message = self.validate_message(raw_message)
                if validated_message:
                    extracted_data = self.extract_data(validated_message)
                    key = self.set_key(extracted_data)
                    value = self.set_value(extracted_data)
                    timestamp = self.set_timestamp(extracted_data)
                    headers = self.set_headers(extracted_data)
                    coerced_value = self.coerce_data(value)
                    self.produce_to_kafka(key, coerced_value, timestamp, headers)
        finally:
            await self.websocket_client.close()

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
    asyncio.run(pipeline.start())
