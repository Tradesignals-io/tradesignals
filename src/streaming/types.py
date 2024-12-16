
"""
Streaming Pipeline Abstract Base Class.

Streaming pipelines facilitate the streaming of data from various input sources to a Kafka topic. Input sources can include a websocket, a file, a database, or another Kafka topic.

The configuration for the pipeline is divided into three main sections:

1. Pipeline Configuration
2. Input Configuration
3. Output Configuration

Pipeline Configuration:
---------------------------------------------------------------------
Pipeline:
[pipeline]
- id: The unique identifier for the pipeline.
- name: A human-readable name for the pipeline.

Pipeline Logging:
[pipeline.logging]
- level: The log level for the pipeline.
- file: The file path for logging output.

Pipeline Stats:
[pipeline.stats]
- callback: The callback function for statistics.
- db_type: The type of database ('postgres', 'clickhouse').
- db_uri: The URI for the database to store statistics.
- db_schema: The schema of the database for statistics.
- db_table: The table name for storing statistics.

Input Configuration:
---------------------------------------------------------------------
Inputs can be a websocket, a file, a database, or another Kafka topic. If the input is a Kafka topic, the pipeline reads data from it and sends it to the output. The input Kafka endpoint can be the same or different from the output Kafka endpoint.

Pipeline Input:
[pipeline.input]
- name: The name of the data source.
- type: The type of the data source.
- source_id: The ID of the data provider.
- dataset_id: The name of the data provider.
- root_key: The root key of the JSON message to extract.
- group_id: The group ID for the data.

Pipeline Input Kafka:
[pipeline.input.kafka]
- topic: The Kafka topic name for sending messages.
- key_schema: The schema for the key sent to the Kafka topic.
- value_schema: The schema for the value sent to the Kafka topic.
- key_serializer: The serializer for the key.
- value_serializer: The serializer for the value.

Output Configuration:
---------------------------------------------------------------------
Pipeline Output:
[pipeline.output]
- type: The type of data output (flink, kafka, file, db, s3, etc.)
- name: The name of the record to send to the output.
- namespace: The namespace of the record to send to the output.

Pipeline Output Kafka:
[pipeline.output.kafka]
- bootstrap_servers: The Kafka connection bootstrap servers.
- security_protocol: The security protocol for Kafka connection.
- sasl_mechanisms: The SASL mechanisms for Kafka connection.
- sasl_username: The SASL username for Kafka connection.
- sasl_password: The SASL password for Kafka connection.

Pipeline Output Kafka Topic:
[pipeline.output.kafka.topic]
- topic: The Kafka topic name for sending messages.
- key_schema: The schema for the key sent to the Kafka topic.
- value_schema: The schema for the value sent to the Kafka topic.
- key_serializer: The serializer for the key.
- value_serializer: The serializer for the value.

Output Configuration:
- output_topic: The Kafka topic name for sending messages.
- kafka_config: Dictionary of Kafka configuration (librdkafka).
- topic_name: The name of the Kafka topic.
- avro_schema_path: The path to the Avro schema file.
- message_root: The root key of the JSON message to extract, used as the Kafka message value.
- input_type: The type of the input data.
- input_name: The name of the input data.
- input_key_schema: The schema for the key sent to the Kafka topic.
- input_key_serializer: The serializer for the key.
- input_value_schema: The schema for the value sent to the Kafka topic.
- input_value_serializer: The serializer for the value.
- output_topic: The Kafka topic name for sending messages.
- output_key_schema: The schema for the key sent to the Kafka topic.
- output_value_schema: The schema for the value sent to the Kafka topic.

Examples:
---------------------------------------------------------------------
Setup the input and output topics with by constructing new StreamingTopic
objects.  Each object is a configuration for a Kafka topic.

# Input Topic
>>> input = StreamingTopic(
...     topic="input",
...     partitions=8,
...     replication_factor=3,
...     min_insync_replicas=2,
...     key_schema="input_key_schema",
...     value_schema="input_value_schema",
...     key_serializer="input_key_serializer",
...     value_serializer="input_value_serializer",
... )

# Output Topic
>>> output = StreamingTopic(
...     topic="output",
...     partitions=8,
...     replication_factor=3,
...     min_insync_replicas=2,
...     key_schema="output_key_schema",
...     value_schema="output_value_schema",
...     key_serializer="output_key_serializer",
...     value_serializer="output_value_serializer",
... )

Then create a pipeline with the input and output topics set as the returns
of the abstract properties input_topic and output_topic.

>>> class MyPipeline(Pipeline):
...
...     @property
...     def input_topic(self) -> StreamingTopic:
...         return input_topic

...     @property
...     def output_topic(self) -> StreamingTopic:
...         return output_topic
...
...     @property
...     def websocket_connect_message(self) -> Dict[str, Any]:
...         return {"type": "connect"}
...
...     # Validate the message from the websocket
...     def websocket_message_validator(self, raw_message: Dict[str, Any]) -> Dict[str, Any] | None:
...         return raw_message if raw_message["type"] == "connect" else None
...
...     # Extract the data from the message
...     def extract_data(self, raw_message: Dict[str, Any]) -> Dict[str, Any]:
...         return raw_message["data"]
...
...     # Set the key for the message
...     def set_key(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
...         return extracted_data["key"]
...
...     # Set the value for the message
...     def set_value(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
...         return extracted_data["value"]

>>> pipeline = MyPipeline(ws_url, kafka_config, topic_name, avro_schema_path)
>>> pipeline.start()
# pipeline starts streaming data to Kafka
>>> pipeline.stop()
# pipeline stops streaming data to Kafka

"""
import io
import json
import threading
import time

from confluent_kafka import Producer
from fastavro import writer
from fastavro.schema import load_schema
from websocket import WebSocketApp


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
