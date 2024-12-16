import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, Producer

from src.pipelines.kafka.pipeline_manager import PipelineManager

# Set up logging
logger = logging.getLogger('Job-Logger')
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.propagate = True


class DataPipeline(ABC):
    """Abstract base class for batch and streaming jobs.

    This class defines the interface for executing batch and streaming jobs.
    Streaming jobs should never terminate, while batch jobs will complete after processing.

    Attributes:
        pipeline_manager (PipelineManager): Instance of PipelineManager for managing Kafka clients.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize the DataPipeline class.

        Args:
            config (Dict[str, Any]): Configuration dictionary for the job.
        """
        self.config: Dict[str, Any] = config
        self.pipeline_manager: Optional[PipelineManager] = None
        self.producer: Optional[Producer] = None
        self.consumer: Optional[Consumer] = None

    def _initialize_pipeline(self) -> None:
        """Initialize the pipeline components."""
        self.pipeline_manager = PipelineManager(
            output_topic=self.config['output_topic'],
            input_topic=self.config.get('input_topic'),
            consumer_group_id=self.config.get('consumer_group_id', 'default-consumer-group'),
            auto_offset_reset=self.config.get('auto_offset_reset', 'earliest'),
            debug=self.config.get('debug', False),
            producer_config=self.config.get('producer_config'),
            consumer_config=self.config.get('consumer_config')
        )
        self.producer = Producer(self.pipeline_manager.producer)
        self.consumer = Consumer(self.pipeline_manager.consumer)

    def serialize_record(self, record: Dict[str, Any]) -> bytes:
        """Serialize a record to Avro bytes.

        This method processes a record to ensure all Decimal fields comply
        with the Avro schema and serializes it to Avro bytes.

        Args:
            record (Dict[str, Any]): The record to serialize.

        Returns:
            bytes: The serialized record in Avro format.
        """
        schema = self.pipeline_manager.producer_value_schema_str if self.pipeline_manager else None
        avro_record: Dict[str, Any] = (
            self.pipeline_manager._coerce_values_for_avro(record, schema)
            if self.pipeline_manager else record
        )
        return (
            self.pipeline_manager.message_serializer.encode_record_with_schema(
                topic=self.pipeline_manager.output_topic if self.pipeline_manager else '',
                schema=schema,
                record=avro_record
            )
            if self.pipeline_manager else bytes()
        )

    def produce_record(self, record: Dict[str, Any]) -> None:
        """Produce a serialized record to Kafka.

        This method serializes the record to Avro bytes and sends it to the configured Kafka topic.

        Args:
            record (Dict[str, Any]): The record to produce.
        """
        avro_bytes = self.serialize_record(record)
        self._produce(avro_bytes)
        self._flush()

    def _produce(self, avro_bytes: bytes) -> None:
        """Produce a message to Kafka and poll for events.

        Args:
            avro_bytes (bytes): The serialized record in Avro format.
        """
        if self.pipeline_manager and self.pipeline_manager.producer:
            self.producer.produce(
                self.pipeline_manager.output_topic,
                value=avro_bytes
            )
            self.producer.poll(0)

    def _flush(self) -> None:
        """Flush the producer to ensure all messages are sent."""
        if self.producer:
            self.producer.flush()

    @abstractmethod
    def execute(self) -> None:
        """Execute the job.

        This method should be implemented by subclasses to define the job's execution logic.
        Use this method to produce records to kafka.  All you need to do is call produce_record()
        In order for this to work correctly you need to call start() and stop() and also make
        sure you call this method with a valid avro schema.  The function to produce will
        attempt to coerce the values to the schema - it will also auto-register the schema with the
        schema registry if it is not already registered.
        """
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """Shutdown the job.

        This method should be implemented by subclasses to define the job's shutdown logic.
        """
        pass

    def start(self) -> None:
        """Start the job."""
        if self.pipeline_manager is None:
            self._initialize_pipeline()
        logger.info(f"Starting job {self.__class__.__name__}...")
        self.execute()

    def stop(self) -> None:
        """Stop the job."""
        logger.info(f"Shutting down job {self.__class__.__name__}...")
        self.shutdown()


__all__ = ['DataPipeline']
