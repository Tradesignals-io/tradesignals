from confluent_kafka.avro import AvroConsumer, AvroProducer
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient


def make_avro_ready(schema_str: str) -> CachedSchemaRegistryClient:
    return CachedSchemaRegistryClient(schema_str)


class AvroClient(AvroProducer, AvroConsumer):
    """Avro Client for writing and reading data to Kafka.

    Attributes:
        reader_topic: The topic to read from.
        writer_topic: The topic to write to.
        writer_key_schema_str: The schema string for the key of the writer.
        writer_value_schema_str: The schema string for the value of the writer.
        reader_key_schema_str: The schema string for the key of the reader.
        reader_value_schema_str: The schema string for the value of the reader.
    """


    def __init__(
        self,
        reader_topic: str,
        writer_topic: str,
        writer_key_schema_str: str,
        writer_value_schema_str: str,
        reader_key_schema_str: str,
        reader_value_schema_str: str
    ) -> None:
        self.reader_topic = reader_topic
        self.writer_topic = writer_topic
        self.writer_key_schema_str = writer_key_schema_str
        self.writer_value_schema_str = writer_value_schema_str
        self.reader_key_schema_str = reader_key_schema_str
        self.reader_value_schema_str = reader_value_schema_str
        self.config = Settings().base_config
        super(AvroProducer, self).__init__(default_value_schema=writer_value_schema_str, default_key_schema=writer_key_schema_str, )
        super(AvroConsumer, self).__init__(default_value_schema=reader_value_schema_str)
