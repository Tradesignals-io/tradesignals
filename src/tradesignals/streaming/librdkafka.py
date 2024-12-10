import os

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient


class KafkaConfig:
    """
    Wrapper class to read environment variables and build a configuration dictionary
    for librdkafka shared between producers and consumers.
    """

    def __init__(self):
        self.config = self._build_config_dict()

    def _build_config_dict(self) -> dict:
        """
        Build the configuration dictionary from environment variables.

        Returns
        -------
        dict
            Configuration dictionary for librdkafka.
        """
        config = {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
            "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAIN"),
            "sasl.username": os.getenv("KAFKA_SASL_USERNAME", ""),
            "sasl.password": os.getenv("KAFKA_SASL_PASSWORD", ""),
            "schema.registry.url": os.getenv("SCHEMA_REGISTRY_URL", ""),
            "schema.registry.basic.auth.user.info": os.getenv(
                "SCHEMA_REGISTRY_AUTH_INFO", ""
            ),
            "consumer.group.id": os.getenv(
                "KAFKA_CONSUMER_GROUP_ID", "tradesignals-default"
            ),
            # "auto.offset.reset": "earliest",
            # "enable.auto.commit": False,
            # "enable.auto.offset.store": False,
            # "auto.commit.interval.ms": 1000,
            # "auto.offset.store.sync.interval.ms": 1000,
            # "auto.offset.store.retry.count": 3,
            # "auto.offset.store.retry.interval.ms": 1000,
            # "auto.offset.store.retry.backoff.ms": 1000,
        }
        return config


class Kafka:
    """
    Kafka class to initialize Producer, Consumer, AdminClient, and SchemaRegistryClient
    with the configuration built from environment variables.
    """

    def __init__(self):
        self.config = KafkaConfig().config
        self.producer = Producer(self.config)
        self.consumer = Consumer(self.config)
        self.admin = AdminClient(self.config)
        self.schema_registry = SchemaRegistryClient(self.config)
