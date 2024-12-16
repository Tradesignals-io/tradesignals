"""Decorators for injecting confluent kafka clients into classes.

Required Environment Variables:
---------------------------------------------------------------------
    - SCHEMA_REGISTRY_URL
    - SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO
    - KAFKA_BOOTSTRAP_SERVERS
    - KAFKA_SASL_USERNAME
    - KAFKA_SASL_PASSWORD

Examples:
---------------------------------------------------------------------
>>> @schema_registry(
>>>     url="http://localhost:8081",
>>>     auto_register_schemas=True,
>>>     basic_auth_user_info="user:password"
>>> )

>>> class MyClass:
>>>     pass

>>> my_class = MyClass()
>>> my_class.schema_registry.get_subjects()

>>> @admin_client(
>>>     bootstrap_servers="localhost:9092",
>>>     sasl_username="user",
>>>     sasl_password="password",
>>>     sasl_mechanism="PLAIN",
>>>     security_protocol="SASL_SSL"
>>> )
>>> class MyClass:
>>>     pass

>>> my_class = MyClass()

>>> my_class.admin_client.list_topics()

>>> @kafka_producer(
>>>     bootstrap_servers="localhost:9092",
>>>     sasl_username="user",
>>>     sasl_password="password",
>>>     sasl_mechanism="PLAIN",
>>>     security_protocol="SASL_SSL"
>>> )
>>> class MyClass:
>>>     pass

>>> @kafka_consumer()  # derives bootstrap_servers, sasl_username, sasl_password,
>>> class MyClass:  # sasl_mechanism, and security_protocol from environment variables
>>>     pass

>>> my_class = MyClass()
>>> my_class.consumer.consume()  # Consume messages from Kafka, etc.
"""
import os

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient


def schema_registry(
    url=None,
    auto_register_schemas=True,
    basic_auth_user_info=None
):
    """Inject a schema registry client into a class.

    Args:
        url (str, optional): The URL of the schema registry.
        auto_register_schemas (bool, optional): Register schemas automatically.
        basic_auth_user_info (str, optional): username:password formatted string.

    Returns:
        class: A wrapped class with a schema registry client.
    """
    def decorator(cls):
        class Wrapped(cls):
            def __init__(self, *args, **kwargs):
                self._schema_registry = None
                self._schema_registry_url = url or os.getenv(
                    "SCHEMA_REGISTRY_URL"
                )
                if not self._schema_registry_url:
                    raise ValueError(
                        "Schema registry URL must be provided or set "
                        "in environment variable 'SCHEMA_REGISTRY_URL'"
                    )
                self._auto_register_schemas = auto_register_schemas
                self._basic_auth_user_info = basic_auth_user_info or os.getenv(
                    "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO"
                )
                super().__init__(*args, **kwargs)

            @property
            def schema_registry(self):
                """Lazy initialization of the schema registry client.

                Returns:
                    SchemaRegistryClient: The schema registry client instance.
                """
                if self._schema_registry is None:
                    self._schema_registry = SchemaRegistryClient({
                        'url': self._schema_registry_url,
                        'basic.auth.user.info': self._basic_auth_user_info,
                        'basic.auth.credentials.source': 'USER_INFO'
                    })
                return self._schema_registry
        return Wrapped
    return decorator


def admin_client(
    bootstrap_servers,
    sasl_username=None,
    sasl_password=None,
    sasl_mechanism="PLAIN",
    security_protocol="SASL_SSL"
):
    """Inject an admin client into a class.

    Args:
        bootstrap_servers (str): Kafka bootstrap servers.
        sasl_username (str, optional): SASL username for authentication.
        sasl_password (str, optional): SASL password for authentication.
        sasl_mechanism (str, optional): SASL mechanism.
        security_protocol (str, optional): Security protocol.

    Returns:
        class: A wrapped class with an admin client.
    """
    def decorator(cls):
        class Wrapped(cls):
            def __init__(self, *args, **kwargs):
                self._admin_client = None
                self._bootstrap_servers = bootstrap_servers
                self._sasl_username = sasl_username
                self._sasl_password = sasl_password
                self._sasl_mechanism = sasl_mechanism
                self._security_protocol = security_protocol
                super().__init__(*args, **kwargs)

            @property
            def admin_client(self):
                """Lazy initialization of the admin client.

                Returns:
                    AdminClient: The admin client instance.
                """
                if self._admin_client is None:
                    self._admin_client = AdminClient({
                        'bootstrap.servers': self._bootstrap_servers,
                        'sasl.username': self._sasl_username,
                        'sasl.password': self._sasl_password,
                        'sasl.mechanism': self._sasl_mechanism,
                        'security.protocol': self._security_protocol
                    })
                return self._admin_client
        return Wrapped
    return decorator

def kafka_producer(
    bootstrap_servers,
    sasl_username=None,
    sasl_password=None,
    sasl_mechanism="PLAIN",
    security_protocol="SASL_SSL"
):
    """Inject a Kafka producer into a class.

    Args:
        bootstrap_servers (str): Kafka bootstrap servers.
        sasl_username (str, optional): SASL username for authentication.
        sasl_password (str, optional): SASL password for authentication.
        sasl_mechanism (str, optional): SASL mechanism.
        security_protocol (str, optional): Security protocol.

    Returns:
        class: A wrapped class with a Kafka producer.
    """
    def decorator(cls):
        class Wrapped(cls):
            def __init__(self, *args, **kwargs):
                self._producer = None
                self._bootstrap_servers = bootstrap_servers
                self._sasl_username = sasl_username
                self._sasl_password = sasl_password
                self._sasl_mechanism = sasl_mechanism
                self._security_protocol = security_protocol
                super().__init__(*args, **kwargs)

            @property
            def producer(self):
                """Lazy initialization of the Kafka producer.

                Returns:
                    Producer: The Kafka producer instance.
                """
                if self._producer is None:
                    self._producer = Producer({
                        'bootstrap.servers': self._bootstrap_servers,
                        'sasl.username': self._sasl_username,
                        'sasl.password': self._sasl_password,
                        'sasl.mechanism': self._sasl_mechanism,
                        'security.protocol': self._security_protocol
                    })
                return self._producer
        return Wrapped
    return decorator


def kafka_consumer(
    bootstrap_servers,
    sasl_username=None,
    sasl_password=None,
    sasl_mechanism="PLAIN",
    security_protocol="SASL_SSL"
):
    """Inject a Kafka consumer into a class.

    Args:
        bootstrap_servers (str): Kafka bootstrap servers.
        sasl_username (str, optional): SASL username for authentication.
        sasl_password (str, optional): SASL password for authentication.
        sasl_mechanism (str, optional): SASL mechanism.
        security_protocol (str, optional): Security protocol.

    Returns:
        class: A wrapped class with a Kafka consumer.
    """
    def decorator(cls):
        class Wrapped(cls):
            def __init__(self, *args, **kwargs):
                self._consumer = None
                self._bootstrap_servers = bootstrap_servers
                self._sasl_username = sasl_username
                self._sasl_password = sasl_password
                self._sasl_mechanism = sasl_mechanism
                self._security_protocol = security_protocol
                super().__init__(*args, **kwargs)

            @property
            def consumer(self):
                """Lazy initialization of the Kafka consumer.

                Returns:
                    Consumer: The Kafka consumer instance.
                """
                if self._consumer is None:
                    self._consumer = Consumer({
                        'bootstrap.servers': self._bootstrap_servers,
                        'sasl.username': self._sasl_username,
                        'sasl.password': self._sasl_password,
                        'sasl.mechanism': self._sasl_mechanism,
                        'security.protocol': self._security_protocol
                    })
                return self._consumer
        return Wrapped
    return decorator


__all__ = [
    "schema_registry",
    "admin_client",
    "kafka_producer",
    "kafka_consumer",
]
