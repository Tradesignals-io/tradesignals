"""Wrapper for Avro serialization using Confluent Kafka's MessageSerializer."""
from datetime import datetime
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import Any, Dict

# type: ignore
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient

# from confluent_kafka.schema_registry.avro import AvroSerializer  # type: ignore
from pipelines.kafka.pipeline_manager import PipelineManager  # type: ignore


def coerce_decimal(value: Decimal, precision: int, scale: int) -> Decimal:
    """Coerce a Decimal to match the specified precision and scale.

    Args:
        value: The Decimal value to be coerced.
        precision: The total number of significant digits.
        scale: The number of digits to the right of the decimal point.

    Returns:
        The coerced Decimal value.

    Raises:
        ValueError: If the value cannot be coerced to the specified precision.
        TypeError: If the value is not a Decimal.

    Examples:
        >>> schema_precision = 5
        >>> schema_scale = 2

        >>> value = Decimal("123.4567")
        >>> try:
        >>>     coerced_value = coerce_decimal(value, schema_precision, schema_scale)
        >>>     print(f"Coerced value: {coerced_value}")
        >>> except ValueError as e:
        >>>     print(f"Error: {e}")

    """
    if not isinstance(value, Decimal):
        raise TypeError("Value must be a Decimal.")

    # Step 1: Adjust scale
    scale_factor = Decimal("1").scaleb(-scale)  # e.g., 10^-scale
    try:
        coerced_value = value.quantize(scale_factor, rounding=ROUND_DOWN)
    except InvalidOperation as err:
        raise ValueError(f"Cannot coerce {value} to scale {scale}.") from err

    # Step 2: Validate precision
    total_digits = len(coerced_value.as_tuple().digits)  # Total digits in the number
    if total_digits > precision:
        raise ValueError(f"Value {coerced_value} exceeds precision {precision}.")

    return coerced_value


def coerce_timestamp(value: int, precision: str) -> int:
    """Coerce a timestamp integer to match the specified precision.

    Args:
        value: The timestamp value to be coerced.
        precision: The precision type ('seconds', 'milliseconds', etc.).

    Returns:
        The coerced timestamp value.

    Raises:
        ValueError: If the precision type is not supported.

    Examples:
        >>> timestamp = 1633072800
        >>> coerced_timestamp = coerce_timestamp(timestamp, 'milliseconds')
        >>> print(coerced_timestamp)
    """
    if precision == 'seconds':
        return value
    elif precision == 'milliseconds':
        return value * 1000
    elif precision == 'microseconds':
        return value * 1000000
    else:
        raise ValueError(f"Unsupported precision type: {precision}")


def coerce_time(value: datetime, precision: str) -> int:
    """Coerce a datetime object to extract time based on the schema precision.

    Args:
        value: The datetime object to be coerced.
        precision: The precision type ('seconds', 'milliseconds', etc.).

    Returns:
        The coerced time value.

    Examples:
        >>> dt = datetime(2023, 10, 1, 12, 0, 0)
        >>> coerced_time = coerce_time(dt, 'milliseconds')
        >>> print(coerced_time)
    """
    epoch = datetime.utcfromtimestamp(0)
    delta = value - epoch
    if precision == 'seconds':
        return int(delta.total_seconds())
    elif precision == 'milliseconds':
        return int(delta.total_seconds() * 1000)
    elif precision == 'microseconds':
        return int(delta.total_seconds() * 1000000)
    else:
        raise ValueError(f"Unsupported precision type: {precision}")


def validate_utf8_and_trim(value: str) -> str:
    """Validate UTF-8 encoding and trim whitespace from a string.

    Args:
        value: The string to validate and trim.

    Returns:
        The validated and trimmed string.

    Raises:
        ValueError: If the string is not valid UTF-8.
    """
    try:
        value.encode('utf-8')
    except UnicodeEncodeError:
        raise ValueError("String is not valid UTF-8.")
    return value.strip()


def serialize_avro_record(
    record: Dict[str, Any],
    schema: Dict[str, Any],
    schema_registry_client: SchemaRegistryClient,
    producer_topic: str | None = None,
    consumer_topic: str | None = None,
) -> bytes:
    """Serialize a record to Avro format using Confluent Kafka's MessageSerializer.

    Args:
        record: The record to serialize.
        schema: The Avro schema to use for serialization.
        schema_registry_client: The schema registry client to use for serialization.
        producer_topic: The topic to use for serialization.
        consumer_topic: The topic to use for serialization.

    Returns:
        The serialized Avro record as bytes.

    Raises:
        ValueError: If the record cannot be serialized.
    """
    # Coerce the record to be Avro-ready
    avro_ready_record = make_avro_ready(record, schema)
    # Initialize the schema registry client and serializer
    schema_registry_client = PipelineManager(
        producer_topic=producer_topic,
        consumer_topic=consumer_topic,
    ).schema_registry_client
    avro_serializer = MessageSerializer(schema_registry_client)

    # Serialize the record
    try:
        serialized_record = avro_serializer.encode_record_with_schema(
            producer_topic,
            schema,
            avro_ready_record,
            is_key=False
        )
    except Exception as e:
        raise ValueError(f"Failed to serialize record: {e}") from e

    return serialized_record


def make_avro_ready(
    record: Dict[str, Any],
    schema: Dict[str, Any],
) -> Dict[str, Any]:
    """Make a record avro-ready.

    Args:
        record: The record to make avro-ready.
        schema: The schema to use for coercion.

    Returns:
        The avro-ready record.

    """
    for field in schema["fields"]:
        field_name = field["name"]
        if field_name in record:
            if field["type"] == "bytes" and field.get("logicalType") == "decimal":
                record[field_name] = coerce_decimal(
                    record[field_name],
                    field["precision"],
                    field["scale"],
                )
            elif field["type"] == "long" and field.get("logicalType") == "timestamp-millis":
                record[field_name] = coerce_timestamp(record[field_name], 'milliseconds')
            elif field["type"] == "long" and field.get("logicalType") == "timestamp-micros":
                record[field_name] = coerce_timestamp(record[field_name], 'microseconds')
            elif field["type"] == "int" and field.get("logicalType") == "time-millis":
                record[field_name] = coerce_time(record[field_name], 'milliseconds')
            elif field["type"] == "int" and field.get("logicalType") == "time-micros":
                record[field_name] = coerce_time(record[field_name], 'microseconds')
            elif field["type"] == "string":
                record[field_name] = validate_utf8_and_trim(record[field_name])
    return record


__all__ = [
    "serialize_avro_record",
    "make_avro_ready",
    "coerce_decimal",
    "coerce_timestamp",
    "coerce_time",
    "validate_utf8_and_trim",
]
