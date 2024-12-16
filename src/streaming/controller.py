#
# Copyright 2024 The Unnatural Group, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
"""
------------------------------------------------------------------------------
# Tradesignals Streaming Controller
------------------------------------------------------------------------------

##Purpose:
------------------------------------------------------------------------------
    This module is responsible for managing the streaming pipelines and their
    respective consumers and producers.

## Examples:
------------------------------------------------------------------------------

### Starting a Pipeline:

Pipeline configuration is stored in a yaml file typically located in the same
directory as the pipeline python file. Provide a path to the yaml file if it
is not in the same directory as the pipeline python file.

Optionally, override the configuration with a dictionary of values that can be
passed as a keyword argument to the start method.

```python
> # start a pipeline
> from streaming.controller import PipelineController
>
> pipeline_controller = PipelineController()
> pipeline_controller.start("my_pipeline")
```

### Stopping a Pipeline:

```python
> pipeline_controller.stop("my_pipeline")
```

### Deriving a pipeline from a configuration yaml file:

```python
> pipeline_controller.derive("my_pipeline", "my_pipeline.yaml")
```

### Printing the current status of all pipelines:

```python
> print(pipeline_controller.current_status())
```

Result:

```python
> pipeline_controller: {
>     "my_pipeline": {
>         "config": "my_pipeline.yaml",
>         "status": "running",
>         "start_time": "2024-01-01 12:00:00",
>         "end_time": "2024-01-01 12:00:00",
>     }
> }
```

### Restarting a pipeline:

```python
> pipeline_controller.restart("my_pipeline")
```

### Shutting down a pipelines:

Shutting down a pipeline will stop active jobs currently running in the
pipeline and close out any open connections including connections to the schema
registry, kafka brokers, and any other services registered with the pipeline.

```python
> pipeline_controller.shutdown()
```

## Startup Tasks:

The controller will automatically call methods registered with the `@startup`
decorator.

```python
> @startup
> def my_startup_task():
>     print("Startup task complete")
```

## Shutdown Tasks:

The controller will automatically call methods registered with the `@shutdown`
decorator.

```python
> @shutdown
> def my_shutdown_task():
>     print("Shutdown task complete")
```

## Configuration:

The controller will automatically load configuration from a yaml file
located in the same directory as the pipeline python file. Provide a path to
the yaml file if it is not in the same directory as the pipeline python file.

```python
> pipeline_controller.start("my_pipeline", config="my_pipeline.yaml")
```

The configuration can also be overridden with a dictionary of values that can
be passed as a keyword argument to the start method.

```python
> pipeline_controller.start("my_pipeline", config={"my_key": "my_value"})
```

For a full list of configuration options, see the documentation:
[Read the docs](https://docs.tradesignals.ai/streaming/configuration)

## Decorators:

The controller will automatically call methods registered with the `@startup`
decorator.

- @startup:    Called when the pipeline starts.
- @shutdown:   Called when the pipeline stops.
- @on_error:   Called when the pipeline encounters an error.

## Producing Records:

When calling my_pipeline.produce(KafkaMessafe), the controller will automatically call

The controller will automatically call methods registered with the `@producer`
decorator.

To override the default producer, add the `@producer` decorator to a method.

```python
> @producer
> def my_producer(record: Record) -> None:
>     print(record)
```

"""
import json
import os
from typing import Any, Callable, Dict, List, Literal

from attrs import define, field, fields, validate
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from dotenv_derive import dotenv

from pipelines.kafka.decorators import admin, consumer, producer, schema_registry

SchemaRegistryClient = SchemaRegistryClient
Consumer = Consumer
Producer = Producer


@schema_registry
@admin
@consumer
@producer
@dotenv
class DummyClass:
    """Dummy class for testing."""
    pass


field_base_types = Literal[
    "string", "int", "long", "float", "double", "fixed", "enum", "record",
    "array", "map", "union", "error", "bytes"
]

field_logical_types = Literal[
    "decimal", "timestamp-millis", "timestamp-micros", "timestamp-nanos",
    "date", "time-millis", "time-micros", "time-nanos"
]


def transform_field(field_obj) -> Dict[str, Any]:
    """Transform field object to Avro schema field.

    Args:
        field_obj (attrs.Attribute): The field object.

    Returns:
        Dict[str, Any]: The Avro schema field.

    """
    field_metadata = field_obj.metadata if hasattr(field_obj, 'metadata') else {}
    field_type = field_metadata.get('type', 'string')
    field_doc = field_metadata.get('doc', '')
    field_default = field_metadata.get('default', None)
    field_logical_type = field_metadata.get('logical_type', None)

    if field_logical_type:
        field_type = {
            "type": field_type,
            "logicalType": field_logical_type,
            **{k: v for k, v in field_metadata.items() if k not in ['type', 'logical_type']}
        }

        field_schema = {
            "name": field_obj.name,
            "type": field_type,
            "doc": field_doc
        }

        if field_default is not None:
            field_schema["default"] = field_default

    return field_schema


def avro_schema(cls):
    """Decorator to generate Avro schema for a class.

    Args:
        cls (type): The class to decorate.

    Returns:
        type: The decorated class with schema properties.
    """
    cls._doc: str = field(default="", alias="doc")
    cls._name: str = field(default="", alias="name")
    cls._namespace: str = field(default="", alias="namespace")

    def avro_schema_dict(instance) -> Dict[str, Any]:
        """Generate the Avro schema dictionary from the class instance.

        Args:
            instance (object): The class instance.

        Returns:
            Dict[str, Any]: The Avro schema dictionary.
        """
        field_list = [transform_field(f) for f in fields(instance)]
        return {
            "type": "record",
            "name": instance._name,
            "namespace": instance._namespace,
            "doc": instance._doc,
            "fields": field_list
        }

    def transform_field(field_obj):
        """Transform field object to Avro schema field.

        Args:
            field_obj (attrs.Attribute): The field object.

        Returns:
            dict: The Avro schema field.
        """
        field_metadata = field_obj.metadata if hasattr(field_obj, "metadata") else {}
        field_type = field_metadata.get("type", "string")
        field_doc = field_metadata.get("doc", "")
        field_default = field_metadata.get("default", None)
        field_logical_type = field_metadata.get("logical_type", None)

        if field_logical_type:
            field_type = {
                "type": field_type,
                "logicalType": field_logical_type,
                **{k: v for k, v in field_metadata.items() if k not in ["type", "logical_type"]},
            }

            field_schema = {
                "name": field_obj.name,
                "type": field_type,
                "doc": field_doc
            }

            if field_default is not None:
                field_schema["default"] = field_default

        return field_schema

    def avro_schema_str(instance) -> str:
        """Generate the Avro schema string from the class instance.

        Args:
            instance (object): The class instance.

        Returns:
            str: The Avro schema string.
        """
        return json.dumps(instance.schema_dict, indent=2)

    cls.schema_dict = property(avro_schema_dict)
    cls.schema_str = property(avro_schema_str)

    def export_schema(self, file_path: str):
        """Export the schema to a file.

        Args:
            file_path (str): The full path to the file.

        Returns:
            None

        Raises:
            ValueError: If the file path is invalid.
        """

        if not os.path.isabs(file_path):
            raise ValueError("The file path is not valid.")

        with open(file_path, 'w') as file:
            json.dump(self.schema_dict, file, indent=2)

    return cls


class Field(bound=field):
    base_type: avro_base_types
    logical_type: avro_logical_types
    preprocessor: Callable[[Any], Any]
    initializer: Callable[[Any], Any]
    extra_configs: Dict[str, Any]

@avro_schema
class TradeSchema:
    price: Field = Field(base_type="bytes", logical_type="decimal", extra_configs={"precision": 16, "scale": 4})

@define
class AvroLogicalTypeMetadata:
    logical_type: str = field(default="decimal", alias="logicalType")
from typing import Any, Callable, Dict

from attrs import define, field


def logical_type(
    base: str,
    logical: str,
    preprocessor: Callable,
    initializer: Callable,
    **extra_configs
):
    """Decorator to define logical types for Avro serialization/deserialization.

    Args:
        base (str): The base Avro type.
        logical (str): The logical Avro type.
        preprocessor (Callable): Function to process data before serialization.
        initializer (Callable): Function to process data after deserialization.
        **extra_configs: Additional configurations for the logical type.

    Returns:
        Callable: A class decorator that adds logical type metadata.
    """
    def decorator(cls):
        cls.base: Literal[
            "string",
            "int",
            "long",
            "float",
            "double",
            "fixed",
            "enum",
            "record",
            "array",
            "map",
            "union",
            "error",
            "bytes"
        ] = base
        cls.logical: Literal[
            "decimal", "timestamp-millis", "timestamp-micros", "timestamp-nanos", "date", "time-millis", "time-micros", "time-nanos"
        ] = logical
        cls.preprocessor: Callable[..., Any] = preprocessor
        cls.initializer: Callable[..., Any] = initializer
        cls.extra_configs: Dict[str, Any] = extra_configs
        return cls
    return decorator


from decimal import Decimal


@logical_type(base="bytes", logical="decimal", extra_configs={"precision": 16, "scale": 4})
@define
class QuotePrice(Decimal):
    schema_type: str | AvroLogicalTypeMetadata = AvroLogicalTypeMetadata(
        logical_type="decimal", properties={"precision": 16, "scale": 4}
    )


@logical_type(
    avro_base_type="int",
    avro_logical_type="timestamp-millis",
    pre_processor=lambda x: x,  # Example pre-processor
    post_processor=lambda x: x  # Example post-processor
)
@define
class TimestampMillisLogicalTypeMetadata:
    avro_type: str | AvroLogicalTypeMetadata = AvroLogicalTypeMetadata(
        logical_type="timestamp-millis"
    )

@logical_type(
    avro_base_type="int",
    avro_logical_type="timestamp-micros",
    pre_processor=lambda x: x,  # Example pre-processor
    post_processor=lambda x: x  # Example post-processor
)
@define
class TimestampMicrosLogicalTypeMetadata:
    avro_type: str | AvroLogicalTypeMetadata = AvroLogicalTypeMetadata(
        logical_type="timestamp-micros"
    )

@logical_type(
    avro_base_type="int",
    avro_logical_type="timestamp-nanos",
    pre_processor=lambda x: x,  # Example pre-processor
    post_processor=lambda x: x  # Example post-processor
)
@define
class TimestampNanosLogicalTypeMetadata:
    avro_type: str | AvroLogicalTypeMetadata = AvroLogicalTypeMetadata(
        logical_type="timestamp-nanos"
    )

@logical_type(
    avro_base_type="int",
    avro_logical_type="date",
    pre_processor=lambda x: x,  # Example pre-processor
    post_processor=lambda x: x  # Example post-processor
)
@define
class DateLogicalTypeMetadata:
    avro_type: str | AvroLogicalTypeMetadata = AvroLogicalTypeMetadata(
        logical_type="date"
    )
    avro_type: str | AvroLogicalTypeMetadata = AvroLogicalTypeMetadata(logical_type="date")

@define
class TimeLogicalTypeMetadata:
    avro_type: str | AvroLogicalTypeMetadata = AvroLogicalTypeMetadata(logical_type="time-millis")

@define
class TimeMicrosLogicalTypeMetadata:
    avro_type: str | AvroLogicalTypeMetadata = AvroLogicalTypeMetadata(logical_type="time-micros")

@define
class TimeNanosLogicalTypeMetadata:
    avro_type: str | AvroLogicalTypeMetadata = AvroLogicalTypeMetadata(logical_type="time-nanos")

@avro_model
@define
class OptionTradeRecord:
    symbol: str = field(
        default="",
        metadata={"avro_type": "string"},
        validator=validate(
            lambda s:
                len(s) > 0 and len(s) < 10,
                "Symbol must be between 1 and 10 characters"
        ),
    )
    price: float = field(
        default=0.0,
        metadata={"avro_type": "bytes", "logicalType": "decimal", "precision": 16, "scale": 4},
        validator=validate(lambda p: p > 0, "Price must be greater than 0"),
    )

@pipeline
class MyPipeline:

    # override the default consumer
    @consumer
    def my_consumer(
        self,
        record: Record,
        key_schema: KeySchema,
        value_schema: ValueSchema,
        default_headers: get_default_headers,
        topics: List[str],  # list of topics that the consumer subscribes to
    ) -> None:
        print(record)

    # override the default producer
    @producer
    def my_producer(self, record: Record) -> None:
        print(record)
