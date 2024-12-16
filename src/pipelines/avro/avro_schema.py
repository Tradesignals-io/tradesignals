# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Copyright 2023 The Unnatural Group, LLC
#
# Attribution to Tradesignals. For additional resources and docs, visit
# dev.tradesignals.io

import json
import os
from decimal import Decimal
from typing import Any, Dict, Literal

from attrs import define, field, fields

field_base_types = Literal[
    "string", "int", "long", "float", "double", "fixed", "enum", "record",
    "array", "map", "union", "error", "bytes"
]

field_logical_types = Literal[
    "decimal",
    "timestamp-millis",
    "timestamp-micros",
    "timestamp-nanos",
    "date",
    "time-millis",
    "time-micros",
    "time-nanos"
]

types_to_logical_types = {
    "string": "string",
    "int": "long",
    "long": "long",
    "float": "float",
    "double": "double",
    "fixed": "fixed",
    "enum": "enum",
    "record": "record",
    "array": "array",
    "map": "map",
    "union": "union",
    "error": "error",
    "bytes": "bytes"
}

logical_types_to_types = {
    "decimal": "bytes",
    "timestamp-millis": "long",
    "timestamp-micros": "long",
    "timestamp-nanos": "long",
    "date": "string",
    "time-millis": "long",
    "time-micros": "long",
    "time-nanos": "long"
}

logical_type_required_metadata = {
    "decimal": ["precision", "scale"],
    "timestamp-millis": [],
    "timestamp-micros": [],
    "timestamp-nanos": [],
    "date": [],
    "time-millis": [],
    "time-micros": [],
    "time-nanos": []
}

from cattrs import Converter

# Initialize a cattrs converter
converter = Converter()

@define
class AvroField:
    avro_name: str
    avro_field_type: str
    avro_logical_type: str = None
    avro_scale: int = None
    avro_precision: int = None
    avro_min_length: int = None
    avro_max_length: int = None
    avro_doc: str = None
    avro_default: Any = None
    avro_extra_type_info: Dict[str, Any] = field(factory=dict)

    converter = Converter()

    def __attrs_post_init__(self):
        """Post-initialize the AvroField instance.

        This method registers the unstructure hook for
        the AvroField instance.
        """
        self.converter.register_unstructure_hook(
            AvroField, avrofield_unstructure
        )

def avrofield_unstructure(avro_field: AvroField) -> Dict[str, Any]:
    """Convert AvroField to standard Avro field schema format.

    Args:
        avro_field (AvroField): The AvroField instance to convert.

    Returns:
        Dict[str, Any]: The Avro field schema representation.
    """
    schema = {
        "name": avro_field.avro_name,
    }

    type_obj = {
        "type": avro_field.avro_field_type,
    }

    if avro_field.avro_logical_type:
        type_obj["logicalType"] = avro_field.avro_logical_type

    if avro_field.avro_scale:
        type_obj["scale"] = avro_field.avro_scale

    if avro_field.avro_precision:
        type_obj["precision"] = avro_field.avro_precision

    if avro_field.avro_min_length:
        type_obj["minLength"] = avro_field.avro_min_length

    if avro_field.avro_max_length:
        type_obj["maxLength"] = avro_field.avro_max_length

    if avro_field.avro_doc:
        type_obj["doc"] = avro_field.avro_doc

    if avro_field.avro_default:
        type_obj["default"] = avro_field.avro_default

    if avro_field.avro_extra_type_info:
        type_obj.update(avro_field.avro_extra_type_info)

    schema = {
        "name": avro_field.avro_name,
        "type": type_obj,
        "default": avro_field.avro_default
    }
    return schema

# Register the converter for AvroField
converter.register_unstructure_hook(AvroField, avrofield_unstructure)

def get_field_type(field_type: str) -> field_base_types:
    """Get the field type.

    The type is inferred first from the field's type attribute, then from the
    field's metadata.

    If the type is not a valid field type, an error is raised.

    Args:
        field_type (str): The field type.

    Returns:
        field_base_types: The field type.

    Raises:
        ValueError: If the field type is invalid.
        ValueError: If the field has no type.
    """
    if field_type not in types_to_logical_types:
        raise ValueError(f"Invalid field type: {field_type}")

    if hasattr(field_type, "type"):
        return field_type.type
    elif hasattr(field_type, "metadata"):
        logical_type = field_type.metadata.get("logicalType") or \
                       field_type.metadata.get("logical_type")
        if logical_type:
            return types_to_logical_types[logical_type]
        elif "type" in field_type.metadata:
            return field_type.metadata["type"]
    raise ValueError(f"Field {field_type.name} has no type")

def get_field_type_info(field_obj) -> Dict[str, Any]:
    """Get the metadata of a field.

    Args:
        field_obj (attrs.Attribute): The field object.

    Returns:
        Dict[str, Any]: The metadata of the field.

    Raises:
        ValueError: If the field has no metadata or is missing a
            metadata field required by the logical type.
    """
    metadata = {
        "type": {
            "type": get_field_type(field_obj),
        }
    }
    if hasattr(field_obj, "metadata"):
        metadata = field_obj.metadata
        logical_type = metadata.get("logicalType") or \
            metadata.get("logical_type")
        if logical_type and logical_type in logical_type_required_metadata:
            metadata["type"]["logicalType"] = logical_type
            required_metadata = logical_type_required_metadata[logical_type]
            for key in required_metadata:
                if key not in metadata:
                    raise ValueError(
                        f"Field {field_obj.name} is missing required "
                        f"metadata: {key}"
                    )
                metadata["type"][key] = metadata[key]
        return metadata
    raise ValueError(f"Field {field_obj.name} has no metadata")

def get_doc(field_obj) -> str:
    """Get the documentation for a field.

    Args:
        field_obj (attrs.Attribute): The field object.

    Returns:
        str: The documentation for the field.

    Raises:
        ValueError: If the field has no doc attribute.
    """
    if hasattr(field_obj, "doc"):
        return field_obj.doc
    raise ValueError(f"Field {field_obj.name} has no doc")

def get_default(field_obj) -> Any:
    """Get the default value for a field.

    Args:
        field_obj (attrs.Attribute): The field object.

    Returns:
        Any: The default value for the field.
    """
    if hasattr(field_obj, "default"):
        return field_obj.default
    return None

def get_logical_type(field_obj) -> field_logical_types:
    """Get the logical type of a field.

    Args:
        field_obj (attrs.Attribute): The field object.

    Returns:
        field_logical_types: The logical type of the field.

    Raises:
        ValueError: If the field has no logical type.
    """
    field_type = get_field_type(field_obj)
    if field_type == "bytes" or field_type == "fixed":
        if hasattr(field_obj, "logical_type"):
            return field_obj.logical_type
        elif hasattr(field_obj, "metadata"):
            if "logical_type" in field_obj.metadata:
                return field_obj.metadata["logical_type"]
            elif "logicalType" in field_obj.metadata:
                return field_obj.metadata["logicalType"]
    raise ValueError(f"Field {field_obj.name} has no logical type")

def transform_field_to_schema(field_obj) -> Dict[str, Any]:
    """Transform a field object to an Avro schema field dictionary.

    Args:
        field_obj (attrs.Attribute): The field object.

    Returns:
        Dict[str, Any]: The Avro schema field.

    Raises:
        ValueError: If the field has no type.
        ValueError: If the field has an invalid type.
        ValueError: If the field has an invalid logical type.
        ValueError: If the field has no scale or precision for a logical type.
    """
    field_metadata = field_obj.metadata if hasattr(field_obj, "metadata") else {}
    field_type = None
    field_logical_type = None

    # Infer field type if not explicitly provided
    if not field_metadata:
        if isinstance(field_obj.default, str):
            field_type = "string"
        elif isinstance(field_obj.default, int):
            field_type = "int"
        elif isinstance(field_obj.default, float):
            field_type = "float"
        elif isinstance(field_obj.default, Decimal):
            field_type = "bytes"
            field_logical_type = "decimal"
            field_metadata["precision"] = 10  # Default precision
            field_metadata["scale"] = 2  # Default scale
        else:
            raise ValueError(f"Cannot infer type for field {field_obj.name}")

    if hasattr(field_obj, "type"):
        field_type = field_obj.type
    else:
        if field_metadata:
            if "type" in field_metadata:
                field_type = field_metadata["type"]
            elif "logical_type" in field_metadata:
                field_type = field_metadata["logical_type"]
            elif "logicalType" in field_metadata:
                field_type = get_logical_type(field_metadata["logicalType"])

    if field_type is None:
        raise ValueError(f"Field {field_obj.name} has no type")

    if field_type not in field_base_types.__args__:
        raise ValueError(f"Field {field_obj.name} has invalid type: {field_type}")

    field_doc = None
    field_default = None

    if hasattr(field_obj, "default"):
        field_default = field_obj.default

    if hasattr(field_obj, "doc"):
        field_doc = field_obj.doc

    if hasattr(field_obj, "logical_type"):
        field_logical_type = field_obj.logical_type
    elif field_metadata:
        if "logical_type" in field_metadata:
            field_logical_type = field_metadata["logical_type"]
        elif "logicalType" in field_metadata:
            field_logical_type = field_metadata["logicalType"]

    if hasattr(field_obj, "precision"):
        field_metadata["precision"] = field_obj.precision
    elif field_metadata:
        if "precision" in field_metadata:
            field_metadata["precision"] = field_metadata["precision"]

    if hasattr(field_obj, "scale"):
        field_metadata["scale"] = field_obj.scale
    elif field_metadata:
        if "scale" in field_metadata:
            field_metadata["scale"] = field_metadata["scale"]

    if field_logical_type is not None:
        if field_logical_type not in field_logical_types.__args__:
            raise ValueError(f"Field {field_obj.name} has invalid logical type: {field_logical_type}")

        for req_field in logical_type_required_metadata[field_logical_type]:
            if req_field not in field_metadata:
                raise ValueError(
                    f"Field {field_obj.name} has no {req_field} for logical type {field_logical_type}"
                )
    field_schema = {}
    field_schema["name"] = field_obj.name
    field_schema["type"] = field_type if field_type and field_logical_type is None else {
        "type": field_type,
        "logicalType": field_logical_type,
        **{k: v for k, v in field_metadata.items()
            if k not in ["type", "logical_type"]}
    }
    field_schema["doc"] = field_doc if field_doc else None
    field_schema["default"] = field_default if field_default else None
    return field_schema

    if field_default is not None:
        field_schema["default"] = field_default
    return field_schema

def avro_model(name: str, namespace: str, doc: str):
    """Turn a class into an Avro schema with enhanced metadata.

    Args:
        name (str): The name of the Avro schema.
        namespace (str): The namespace of the Avro schema.
        doc (str): The documentation for the Avro schema.

    Returns:
        Callable: A decorator that adds Avro schema metadata to a class.
    """
    def decorator(cls):
        cls = define(cls)  # Inherit all attributes from attrs' define
        cls._name = name
        cls._namespace = namespace
        cls._doc = doc

        def avro_schema_dict(self) -> Dict[str, Any]:
            """Generate Avro schema dictionary from class instance.

            Args:
                self (object): The instance of the class.

            Returns:
                Dict[str, Any]: The Avro schema dictionary.
            """
            field_list = [transform_field_to_schema(f) for f in fields(cls)]
            return {
                "type": "record",
                "name": self._name,
                "namespace": self._namespace,
                "doc": self._doc,
                "fields": field_list
            }

        def export_schema(self, file_path: str) -> None:
            """Export the schema to a file.

            Args:
                self (object): The instance of the class.
                file_path (str): The path to the file to export the schema to.

            Raises:
                ValueError: If the file path is not valid.
            """
            if not os.path.isabs(file_path):
                raise ValueError("The file path is not valid.")
            with open(file_path, 'w') as file:
                json.dump(self.schema_dict, file, indent=2)

        cls.schema_dict = property(avro_schema_dict)
        cls.schema_str = property(lambda self: json.dumps(self.schema_dict, indent=2))
        cls.export_schema = export_schema
        cls.name = property(lambda self: self._name)
        cls.namespace = property(lambda self: self._namespace)
        cls.doc = property(lambda self: self._doc)

        return cls

    return decorator

@avro_model(
    name="ExampleSchema",
    namespace="com.tradesignals.example",
    doc="Example schema for demonstration purposes."
)
class ExampleSchema:
    """Do something cool.

    This class demonstrates how to define an Avro schema using the avro_schema
    decorator.

    Attributes:
        field1 (str): An example string field.
        field2 (int): An example integer field.
        field3 (float): An example float field.
        field4 (bool): An example boolean field.
        field6 (Decimal): An example decimal field.
    """

    field1: AvroField = AvroField(avro_name="field1", avro_field_type="string", avro_default="default_value")
    field2: AvroField = AvroField(avro_name="field2", avro_field_type="int", avro_default=0)
    field3: AvroField = AvroField(avro_name="field3", avro_field_type="float", avro_default=0.0)
    field4: AvroField = AvroField(avro_name="field4", avro_field_type="boolean", avro_default=True)
    field6: AvroField = AvroField(avro_name="field6", avro_field_type="bytes", avro_logical_type="decimal", avro_precision=10, avro_scale=2, avro_default=Decimal("12345.67"))

# Example usage
example_instance = ExampleSchema()
print(example_instance.schema_str)

from decimal import Decimal, InvalidOperation, getcontext


def coerce_record_to_avro_bytes(avro_model_instance, record):
    """Coerce record dictionary to Avro bytes based on configured scale and precision.

    Args:
        avro_model_instance (object): An instance of the avro_model decorated class.
        record (dict): A dictionary containing the record data.

    Returns:
        dict: A dictionary with the coerced data.

    Raises:
        ValueError: If the scale and precision attributes don't exist.
    """
    coerced_record = {}
    for field_name, field_value in record.items():
        field_attr = getattr(avro_model_instance, field_name, None)
        if field_attr and field_attr.metadata:
            field_metadata = field_attr.metadata
            if field_metadata.get("logicalType") == "decimal":
                scale = field_metadata.get("scale")
                precision = field_metadata.get("precision")
                if scale is None or precision is None:
                    raise ValueError(
                        f"Scale and precision must be defined for field {field_name}"
                    )
                try:
                    getcontext().prec = precision
                    decimal_value = Decimal(field_value).scaleb(-scale)
                    coerced_record[field_name] = decimal_value.to_integral_value()
                except InvalidOperation as e:
                    raise ValueError(
                        f"Invalid decimal value for field {field_name}: {e}"
                    ) from e
            else:
                coerced_record[field_name] = field_value
        else:
            coerced_record[field_name] = field_value
    return coerced_record


# Example usage
example_record = {
    "field1": "example_string",
    "field2": 123,
    "field3": 45.67,
    "field4": True,
    "field6": "12345.67"
}

my_instance = ExampleSchema()
my_instance.field1 = "example_string"
my_instance.field2 = 123
my_instance.field3 = 45.67
my_instance.field4 = True
my_instance.field6 = Decimal("12345.67")

try:
    coerced_record = coerce_record_to_avro_bytes(example_instance, example_record)
    print(coerced_record)
except ValueError as e:
    print(e)
    print(e)
