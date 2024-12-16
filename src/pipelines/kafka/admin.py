from confluent_kafka.admin import AdminClient, NewTopic

# a = AdminClient({"bootstrap.servers": "mybroker"})
# new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in ["topic1", "topic2"]]
# # Note: In a multi-cluster production scenario, it is more typical to use a replication_factor of 3 for durability.
# # Call create_topics to asynchronously create topics. A dict
# # of <topic,future> is returned.
# fs = a.create_topics(new_topics)
# # Wait for each operation to finish.
# for topic, f in fs.items():
#     try:
#         f.result()  # The result itself is None
#         print("Topic {} created".format(topic))
#     except Exception as e:
#         print("Failed to create topic {}: {}".format(topic, e))
from fastavro import writer


def process_record(record, schema):
    """
    Process a record to ensure all Decimal fields comply with the Avro schema.
    """
    for field in schema["fields"]:
        if field["type"] == "bytes" and "logicalType" in field and field["logicalType"] == "decimal":
            precision = field["precision"]
            scale = field["scale"]
            field_name = field["name"]
            if field_name in record:
                record[field_name] = coerce_decimal(record[field_name], precision, scale)
    return record


# Define your Avro schema
schema = {
    "type": "record",
    "name": "Test",
    "fields": [{"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 2}}],
}

# Example record
record = {"amount": Decimal("123.4567")}

# Process the record
processed_record = process_record(record, schema)

# Serialize with Avro
with open("output.avro", "wb") as out:
    writer(out, schema, [processed_record])

__all__ = ["AvroProducer"]
