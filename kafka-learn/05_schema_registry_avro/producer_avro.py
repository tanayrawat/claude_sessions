"""Stage 5: produce Avro-encoded events, registering the schema with Schema Registry.

Run with --schema order_v1.avsc first, then again with --schema order_v2.avsc (which adds
the optional `currency` field) WITHOUT changing consumer_avro.py at all -- that's the point.
"""
import argparse
import sys
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

sys.path.append(str(Path(__file__).resolve().parents[1]))
from common.events import make_event  # noqa: E402

TOPIC = "orders.avro"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="order_v1.avsc")
    args = parser.parse_args()
    schema_path = Path(__file__).parent / args.schema
    schema_str = schema_path.read_text()
    has_currency = "currency" in schema_str

    schema_registry = SchemaRegistryClient({"url": "http://localhost:8081"})
    avro_serializer = AvroSerializer(schema_registry, schema_str, lambda obj, ctx: obj)
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print(f"producing with {args.schema} (currency field: {has_currency})")
    for _ in range(10):
        event = make_event()
        payload = event.to_dict()
        if has_currency:
            payload["currency"] = "USD"
        producer.produce(
            TOPIC,
            key=event.order_id.encode(),
            value=avro_serializer(payload, SerializationContext(TOPIC, MessageField.VALUE)),
        )
        producer.poll(0)
        time.sleep(0.2)
    producer.flush()


if __name__ == "__main__":
    main()
