"""Stage 5: this consumer is written ONCE and never changes between order_v1 and order_v2
producer runs. The AvroDeserializer pulls whichever writer schema was used for each message
straight from Schema Registry (its id travels in the first few bytes of the payload), so
old and new messages both deserialize correctly side by side."""
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

TOPIC = "orders.avro"


def main():
    schema_registry = SchemaRegistryClient({"url": "http://localhost:8081"})
    avro_deserializer = AvroDeserializer(schema_registry, from_dict=lambda obj, ctx: obj)

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:19092",
            "group.id": "avro-readers",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"! error: {msg.error()}")
                continue
            event = avro_deserializer(msg.value(), SerializationContext(TOPIC, MessageField.VALUE))
            currency = event.get("currency", "<not in this message's schema>")
            print(f"order={event['order_id']} amount={event['amount']} currency={currency}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
