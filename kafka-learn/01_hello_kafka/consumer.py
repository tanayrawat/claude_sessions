"""Stage 1: read everything from the beginning and print it."""
import json

from confluent_kafka import Consumer

TOPIC = "orders.v1"


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "hello-kafka-readers",
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
                print(f"! consumer error: {msg.error()}")
                continue
            event = json.loads(msg.value())
            key = msg.key().decode() if msg.key() else None
            print(f"partition={msg.partition()} offset={msg.offset()} key={key} -> {event}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
