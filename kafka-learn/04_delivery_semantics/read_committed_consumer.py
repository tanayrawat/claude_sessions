"""Stage 4: read revenue.v1 with isolation.level=read_committed -- aborted transactions'
writes never appear here, even though they briefly existed in the partition's log."""
import json

from confluent_kafka import Consumer

TOPIC = "revenue.v1"


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "revenue-readers",
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",  # the default is read_uncommitted!
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
            event = json.loads(msg.value())
            print(f"offset={msg.offset()} -> {event}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
