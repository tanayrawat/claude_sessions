"""Stage 2: consume and prove every key always shows up on the same partition."""
import json

from confluent_kafka import Consumer

TOPIC = "orders.keyed"


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "keyed-readers",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])
    seen_partition_for_key = {}
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"! consumer error: {msg.error()}")
                continue
            key = msg.key().decode()
            partition = msg.partition()
            event = json.loads(msg.value())

            prior = seen_partition_for_key.get(key)
            seen_partition_for_key[key] = partition
            note = "" if prior in (None, partition) else f"  <-- MOVED from partition {prior}!"

            print(f"key={key} partition={partition} status={event['status']}{note}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
