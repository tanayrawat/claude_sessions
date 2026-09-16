"""Stage 2: produce WITH a key so every event for the same order lands in the same partition."""
import json
import sys
import time
from pathlib import Path

from confluent_kafka import Producer

sys.path.append(str(Path(__file__).resolve().parents[1]))
from common.events import make_event, random_order_id  # noqa: E402

TOPIC = "orders.keyed"


def delivery_report(err, msg):
    if err is not None:
        print(f"! delivery failed: {err}")
        return
    print(f"key={msg.key().decode()} -> partition={msg.partition()} offset={msg.offset()}")


def main():
    producer = Producer({"bootstrap.servers": "localhost:19092"})
    # walk each of a handful of orders through its lifecycle, out of order on the wire,
    # to prove the partition (not send order) is what preserves per-key ordering
    order_ids = [random_order_id(pool_size=6) for _ in range(6)]
    for _ in range(24):
        order_id = order_ids[_ % len(order_ids)]
        event = make_event(order_id=order_id)
        producer.produce(
            TOPIC,
            key=event.order_id.encode("utf-8"),
            value=json.dumps(event.to_dict()).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(0.15)
    producer.flush()


if __name__ == "__main__":
    main()
