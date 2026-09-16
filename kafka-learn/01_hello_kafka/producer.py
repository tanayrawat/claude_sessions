"""Stage 1: the simplest possible producer -- one broker, one topic, fire-and-forget."""
import json
import sys
import time
from pathlib import Path

from confluent_kafka import Producer

sys.path.append(str(Path(__file__).resolve().parents[1]))
from common.events import make_event  # noqa: E402

TOPIC = "orders.v1"


def delivery_report(err, msg):
    if err is not None:
        print(f"! delivery failed: {err}")
    else:
        print(f"delivered -> partition={msg.partition()} offset={msg.offset()}")


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    for _ in range(20):
        event = make_event()
        producer.produce(
            TOPIC,
            value=json.dumps(event.to_dict()).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)  # let delivery callbacks fire without blocking the loop
        time.sleep(0.3)
    producer.flush()  # block here until every in-flight message is acknowledged


if __name__ == "__main__":
    main()
