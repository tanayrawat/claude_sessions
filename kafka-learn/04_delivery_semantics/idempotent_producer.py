"""Stage 4: idempotence stops the PRODUCER from creating duplicates on its own retries --
it does NOT stop your application code from calling produce() twice for the same event.

Run with --duplicate-bug to see the trap: idempotence is on, but we still get two copies
in the topic, because the "duplicate" happened at the call site, not on the wire.
"""
import argparse
import json
import sys
import time
from pathlib import Path

from confluent_kafka import Producer

sys.path.append(str(Path(__file__).resolve().parents[1]))
from common.events import make_event  # noqa: E402

TOPIC = "orders.idempotent"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--duplicate-bug",
        action="store_true",
        help="simulate an application bug that calls produce() twice for one logical event",
    )
    args = parser.parse_args()

    producer = Producer(
        {
            "bootstrap.servers": "localhost:19092",
            "enable.idempotence": True,  # dedupes retries caused by broker-side timeouts/retries
            "acks": "all",  # required by, and implied by, enable.idempotence
        }
    )

    for _ in range(10):
        event = make_event()
        payload = json.dumps(event.to_dict()).encode("utf-8")
        producer.produce(TOPIC, key=event.order_id.encode(), value=payload)
        if args.duplicate_bug:
            # this is a bug in the CALLING code, not a broker retry --
            # idempotence has no way to know these two produce() calls "mean" the same event
            producer.produce(TOPIC, key=event.order_id.encode(), value=payload)
        producer.poll(0)
        time.sleep(0.2)

    producer.flush()
    print("done -- compare message counts in the topic with/without --duplicate-bug")


if __name__ == "__main__":
    main()
