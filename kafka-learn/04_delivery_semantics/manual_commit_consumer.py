"""Stage 4: at-most-once vs at-least-once, controlled by WHEN you commit relative to processing.

    python 04_delivery_semantics/manual_commit_consumer.py --commit before   # at-most-once
    python 04_delivery_semantics/manual_commit_consumer.py --commit after    # at-least-once

Run it, let it print ~8 messages, then Ctrl+C it *before* it reaches the next commit boundary,
then run the exact same command again and watch what happens at the boundary you crossed:

  --commit after   -> the last few messages before your Ctrl+C get printed AGAIN (duplicates:
                      they were processed but the offset commit never happened).
  --commit before  -> the last few messages before your Ctrl+C are simply GONE (silently lost:
                      the offset was already committed before you got around to processing them).

This is the actual tradeoff "at-least-once" vs "at-most-once" describes -- it's not a client
setting you flip, it's a consequence of your own commit placement.
"""
import argparse
import json

from confluent_kafka import Consumer

TOPIC = "orders.delivery"
COMMIT_EVERY = 5


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", choices=["before", "after"], default="after")
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:19092",
            "group.id": f"manual-commit-{args.commit}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # we decide exactly when an offset counts as "done"
        }
    )
    consumer.subscribe([TOPIC])

    processed_since_commit = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"! error: {msg.error()}")
                continue

            if args.commit == "before":
                consumer.commit(message=msg, asynchronous=False)

            event = json.loads(msg.value())
            print(f"offset={msg.offset()} order={event['order_id']} status={event['status']}")

            if args.commit == "after":
                processed_since_commit += 1
                if processed_since_commit >= COMMIT_EVERY:
                    consumer.commit(message=msg, asynchronous=False)
                    processed_since_commit = 0
                    print(f"  -- committed through offset {msg.offset()} --")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
