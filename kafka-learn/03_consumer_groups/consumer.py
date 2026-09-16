"""Stage 3: run this SAME script in 2-3 separate terminals, with different --name values,
to watch Kafka rebalance partitions across them live.

    python 03_consumer_groups/consumer.py --name c1
    python 03_consumer_groups/consumer.py --name c2

Both use the same group.id, so they split the topic's partitions between them.
"""
import argparse
import json

from confluent_kafka import Consumer

TOPIC = "orders.groups"
GROUP_ID = "order-processors"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="c1", help="label for this consumer instance's output")
    parser.add_argument(
        "--group",
        default=GROUP_ID,
        help="use a different value to start an independent group (gets ALL partitions itself)",
    )
    args = parser.parse_args()

    def on_assign(consumer, partitions):
        parts = [p.partition for p in partitions]
        print(f"[{args.name}] >>> ASSIGNED partitions {parts}")

    def on_revoke(consumer, partitions):
        parts = [p.partition for p in partitions]
        print(f"[{args.name}] <<< REVOKED partitions {parts} (rebalance starting)")

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": args.group,
            "client.id": args.name,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{args.name}] ! error: {msg.error()}")
                continue
            event = json.loads(msg.value())
            print(f"[{args.name}] partition={msg.partition()} order={event['order_id']} status={event['status']}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
