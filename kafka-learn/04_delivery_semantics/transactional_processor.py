"""Stage 4: exactly-once consume-transform-produce.

Reads "paid" events from orders.delivery, produces a revenue record to revenue.v1,
AND commits the consumer offset -- all as ONE atomic transaction. Either both the revenue
record and the offset advance land together, or neither does. A crash mid-loop can never
produce a revenue record without also having durably advanced past the input message (no
double-counted revenue on restart), and can never advance the offset without the revenue
record existing (no silently dropped revenue either).

Pair this with read_committed_consumer.py to see the other half: an aborted transaction's
writes are never visible to a read_committed reader, even though they did briefly exist
in the log.
"""
import json
import sys
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer

sys.path.append(str(Path(__file__).resolve().parents[1]))

INPUT_TOPIC = "orders.delivery"
OUTPUT_TOPIC = "revenue.v1"


def main():
    producer = Producer(
        {
            "bootstrap.servers": "localhost:19092",
            "transactional.id": "orders-to-revenue-1",
        }
    )
    producer.init_transactions()

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:19092",
            "group.id": "orders-to-revenue",
            "enable.auto.commit": False,  # required: offsets travel inside the transaction instead
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe([INPUT_TOPIC])

    print("running -- Ctrl+C to stop")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"! consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value())
            if event["status"] != "paid":
                # nothing to emit -- but consumer.position() below still reflects having read
                # this message, so it rolls into whichever transaction commits offsets next
                continue

            try:
                producer.begin_transaction()
                producer.produce(
                    OUTPUT_TOPIC,
                    key=event["order_id"].encode(),
                    value=json.dumps(
                        {"order_id": event["order_id"], "amount": event["amount"]}
                    ).encode(),
                )
                producer.send_offsets_to_transaction(
                    consumer.position(consumer.assignment()),
                    consumer.consumer_group_metadata(),
                )
                producer.commit_transaction()
                print(f"committed: order={event['order_id']} amount={event['amount']}")
            except KafkaException as e:
                print(f"! transaction failed, aborting: {e}")
                producer.abort_transaction()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
