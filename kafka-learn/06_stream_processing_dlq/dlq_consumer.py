"""Stage 6: never let one bad message stall or crash a consumer -- quarantine it instead.

Good messages get printed. Anything that fails to parse gets forwarded, as-is, to a
dead-letter topic along with why it failed and where it came from, and the main loop
keeps going either way.
"""
import json
import time

from confluent_kafka import Consumer, Producer

INPUT_TOPIC = "orders.dirty"
DLQ_TOPIC = "orders.dirty.dlq"


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "dlq-demo",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([INPUT_TOPIC])
    dlq_producer = Producer({"bootstrap.servers": "localhost:9092"})

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"! consumer error: {msg.error()}")
                continue

            try:
                event = json.loads(msg.value())
                print(f"ok: {event}")
            except json.JSONDecodeError as e:
                print(f"! poison message at offset {msg.offset()}: {e} -- routing to DLQ")
                dlq_producer.produce(
                    DLQ_TOPIC,
                    value=msg.value(),  # the original, unparseable bytes -- don't lose evidence
                    headers={
                        "source_topic": INPUT_TOPIC.encode(),
                        "source_partition": str(msg.partition()).encode(),
                        "source_offset": str(msg.offset()).encode(),
                        "error": str(e).encode(),
                        "failed_at": str(int(time.time())).encode(),
                    },
                )
                dlq_producer.poll(0)
    except KeyboardInterrupt:
        pass
    finally:
        dlq_producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
