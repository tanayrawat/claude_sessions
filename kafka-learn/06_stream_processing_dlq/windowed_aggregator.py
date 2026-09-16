"""Stage 6: a from-scratch tumbling-window aggregator -- the mental model underneath
Kafka Streams / ksqlDB / Structured Streaming, minus the framework.

Buckets "paid" events into 10-second tumbling windows by event time and, once a window
has fully closed, emits {window_start, order_count, revenue} to revenue.windowed.

DELIBERATE SIMPLIFICATION, called out on purpose: this closes a window purely based on
wall-clock time passing, with no watermark for late/out-of-order events. A real stream
processor (Kafka Streams, Flink, Structured Streaming) tracks event-time watermarks so a
slightly-late event still lands in the right window instead of being dropped or double
counted at the boundary. Don't ship this part to production -- do internalize the shape
of "consumer with local state, periodic emit" that every one of those frameworks builds on.
"""
import json
import time
from collections import defaultdict

from confluent_kafka import Consumer, Producer

INPUT_TOPIC = "orders.delivery"
OUTPUT_TOPIC = "revenue.windowed"
WINDOW_SECONDS = 10


def window_start(ts: float) -> int:
    return int(ts // WINDOW_SECONDS) * WINDOW_SECONDS


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "windowed-aggregator",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([INPUT_TOPIC])
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    windows: dict[int, dict] = defaultdict(lambda: {"order_count": 0, "revenue": 0.0})

    print(f"aggregating into {WINDOW_SECONDS}s tumbling windows -- Ctrl+C to stop")
    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.time()

            if msg is not None and not msg.error():
                event = json.loads(msg.value())
                if event["status"] == "paid":
                    w = window_start(event["ts"])
                    windows[w]["order_count"] += 1
                    windows[w]["revenue"] += event["amount"]

            # a window is "closed" once wall-clock time has moved a full window past its start
            closed = [w for w in windows if now - w >= WINDOW_SECONDS]
            for w in sorted(closed):
                agg = windows.pop(w)
                agg["revenue"] = round(agg["revenue"], 2)
                producer.produce(
                    OUTPUT_TOPIC,
                    key=str(w).encode(),
                    value=json.dumps({"window_start": w, **agg}).encode(),
                )
                print(f"window[{w}] -> {agg}")
            if closed:
                producer.flush()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
