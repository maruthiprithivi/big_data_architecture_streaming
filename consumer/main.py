"""
Velocity Showdown — Kafka Consumer → ClickHouse
================================================
Reads from all three velocity topics and persists events to ClickHouse.
Consumer group: clickhouse-sink

Students can observe this consumer's offset lag in Kafka UI at localhost:8080
under Consumer Groups → clickhouse-sink.
"""

import asyncio, json, os
from datetime import datetime, timezone

import clickhouse_connect
from aiokafka import AIOKafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP",   "localhost:9092")
CH_HOST         = os.getenv("CLICKHOUSE_HOST",   "localhost")
CH_PASS         = os.getenv("CLICKHOUSE_PASSWORD","velocity_pass")

TOPICS = ["velocity.solana", "velocity.bluesky", "velocity.wikipedia"]


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=8123, username="default",
        password=CH_PASS, database="velocity_lab"
    )


async def consume():
    ch = get_ch()

    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="clickhouse-sink",      # students see this in Kafka UI
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()
    print(f"[consumer] Started. Subscribed to: {TOPICS}")

    buffer = []
    last_flush = asyncio.get_event_loop().time()

    try:
        async for msg in consumer:
            ev = msg.value
            buffer.append([
                ev.get("source", ""),
                ev.get("event_type", ""),
                datetime.fromisoformat(ev.get("timestamp", datetime.now(timezone.utc).isoformat())),
                datetime.now(timezone.utc),
                ev.get("summary", "")[:400],
                json.dumps({k: v for k, v in ev.items() if k not in
                            ("source", "event_type", "timestamp", "summary")}),
            ])

            # Micro-batch flush: every 20 events or 2 seconds
            now = asyncio.get_event_loop().time()
            if len(buffer) >= 20 or (now - last_flush) > 2.0:
                try:
                    ch.insert(
                        "events",
                        buffer,
                        column_names=["source", "event_type", "event_time",
                                      "collected_at", "summary", "metadata"],
                    )
                    print(f"[consumer] Flushed {len(buffer)} events to ClickHouse")
                except Exception as e:
                    print(f"[consumer] ClickHouse write error: {e}")
                buffer = []
                last_flush = now

    finally:
        if buffer:
            try:
                ch.insert("events", buffer,
                          column_names=["source", "event_type", "event_time",
                                        "collected_at", "summary", "metadata"])
            except Exception:
                pass
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume())
