"""
Kafka Consumer Example — JSON Event Reader
============================================
Consumes user events from a Kafka topic and prints them.

Usage:
    pip install kafka-python
    python examples/consumer.py

Configuration:
    Adjust BOOTSTRAP_SERVERS based on where you're running:
    - From host machine:     localhost:9092
    - From Docker container: kafka1:19092
"""

import json
from kafka import KafkaConsumer

# ── Configuration ─────────────────────────────────────
BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "user-events"
GROUP_ID = "example-consumer-group"
AUTO_OFFSET_RESET = "earliest"  # "earliest" or "latest"


def main():
    print(f"📨 Kafka Consumer — reading from '{TOPIC}'")
    print(f"   Bootstrap: {BOOTSTRAP_SERVERS}")
    print(f"   Group:     {GROUP_ID}")
    print("━" * 55)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=30000,  # Stop after 30s of no messages
    )

    try:
        count = 0
        for message in consumer:
            count += 1
            event = message.value
            print(
                f"  📩 [{count:04d}] "
                f"topic={message.topic} "
                f"partition={message.partition} "
                f"offset={message.offset} "
                f"key={message.key}"
            )
            print(f"         {json.dumps(event, indent=2)}")
            print()

    except KeyboardInterrupt:
        print("\n⏹️  Stopped by user.")
    finally:
        consumer.close()
        print(f"🏁 Consumed {count} messages from '{TOPIC}'.")


if __name__ == "__main__":
    main()
