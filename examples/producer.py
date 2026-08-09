"""
Kafka Producer Example — JSON Event Emitter
=============================================
Produces sample user events to a Kafka topic.

Usage:
    pip install kafka-python
    python examples/producer.py

Configuration:
    Adjust BOOTSTRAP_SERVERS based on where you're running:
    - From host machine:     localhost:9092
    - From Docker container: kafka1:19092
"""

import json
import random
import sys
import time
from datetime import datetime, timezone

# Ensure stdout supports UTF-8 on Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from kafka import KafkaProducer

# ── Configuration ─────────────────────────────────────
BOOTSTRAP_SERVERS = ["127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"]
TOPIC = "user-events"
NUM_MESSAGES = 10
DELAY_SECONDS = 0.1

# ── Event Templates ───────────────────────────────────
ACTIONS = ["page_view", "click", "purchase", "signup", "logout", "search"]
PAGES = ["/home", "/products", "/cart", "/checkout", "/profile", "/search"]
DEVICES = ["mobile", "desktop", "tablet"]


def generate_event(event_id: int) -> dict:
    """Generate a realistic user event."""
    return {
        "event_id": event_id,
        "user_id": random.randint(1000, 9999),
        "action": random.choice(ACTIONS),
        "page": random.choice(PAGES),
        "device": random.choice(DEVICES),
        "duration_ms": random.randint(100, 30000),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    print(f"📤 Kafka Producer — sending {NUM_MESSAGES} events to '{TOPIC}'")
    print(f"   Bootstrap: {BOOTSTRAP_SERVERS}")
    print("━" * 55)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        api_version=(2, 8, 1),
    )

    try:
        for i in range(1, NUM_MESSAGES + 1):
            event = generate_event(i)
            key = str(event["user_id"])

            future = producer.send(TOPIC, key=key, value=event)
            metadata = future.get(timeout=10)

            print(
                f"  ✅ [{i:03d}/{NUM_MESSAGES}] "
                f"partition={metadata.partition} "
                f"offset={metadata.offset} "
                f"action={event['action']}"
            )
            time.sleep(DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\n⏹️  Stopped by user.")
    finally:
        producer.flush()
        producer.close()
        print(f"\n🏁 Done. Sent {i} messages to '{TOPIC}'.")


if __name__ == "__main__":
    main()
