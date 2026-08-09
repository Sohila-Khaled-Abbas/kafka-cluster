# Examples

This directory contains runnable example scripts for producing, consuming, and processing Kafka messages.

## Scripts

| Script | Language | Description |
|--------|----------|-------------|
| [`producer.py`](producer.py) | Python | Generates realistic user events and publishes them to Kafka |
| [`consumer.py`](consumer.py) | Python | Reads and prints events from a Kafka topic with consumer groups |
| [`spark_streaming.py`](spark_streaming.py) | PySpark | Structured Streaming with windowed aggregation |

## Prerequisites

```bash
# For producer.py and consumer.py
pip install kafka-python

# For spark_streaming.py (run from Spark container)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 examples/spark_streaming.py
```

## Quick Demo

```bash
# Terminal 1 — Start the producer
python examples/producer.py

# Terminal 2 — Start the consumer
python examples/consumer.py

# Terminal 3 (Spark container) — Start streaming analytics
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 examples/spark_streaming.py
```
