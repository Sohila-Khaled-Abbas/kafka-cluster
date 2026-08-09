"""
Spark Structured Streaming — Kafka Integration Example
========================================================
Reads user events from Kafka, applies windowed aggregation,
and writes results to the console.

Usage (from Spark container):
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      examples/spark_streaming.py

Configuration:
    Uses INTERNAL listener (kafka1:19092) for container-to-container.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, TimestampType
)

# ── Configuration ─────────────────────────────────────
BOOTSTRAP_SERVERS = "kafka1:19092,kafka2:19093,kafka3:19094"
INPUT_TOPIC = "user-events"
WINDOW_DURATION = "5 minutes"
WATERMARK_DELAY = "10 minutes"

# ── Schema Definition ─────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("page", StringType(), True),
    StructField("device", StringType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
])


def main():
    # ── Initialize Spark ──────────────────────────────
    spark = SparkSession.builder \
        .appName("KafkaUserEventAnalytics") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ── Read from Kafka ───────────────────────────────
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # ── Parse JSON Events ─────────────────────────────
    events = raw_stream \
        .select(
            col("key").cast("string").alias("key"),
            from_json(
                col("value").cast("string"),
                EVENT_SCHEMA
            ).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset"),
        ) \
        .select("key", "data.*", "kafka_timestamp", "partition", "offset")

    # ── Windowed Aggregation ──────────────────────────
    # Count events and average duration per action per 5-minute window
    aggregated = events \
        .withWatermark("timestamp", WATERMARK_DELAY) \
        .groupBy(
            window("timestamp", WINDOW_DURATION),
            "action",
            "device",
        ) \
        .agg(
            count("*").alias("event_count"),
            avg("duration_ms").alias("avg_duration_ms"),
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "action",
            "device",
            "event_count",
            expr("round(avg_duration_ms, 2)").alias("avg_duration_ms"),
        )

    # ── Write to Console ─────────────────────────────
    query = aggregated.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("truncate", False) \
        .option("numRows", 50) \
        .trigger(processingTime="10 seconds") \
        .start()

    print("⚡ Spark Streaming started. Waiting for events...")
    print(f"   Reading from: {INPUT_TOPIC}")
    print(f"   Window: {WINDOW_DURATION}")
    print("   Press Ctrl+C to stop.\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()
