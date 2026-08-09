# 📊 Data Pipeline Patterns with Kafka

> Common architectures for building real-time and batch data pipelines using Kafka and Spark.

---

## Table of Contents

- [Pattern Overview](#pattern-overview)
- [1. Event Streaming Pipeline](#1-event-streaming-pipeline)
- [2. ETL Pipeline (Extract → Transform → Load)](#2-etl-pipeline)
- [3. CQRS & Event Sourcing](#3-cqrs--event-sourcing)
- [4. Lambda Architecture](#4-lambda-architecture)
- [5. Kappa Architecture](#5-kappa-architecture)
- [6. CDC (Change Data Capture)](#6-cdc-change-data-capture)
- [Best Practices](#best-practices)

---

## Pattern Overview

```mermaid
graph LR
    subgraph "Data Pipeline Patterns"
        ES["Event Streaming"]
        ETL["ETL Pipeline"]
        CQRS["CQRS / Event Sourcing"]
        LAMBDA["Lambda Architecture"]
        KAPPA["Kappa Architecture"]
        CDC["Change Data Capture"]
    end

    ES --> |"Real-time"| RT["Low-Latency Analytics"]
    ETL --> |"Batch + Stream"| DW["Data Warehouse"]
    CQRS --> |"Event Store"| MS["Microservices"]
    LAMBDA --> |"Dual Path"| HA["Hybrid Analytics"]
    KAPPA --> |"Stream Only"| SA["Stream Analytics"]
    CDC --> |"DB Sync"| SYNC["Data Replication"]

    style ES fill:#4caf50,color:#fff
    style ETL fill:#ff9800,color:#fff
    style CQRS fill:#2196f3,color:#fff
    style LAMBDA fill:#9c27b0,color:#fff
    style KAPPA fill:#e91e63,color:#fff
    style CDC fill:#00bcd4,color:#fff
```

---

## 1. Event Streaming Pipeline

The simplest and most common pattern. Producers emit events → Kafka stores them → Consumers process them in real time.

```mermaid
graph LR
    P1["🌐 Web App"] -->|events| T["📦 Kafka Topic<br/>'user-events'"]
    P2["📱 Mobile App"] -->|events| T
    P3["🤖 IoT Sensors"] -->|events| T

    T -->|subscribe| C1["⚡ Spark Streaming<br/>(Analytics)"]
    T -->|subscribe| C2["🔔 Alert Service<br/>(Notifications)"]
    T -->|subscribe| C3["🗄️ Data Lake<br/>(Storage)"]

    style T fill:#ff9800,color:#fff
```

**When to use:**
- Real-time dashboards and monitoring
- Event-driven microservices
- User activity tracking

**Implementation with this cluster:**
```bash
# Create the topic
make topic TOPIC=user-events PARTITIONS=6

# Run the example producer
python examples/producer.py

# Run the Spark streaming consumer
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  examples/spark_streaming.py
```

---

## 2. ETL Pipeline

Extract from sources → Transform with Spark → Load into a data warehouse or data lake.

```mermaid
graph LR
    subgraph "Extract"
        DB["🗄️ Database"]
        API["🌐 REST API"]
        FILE["📁 CSV/JSON Files"]
    end

    subgraph "Transform"
        K["📦 Kafka<br/>(Raw Events)"]
        S["⚡ Spark<br/>(Clean & Enrich)"]
    end

    subgraph "Load"
        DW["📊 Data Warehouse"]
        DL["🏔️ Data Lake<br/>(Parquet)"]
    end

    DB -->|CDC| K
    API -->|ingest| K
    FILE -->|batch| K
    K --> S
    S --> DW
    S --> DL

    style K fill:#ff9800,color:#fff
    style S fill:#e25a1c,color:#fff
```

**Key considerations:**
- Use Kafka as a buffer between extract and transform stages
- Spark handles both micro-batch and batch processing
- Write to Parquet for efficient columnar storage

---

## 3. CQRS & Event Sourcing

Separate write (command) and read (query) paths. Kafka acts as the immutable event log.

```mermaid
graph TB
    CMD["✏️ Command<br/>(Write)"] -->|append| KT["📦 Kafka Topic<br/>(Event Store)"]

    KT -->|materialize| MV1["📊 Read Model 1<br/>(SQL Database)"]
    KT -->|materialize| MV2["🔍 Read Model 2<br/>(Search Index)"]
    KT -->|materialize| MV3["📈 Read Model 3<br/>(Analytics Cache)"]

    Q["👁️ Query<br/>(Read)"] --> MV1
    Q --> MV2
    Q --> MV3

    style KT fill:#ff9800,color:#fff
    style CMD fill:#4caf50,color:#fff
    style Q fill:#2196f3,color:#fff
```

**When to use:**
- High-read, low-write workloads
- Need for multiple read optimized views
- Audit trail requirements

**Kafka configuration:**
```bash
# Long retention for event store (30 days)
docker exec kafka1 kafka-configs --alter \
  --entity-type topics --entity-name orders \
  --add-config retention.ms=2592000000 \
  --bootstrap-server kafka1:19092

# Or infinite retention (log compaction)
docker exec kafka1 kafka-configs --alter \
  --entity-type topics --entity-name orders \
  --add-config cleanup.policy=compact \
  --bootstrap-server kafka1:19092
```

---

## 4. Lambda Architecture

Dual-path processing: a **batch layer** for accuracy and a **speed layer** for low latency.

```mermaid
graph TB
    SRC["📥 Data Source"] --> K["📦 Kafka"]

    K -->|"Speed Layer<br/>(Real-time)"| SS["⚡ Spark Streaming"]
    K -->|"Batch Layer<br/>(Historical)"| SB["⚡ Spark Batch"]

    SS --> RT["⏱️ Real-time View"]
    SB --> BT["📊 Batch View"]

    RT --> SV["🖥️ Serving Layer<br/>(Merged Query)"]
    BT --> SV

    style K fill:#ff9800,color:#fff
    style SS fill:#e25a1c,color:#fff
    style SB fill:#e25a1c,color:#fff
    style SV fill:#2196f3,color:#fff
```

**Trade-offs:**
| Aspect | Pros | Cons |
|--------|------|------|
| Accuracy | Batch corrects streaming errors | Two codebases to maintain |
| Latency | Speed layer provides real-time | Complexity of merging views |
| Cost | Handles reprocessing | Higher infrastructure cost |

---

## 5. Kappa Architecture

**Stream-only** approach — eliminates the batch layer. All processing goes through Kafka streams.

```mermaid
graph LR
    SRC["📥 Data Source"] --> K["📦 Kafka<br/>(Immutable Log)"]
    K --> S1["⚡ Stream Processor<br/>(Version N)"]
    K -.->|"reprocess"| S2["⚡ Stream Processor<br/>(Version N+1)"]

    S1 --> OUT["🖥️ Serving Layer"]
    S2 -.-> OUT

    style K fill:#ff9800,color:#fff
    style S1 fill:#e25a1c,color:#fff
    style S2 fill:#e25a1c,color:#fff,stroke-dasharray: 5 5
```

**When to use over Lambda:**
- Simpler operational model (one codebase)
- Kafka retention is long enough to reprocess
- Eventual consistency is acceptable

**Kafka retention for reprocessing:**
```bash
# Set 7-day retention to allow full reprocessing
make topic TOPIC=events PARTITIONS=12
docker exec kafka1 kafka-configs --alter \
  --entity-type topics --entity-name events \
  --add-config retention.ms=604800000 \
  --bootstrap-server kafka1:19092
```

---

## 6. CDC (Change Data Capture)

Capture database changes and stream them through Kafka using Debezium or similar connectors.

```mermaid
graph LR
    DB["🗄️ PostgreSQL<br/>(Source)"] -->|"WAL/Binlog"| DEB["🔌 Debezium<br/>(CDC Connector)"]
    DEB -->|"INSERT/UPDATE/DELETE"| K["📦 Kafka Topic<br/>'db.public.orders'"]

    K --> S["⚡ Spark<br/>(Transform)"]
    K --> ES["🔍 Elasticsearch<br/>(Search)"]
    K --> DW["📊 BigQuery<br/>(Analytics)"]

    style DB fill:#336791,color:#fff
    style DEB fill:#d32f2f,color:#fff
    style K fill:#ff9800,color:#fff
```

**When to use:**
- Replicate databases to data lakes in real-time
- Keep search indexes in sync
- Feed analytics from operational databases

---

## Best Practices

### Topic Design

| Guideline | Recommendation |
|-----------|----------------|
| **Naming** | `<domain>.<entity>.<version>` (e.g., `ecommerce.orders.v1`) |
| **Partitions** | `2× max consumer parallelism` |
| **Replication** | `min(3, number_of_brokers)` |
| **Retention** | 7 days default; longer for event sourcing |
| **Key selection** | Use entity ID for ordering guarantees |

### Message Schema

```json
{
  "schema_version": "1.0",
  "event_type": "order.created",
  "event_id": "uuid-v4",
  "timestamp": "2026-08-09T15:00:00Z",
  "source": "order-service",
  "data": {
    "order_id": 12345,
    "customer_id": 6789,
    "total": 99.99
  },
  "metadata": {
    "correlation_id": "req-abc-123",
    "trace_id": "span-xyz-456"
  }
}
```

### Partitioning Strategy

```mermaid
graph TD
    MSG["Incoming Message"] --> KEY{"Has Key?"}
    KEY -->|Yes| HASH["Hash(key) % partitions"]
    KEY -->|No| RR["Round-Robin"]

    HASH --> P1["Partition 0"]
    HASH --> P2["Partition 1"]
    HASH --> P3["Partition 2"]
    RR --> P1
    RR --> P2
    RR --> P3

    style MSG fill:#ff9800,color:#fff
    style KEY fill:#2196f3,color:#fff
```

> [!TIP]
> **Same key → same partition → guaranteed ordering.** Use customer_id or order_id as the key to ensure all events for an entity are processed in order.
