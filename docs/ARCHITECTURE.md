# 🏗️ Architecture Overview

> **Cluster Topology:** 1 ZooKeeper + 3 Kafka Brokers + Kafka UI + Spark Integration

---

## Architecture Infographic

![Kafka Cluster Architecture](images/architecture-infographic.png)

---

## Component Diagram

```mermaid
graph TB
    subgraph Docker Network: itvdelabnw
        ZK[("🟢 ZooKeeper<br/>zoo1:2181")]
        
        K1["🟠 Kafka Broker 1<br/>kafka1:19092"]
        K2["🟠 Kafka Broker 2<br/>kafka2:19093"]
        K3["🟠 Kafka Broker 3<br/>kafka3:19094"]
        
        UI["🔵 Kafka UI<br/>kafka-ui:8080"]
        
        SPARK["🔴 Spark<br/>itvdelab:8888"]
    end

    ZK -->|"healthcheck: ruok"| K1
    ZK -->|"healthcheck: ruok"| K2
    ZK -->|"healthcheck: ruok"| K3

    K1 --> UI
    K2 --> UI
    K3 --> UI

    K1 -.->|"INTERNAL<br/>:19092"| SPARK
    K2 -.->|"INTERNAL<br/>:19093"| SPARK
    K3 -.->|"INTERNAL<br/>:19094"| SPARK

    HOST["💻 Host Machine"] -->|"EXTERNAL<br/>localhost:9092-9094"| K1
    HOST -->|":8080"| UI
```

---

## Data Flow

```mermaid
sequenceDiagram
    participant P as Producer
    participant K as Kafka Broker
    participant T as Topic (Partitioned)
    participant S as Spark Structured Streaming
    participant O as Output Sink

    P->>K: Send Message (key, value)
    K->>T: Append to Partition
    T-->>K: Acknowledge (offset)
    K-->>P: ACK

    S->>K: Subscribe to Topic
    K->>S: Stream Messages (micro-batch)
    S->>S: Transform / Aggregate
    S->>O: Write to Sink (Console, DB, File)
```

---

## Service Dependencies

```mermaid
graph LR
    A["zoo1<br/>(ZooKeeper)"] -->|"must be healthy"| B["kafka1"]
    A -->|"must be healthy"| C["kafka2"]
    A -->|"must be healthy"| D["kafka3"]
    B -->|"started"| E["kafka-ui"]
    C -->|"started"| E
    D -->|"started"| E
    
    style A fill:#4caf50,color:#fff
    style B fill:#ff9800,color:#fff
    style C fill:#ff9800,color:#fff
    style D fill:#ff9800,color:#fff
    style E fill:#2196f3,color:#fff
```

**Startup Order:**
1. ✅ `zoo1` starts and becomes healthy (healthcheck passes)
2. ✅ `kafka1`, `kafka2`, `kafka3` start simultaneously (after ZK healthy)
3. ✅ `kafka-ui` starts (after all 3 brokers are running)

---

## Network Topology

| Layer | Listener | Port Range | Access From |
|-------|----------|------------|-------------|
| **INTERNAL** | `kafkaX:190XX` | 19092–19094 | Other containers on `itvdelabnw` |
| **EXTERNAL** | `localhost:90XX` | 9092–9094 | Host machine |
| **DOCKER** | `host.docker.internal:290XX` | 29092–29094 | Docker Desktop VMs |

---

## Resource Allocation

| Service | Image | Memory (approx.) | CPU |
|---------|-------|-------------------|-----|
| ZooKeeper | `cp-zookeeper:7.3.2` | 256–512 MB | Low |
| Kafka Broker (×3) | `cp-kafka:7.3.2` | 1–2 GB each | Medium |
| Kafka UI | `kafka-ui:latest` | 256–512 MB | Low |
| **Total** | — | **~4–8 GB** | — |

> [!TIP]
> Allocate at least **8 GB RAM** to Docker Desktop for smooth operation with Spark running alongside.
