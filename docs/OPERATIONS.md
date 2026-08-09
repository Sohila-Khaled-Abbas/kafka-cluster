# 🔧 Operations & Troubleshooting Guide

> Day-to-day management, monitoring, and troubleshooting for the Kafka cluster.

---

## Table of Contents

- [Cluster Lifecycle](#cluster-lifecycle)
- [Topic Operations](#topic-operations)
- [Producing & Consuming](#producing--consuming)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Performance Tuning](#performance-tuning)
- [Backup & Recovery](#backup--recovery)

---

## Cluster Lifecycle

### Start Cluster
```bash
docker compose up -d
```

### Stop Cluster (preserves data)
```bash
docker compose down
```

### Stop Cluster & Remove Volumes (fresh start)
```bash
docker compose down -v
```

### Restart a Single Broker
```bash
docker compose restart kafka1
```

### View Logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f kafka1

# Last N lines
docker logs kafka1 --tail 50
```

### Check Resource Usage
```bash
docker stats zoo1 kafka1 kafka2 kafka3 kafka-ui
```

---

## Topic Operations

### Create Topic
```bash
docker exec kafka1 kafka-topics --create \
  --topic my-topic \
  --bootstrap-server kafka1:19092 \
  --partitions 3 \
  --replication-factor 3
```

### List Topics
```bash
docker exec kafka1 kafka-topics --list \
  --bootstrap-server kafka1:19092
```

### Describe Topic
```bash
docker exec kafka1 kafka-topics --describe \
  --topic my-topic \
  --bootstrap-server kafka1:19092
```

### Modify Partitions (increase only)
```bash
docker exec kafka1 kafka-topics --alter \
  --topic my-topic \
  --partitions 6 \
  --bootstrap-server kafka1:19092
```

### Delete Topic
```bash
docker exec kafka1 kafka-topics --delete \
  --topic my-topic \
  --bootstrap-server kafka1:19092
```

### Configure Topic Retention
```bash
# Set retention to 7 days (in ms)
docker exec kafka1 kafka-configs --alter \
  --entity-type topics \
  --entity-name my-topic \
  --add-config retention.ms=604800000 \
  --bootstrap-server kafka1:19092
```

---

## Producing & Consuming

### Console Producer
```bash
# Interactive producer (type messages, press Enter)
docker exec -it kafka1 kafka-console-producer \
  --topic my-topic \
  --bootstrap-server kafka1:19092

# With keys (key:value format)
docker exec -it kafka1 kafka-console-producer \
  --topic my-topic \
  --bootstrap-server kafka1:19092 \
  --property "parse.key=true" \
  --property "key.separator=:"
```

### Console Consumer
```bash
# Read from beginning
docker exec kafka1 kafka-console-consumer \
  --topic my-topic \
  --bootstrap-server kafka1:19092 \
  --from-beginning

# Read with keys and timestamps
docker exec kafka1 kafka-console-consumer \
  --topic my-topic \
  --bootstrap-server kafka1:19092 \
  --from-beginning \
  --property "print.key=true" \
  --property "print.timestamp=true"

# Read from a specific partition
docker exec kafka1 kafka-console-consumer \
  --topic my-topic \
  --bootstrap-server kafka1:19092 \
  --partition 0 \
  --offset earliest
```

---

## Monitoring

### Broker Health
```bash
# ZooKeeper 4-letter commands
docker exec zoo1 bash -c "echo ruok | nc localhost 2181"   # → imok
docker exec zoo1 bash -c "echo stat | nc localhost 2181"   # Stats
docker exec zoo1 bash -c "echo srvr | nc localhost 2181"   # Server info
```

### Consumer Group Lag
```bash
# List all groups
docker exec kafka1 kafka-consumer-groups --list \
  --bootstrap-server kafka1:19092

# Check lag for a group
docker exec kafka1 kafka-consumer-groups --describe \
  --group my-consumer-group \
  --bootstrap-server kafka1:19092

# Output columns:
# TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
```

### Kafka UI Dashboard
Open [http://localhost:9021](http://localhost:9021) for visual monitoring of:
- Broker status and metrics
- Topic partition distribution
- Consumer group lag
- Message browsing

---

## Troubleshooting

### Diagnostic Checklist

```bash
# 1. Are all containers running?
docker compose ps

# 2. Is ZooKeeper healthy?
docker exec zoo1 bash -c "echo ruok | nc localhost 2181"

# 3. Can brokers communicate?
docker exec kafka1 kafka-broker-api-versions --bootstrap-server kafka1:19092

# 4. Are all brokers registered?
docker exec zoo1 bash -c "echo dump | nc localhost 2181" | grep broker

# 5. Check for errors in logs
docker logs kafka1 2>&1 | grep -i "error\|exception\|fatal"
```

### Common Issues

<details>
<summary><strong>🔴 ZooKeeper SessionExpiredException</strong></summary>

**Root Cause:** Docker host under resource pressure; ZK can't respond in time.

**Fix:**
```bash
# 1. Stop everything cleanly
docker compose down -v

# 2. Free resources — stop unused containers
docker ps -q | xargs docker stop  # ⚠️ stops ALL containers

# 3. Increase Docker Desktop memory (Settings → Resources → 8GB+)

# 4. Restart
docker compose up -d
```

</details>

<details>
<summary><strong>🔴 Leader Not Available</strong></summary>

**Root Cause:** Topic was just created; leader election hasn't completed.

**Fix:** Wait 5–10 seconds and retry. If persistent:
```bash
# Check ISR (In-Sync Replicas)
docker exec kafka1 kafka-topics --describe --topic my-topic --bootstrap-server kafka1:19092
```

</details>

<details>
<summary><strong>🔴 Port Already in Use</strong></summary>

**Root Cause:** Another process is using the port.

**Fix:**
```bash
# Find what's using port 9092 (Windows)
netstat -ano | findstr :9092

# Kill the process by PID
taskkill /PID <PID> /F
```

</details>

<details>
<summary><strong>🔴 Network Not Found</strong></summary>

```bash
docker network create itvdelabnw
docker compose up -d
```

</details>

---

## Performance Tuning

### Broker-Level Settings

| Setting | Recommended | Purpose |
|---------|-------------|---------|
| `num.partitions` | 3–12 | Default partitions for new topics |
| `default.replication.factor` | 3 | Data durability |
| `log.retention.hours` | 168 (7 days) | How long to keep messages |
| `log.segment.bytes` | 1073741824 (1GB) | Log segment file size |
| `message.max.bytes` | 1048576 (1MB) | Max message size |

### Producer Best Practices

| Setting | Value | Effect |
|---------|-------|--------|
| `acks` | `all` | Highest durability |
| `batch.size` | 16384 | Batch before sending |
| `linger.ms` | 5–50 | Wait to fill batch |
| `compression.type` | `snappy` | Reduce network I/O |

### Consumer Best Practices

| Setting | Value | Effect |
|---------|-------|--------|
| `auto.offset.reset` | `earliest` | Start from beginning |
| `enable.auto.commit` | `false` | Manual offset control |
| `max.poll.records` | 500 | Records per poll |
| `fetch.min.bytes` | 1024 | Min batch to fetch |

---

## Backup & Recovery

### Export Topic Data
```bash
# Consume all messages to a file
docker exec kafka1 kafka-console-consumer \
  --topic my-topic \
  --bootstrap-server kafka1:19092 \
  --from-beginning \
  --timeout-ms 10000 > topic_backup.json
```

### Import Topic Data
```bash
# Produce from a file
cat topic_backup.json | docker exec -i kafka1 kafka-console-producer \
  --topic my-topic-restored \
  --bootstrap-server kafka1:19092
```

### Full Cluster Reset
```bash
# Nuclear option — removes all data
docker compose down -v
docker compose up -d
```
