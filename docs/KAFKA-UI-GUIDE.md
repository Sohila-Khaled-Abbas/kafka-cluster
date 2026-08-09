# 🖥️ Kafka UI Guide

> **URL:** [http://localhost:9021](http://localhost:9021)

Kafka UI (by Provectus) provides a web-based dashboard to manage and monitor your Kafka cluster without using CLI commands.

---

## Features

| Feature | Description |
|---------|-------------|
| 📊 **Dashboard** | Cluster overview — brokers, topics, partitions, consumer groups |
| 📝 **Topic Management** | Create, configure, delete topics via GUI |
| 📨 **Message Browser** | Read messages from any topic with filtering and search |
| ✉️ **Produce Messages** | Send test messages to topics directly from the browser |
| 👥 **Consumer Groups** | Monitor consumer lag, offsets, and group membership |
| 📈 **Metrics** | Real-time broker and topic metrics visualization |
| ⚙️ **Dynamic Config** | Modify broker/topic configurations live |

---

## Accessing Kafka UI

### Start the UI
```bash
docker compose up -d kafka-ui
```

### Open in Browser
Navigate to: **[http://localhost:9021](http://localhost:9021)**

You should see the cluster named **`kafka-cluster`** with 3 brokers listed.

---

## Common Tasks

### Create a Topic

1. Open **Topics** → Click **"Add a Topic"**
2. Fill in:
   - **Topic Name:** `my-first-topic`
   - **Number of Partitions:** `3`
   - **Replication Factor:** `3`
3. Click **Create**

### Browse Messages

1. Open **Topics** → Select your topic
2. Click the **"Messages"** tab
3. Use filters:
   - **Offset:** Start from beginning, latest, or specific offset
   - **Partition:** Filter by specific partition
   - **Key/Value search:** Text search within messages

### Produce a Test Message

1. Open **Topics** → Select your topic
2. Click **"Produce Message"**
3. Enter:
   - **Key:** `test-key`
   - **Value:** `{"message": "Hello Kafka!", "timestamp": "2026-08-09"}`
4. Click **Produce**

### Monitor Consumer Groups

1. Open **Consumer Groups** from the sidebar
2. View:
   - **Total Lag** — messages behind the latest offset
   - **Members** — active consumers in the group
   - **Assigned Partitions** — which consumer handles which partition

---

## Configuration

The Kafka UI is configured in `docker-compose.yml`:

```yaml
kafka-ui:
  image: provectuslabs/kafka-ui:latest
  ports:
    - "9021:8080"
  environment:
    KAFKA_CLUSTERS_0_NAME: kafka-cluster
    KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka1:19092,kafka2:19093,kafka3:19094
    KAFKA_CLUSTERS_0_ZOOKEEPER: zoo1:2181
    DYNAMIC_CONFIG_ENABLED: "true"
```

### Adding Multiple Clusters

To monitor additional Kafka clusters, add more `KAFKA_CLUSTERS_N_*` variables:

```yaml
environment:
  # Cluster 0 (existing)
  KAFKA_CLUSTERS_0_NAME: kafka-cluster
  KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka1:19092
  # Cluster 1 (additional)
  KAFKA_CLUSTERS_1_NAME: production-cluster
  KAFKA_CLUSTERS_1_BOOTSTRAPSERVERS: prod-kafka:9092
```

---

## Troubleshooting

<details>
<summary><strong>❌ Cannot access localhost:9021</strong></summary>

1. Check if the container is running:
   ```bash
   docker ps --filter "name=kafka-ui"
   ```
2. Check logs for errors:
   ```bash
   docker logs kafka-ui --tail 30
   ```
3. Ensure port 9021 is not used by another service:
   ```bash
   netstat -ano | findstr :9021
   ```

</details>

<details>
<summary><strong>❌ "Cluster is not available" in UI</strong></summary>

The Kafka brokers may not be fully started. Wait 30–60 seconds and refresh. Verify brokers are running:
```bash
docker compose ps
```

</details>

<details>
<summary><strong>❌ Topics not visible</strong></summary>

Ensure you're looking at the correct cluster (top-left dropdown). Internal topics like `__consumer_offsets` are hidden by default — toggle **"Show Internal Topics"** in Settings.

</details>
