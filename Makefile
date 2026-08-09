# ============================================================
# Makefile — Kafka Cluster Operations
# ============================================================
# Usage:
#   make up         Start the cluster
#   make down       Stop the cluster
#   make restart    Restart the cluster
#   make status     Show container status
#   make logs       Follow all logs
#   make health     Run health checks
#   make topic      Create a test topic
#   make produce    Start interactive producer
#   make consume    Start interactive consumer
#   make clean      Remove all data (volumes)
# ============================================================

.PHONY: up down restart status logs health topic produce consume clean ui network

# ── Cluster Lifecycle ──────────────────────────────────

network: ## Create the external Docker network
	@docker network inspect itvdelabnw >/dev/null 2>&1 || \
		(echo "📡 Creating network: itvdelabnw" && docker network create itvdelabnw)

up: network ## Start all services
	@echo "🚀 Starting Kafka cluster..."
	@docker compose up -d
	@echo "✅ Cluster started. UI: http://localhost:9021"

down: ## Stop all services
	@echo "🛑 Stopping Kafka cluster..."
	@docker compose down

restart: down up ## Restart the cluster

clean: ## Stop and remove all data volumes
	@echo "🧹 Cleaning up (removing volumes)..."
	@docker compose down -v
	@echo "✅ Clean slate."

# ── Monitoring ─────────────────────────────────────────

status: ## Show container status
	@docker compose ps

logs: ## Follow all service logs
	@docker compose logs -f

logs-kafka: ## Follow Kafka broker logs only
	@docker compose logs -f kafka1 kafka2 kafka3

logs-zk: ## Follow ZooKeeper logs only
	@docker compose logs -f zoo1

logs-ui: ## Follow Kafka UI logs only
	@docker compose logs -f kafka-ui

health: ## Run health checks
	@echo "🏥 Health Check"
	@echo "━━━━━━━━━━━━━━━"
	@echo -n "  ZooKeeper: " && docker exec zoo1 bash -c "echo ruok | nc localhost 2181" 2>/dev/null || echo "❌ not responding"
	@echo -n "  Kafka1:    " && docker exec kafka1 kafka-broker-api-versions --bootstrap-server kafka1:19092 2>/dev/null | head -1 || echo "❌ not responding"
	@echo -n "  Kafka2:    " && docker exec kafka2 kafka-broker-api-versions --bootstrap-server kafka2:19093 2>/dev/null | head -1 || echo "❌ not responding"
	@echo -n "  Kafka3:    " && docker exec kafka3 kafka-broker-api-versions --bootstrap-server kafka3:19094 2>/dev/null | head -1 || echo "❌ not responding"
	@echo "  Kafka UI:  http://localhost:9021"

# ── Topic Operations ──────────────────────────────────

TOPIC ?= test-topic
PARTITIONS ?= 3
REPLICATION ?= 3
BOOTSTRAP ?= kafka1:19092

topic: ## Create a topic (TOPIC=name PARTITIONS=3 REPLICATION=3)
	@echo "📝 Creating topic: $(TOPIC)"
	@docker exec kafka1 kafka-topics --create \
		--topic $(TOPIC) \
		--bootstrap-server $(BOOTSTRAP) \
		--partitions $(PARTITIONS) \
		--replication-factor $(REPLICATION)

topics: ## List all topics
	@docker exec kafka1 kafka-topics --list --bootstrap-server $(BOOTSTRAP)

describe: ## Describe a topic (TOPIC=name)
	@docker exec kafka1 kafka-topics --describe --topic $(TOPIC) --bootstrap-server $(BOOTSTRAP)

delete-topic: ## Delete a topic (TOPIC=name)
	@docker exec kafka1 kafka-topics --delete --topic $(TOPIC) --bootstrap-server $(BOOTSTRAP)

# ── Producer & Consumer ───────────────────────────────

produce: ## Start interactive producer (TOPIC=name)
	@echo "✉️  Producing to: $(TOPIC) (Ctrl+C to exit)"
	@docker exec -it kafka1 kafka-console-producer \
		--topic $(TOPIC) \
		--bootstrap-server $(BOOTSTRAP)

consume: ## Start consumer from beginning (TOPIC=name)
	@echo "📨 Consuming from: $(TOPIC) (Ctrl+C to exit)"
	@docker exec kafka1 kafka-console-consumer \
		--topic $(TOPIC) \
		--bootstrap-server $(BOOTSTRAP) \
		--from-beginning

# ── Consumer Groups ───────────────────────────────────

GROUP ?= my-group

groups: ## List consumer groups
	@docker exec kafka1 kafka-consumer-groups --list --bootstrap-server $(BOOTSTRAP)

group-lag: ## Check consumer group lag (GROUP=name)
	@docker exec kafka1 kafka-consumer-groups --describe --group $(GROUP) --bootstrap-server $(BOOTSTRAP)

# ── UI ────────────────────────────────────────────────

ui: ## Open Kafka UI in browser
	@echo "🌐 Opening http://localhost:9021"
	@start http://localhost:9021 2>/dev/null || open http://localhost:9021 2>/dev/null || xdg-open http://localhost:9021 2>/dev/null

# ── Help ──────────────────────────────────────────────

help: ## Show this help
	@echo "Kafka Cluster — Available Commands:"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
