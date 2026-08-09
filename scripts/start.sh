#!/usr/bin/env bash
# ============================================================
# start.sh — Start the Kafka Cluster
# ============================================================
# Usage: ./scripts/start.sh
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
NETWORK_NAME="itvdelabnw"

echo "🚀 Starting Kafka Cluster..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Pre-flight: Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Pre-flight: Check network
if ! docker network inspect "$NETWORK_NAME" &> /dev/null; then
    echo "📡 Creating external network: $NETWORK_NAME"
    docker network create "$NETWORK_NAME"
else
    echo "✅ Network '$NETWORK_NAME' already exists"
fi

# Pre-flight: Copy .env if missing
if [ ! -f "$PROJECT_DIR/.env" ]; then
    echo "📋 Creating .env from .env.example"
    cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
fi

# Start services
cd "$PROJECT_DIR"
docker compose up -d

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "⏳ Waiting for services to be ready..."
sleep 10

# Health check
echo ""
echo "📊 Service Status:"
docker compose ps
echo ""

# ZooKeeper check
ZK_STATUS=$(docker exec zoo1 bash -c "echo ruok | nc localhost 2181" 2>/dev/null || echo "not ready")
if [ "$ZK_STATUS" = "imok" ]; then
    echo "✅ ZooKeeper: healthy"
else
    echo "⚠️  ZooKeeper: still starting (status: $ZK_STATUS)"
fi

echo ""
echo "🎉 Kafka Cluster is starting!"
echo "   Use 'docker compose ps' to check status."
echo "   Use 'docker compose logs -f' to follow logs."
