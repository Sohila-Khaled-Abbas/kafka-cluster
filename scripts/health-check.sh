#!/usr/bin/env bash
# ============================================================
# health-check.sh — Verify Kafka Cluster Health
# ============================================================
# Usage: ./scripts/health-check.sh
# ============================================================

set -euo pipefail

echo "🏥 Kafka Cluster Health Check"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

PASS=0
FAIL=0

check() {
    local name="$1"
    local cmd="$2"
    local expected="$3"

    result=$(eval "$cmd" 2>/dev/null || echo "FAILED")
    if echo "$result" | grep -q "$expected"; then
        echo "  ✅ $name"
        ((PASS++))
    else
        echo "  ❌ $name — got: $result"
        ((FAIL++))
    fi
}

# ZooKeeper
echo ""
echo "📦 ZooKeeper"
check "zoo1 responds to 'ruok'" \
    "docker exec zoo1 bash -c 'echo ruok | nc localhost 2181'" \
    "imok"

# Kafka Brokers
echo ""
echo "📦 Kafka Brokers"
for broker in kafka1 kafka2 kafka3; do
    check "$broker is running" \
        "docker inspect --format='{{.State.Running}}' $broker" \
        "true"
done

# Broker API check
echo ""
echo "📡 Broker Connectivity"
check "kafka1 API available" \
    "docker exec kafka1 kafka-broker-api-versions --bootstrap-server kafka1:19092 2>&1 | head -1" \
    "ApiVersion"

# Summary
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Results: ✅ $PASS passed, ❌ $FAIL failed"

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo "💡 Tip: Run 'docker compose logs' to investigate failures."
    exit 1
fi
