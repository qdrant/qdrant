#!/usr/bin/env bash
set -euo pipefail

QDRANT_HOST="${QDRANT_HOST:-http://localhost:6333}"
TEST_COLLECTION="test_metrics_collection_$(date +%s)"

cleanup() {
    curl -sS -X DELETE "$QDRANT_HOST/collections/$TEST_COLLECTION" > /dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Testing per-collection metrics feature"
echo "Qdrant host: $QDRANT_HOST"
echo "Test collection: $TEST_COLLECTION"

echo "Creating collection..."
curl -fsS -X PUT "$QDRANT_HOST/collections/$TEST_COLLECTION" \
  -H "Content-Type: application/json" \
  -d '{"vectors": {"size": 4, "distance": "Cosine"}}' > /dev/null

echo "Inserting test point..."
curl -fsS -X PUT "$QDRANT_HOST/collections/$TEST_COLLECTION/points" \
  -H "Content-Type: application/json" \
  -d '{"points":[{"id":1,"vector":[0.1,0.2,0.3,0.4],"payload":{"test":"data"}}]}' > /dev/null

sleep 2

echo "Fetching metrics..."
METRICS="$(curl -fsS "$QDRANT_HOST/metrics")"

echo "Validating per-collection metrics..."
if echo "$METRICS" | grep -q "rest_collection_responses_total.*collection=\"$TEST_COLLECTION\""; then
  echo "✓ REST per-collection metrics found"
else
  echo "✗ REST per-collection metrics NOT found"
  exit 1
fi

if echo "$METRICS" | grep -q "rest_responses_total{.*method="; then
  echo "✓ Global REST metrics found"
else
  echo "✗ Global REST metrics NOT found"
  exit 1
fi

echo "Validating telemetry endpoint..."
TELEMETRY="$(curl -fsS "$QDRANT_HOST/telemetry")"
if echo "$TELEMETRY" | grep -q "responses_per_collection"; then
  echo "✓ Telemetry contains responses_per_collection"
else
  echo "✗ Telemetry missing responses_per_collection"
  exit 1
fi

echo ""
echo "✓ All per-collection metrics tests passed!"
