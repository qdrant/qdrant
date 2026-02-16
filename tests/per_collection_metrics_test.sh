#\!/usr/bin/env bash
set -e

# Test script for per-collection metrics feature
# Validates that collection labels appear in Prometheus metrics

QDRANT_HOST="http://localhost:6333"
TEST_COLLECTION="test_metrics_collection_1771263853"

echo "Testing per-collection metrics feature"
echo "Qdrant host: "
echo "Test collection: "

# Create test collection
echo "Creating collection..."
curl -s -X PUT "/collections/"   -H 'Content-Type: application/json'   -d '{"vectors": {"size": 4, "distance": "Cosine"}}' > /dev/null

# Insert test point to generate metrics
echo "Inserting test point..."
curl -s -X PUT "/collections//points"   -H 'Content-Type: application/json'   -d '{"points": [{"id": 1, "vector": [0.1, 0.2, 0.3, 0.4], "payload": {"test": "data"}}]}' > /dev/null

# Wait for metrics to be collected
sleep 2

# Fetch Prometheus metrics
echo "Fetching metrics..."
METRICS=

# Check if per-collection metrics exist
echo "Validating per-collection metrics..."

# Check REST metrics with collection label
if echo "" | grep -q "rest_collection_responses_total.*collection=\"\""; then
    echo "✓ REST per-collection metrics found"
else
    echo "✗ REST per-collection metrics NOT found"
    exit 1
fi

# Check that global metrics still exist (without collection label)
if echo "" | grep -q "rest_responses_total{.*method="; then
    echo "✓ Global REST metrics found"
else
    echo "✗ Global REST metrics NOT found"
    exit 1
fi

# Check telemetry endpoint has per_collection data
echo "Validating telemetry endpoint..."
TELEMETRY=

if echo "" | grep -q "responses_per_collection"; then
    echo "✓ Telemetry contains responses_per_collection"
else
    echo "✗ Telemetry missing responses_per_collection"
    exit 1
fi

# Cleanup
echo "Cleaning up..."
curl -s -X DELETE "/collections/" > /dev/null

echo ""
echo "✓ All per-collection metrics tests passed\!"
