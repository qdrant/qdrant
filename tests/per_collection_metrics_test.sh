#!/usr/bin/env bash
# This test checks that Qdrant exposes per-collection metrics when enabled.
# Prerequisite: Qdrant must be running with `QDRANT_SERVICE__RECORD_PER_COLLECTION=true`.

set -e

QDRANT_HOST=${QDRANT_HOST:-'localhost:6333'}
COLLECTION_NAME="test_collection_metrics"

echo "Using Qdrant host: $QDRANT_HOST"

# 1. Cleanup & Create collection
echo "Creating collection $COLLECTION_NAME..."
# Attempt to delete collection if it exists, ignore output/errors
curl -X DELETE "http://$QDRANT_HOST/collections/$COLLECTION_NAME" -s > /dev/null || true

curl -X PUT "http://$QDRANT_HOST/collections/$COLLECTION_NAME" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
      "vectors": {
        "size": 4,
        "distance": "Dot"
      }
    }'


# 2. Insert points
echo "Inserting points..."
curl -X PUT "http://$QDRANT_HOST/collections/$COLLECTION_NAME/points?wait=true" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
      "points": [
        {"id": 1, "vector": [0.05, 0.61, 0.76, 0.74]},
        {"id": 2, "vector": [0.19, 0.81, 0.75, 0.11]}
      ]
    }'


# 3. Search points (to generate read metrics)
echo "Searching points..."
curl -X POST "http://$QDRANT_HOST/collections/$COLLECTION_NAME/points/search" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
        "vector": [0.2,0.1,0.9,0.7],
        "top": 3
    }'


# 4. Fetch metrics and verify
echo "Fetching metrics..."
METRICS=$(curl -s "http://$QDRANT_HOST/metrics")

echo "Verifying per-collection metrics..."

# Check for existence of collection label in rest responses
if echo "$METRICS" | grep -q "rest_responses_total{.*collection=\"$COLLECTION_NAME\".*}"; then
  echo "Found per-collection rest_responses_total"
else
  echo "Failed to find per-collection rest_responses_total"
  exit 1
fi

# Check for existence of collection label in collection_points
# Note: collection_points uses 'collection' label now (updated in metrics.rs)
if echo "$METRICS" | grep -q "collection_points{.*collection=\"$COLLECTION_NAME\""; then
  echo "Found per-collection collection_points"
else
  echo "Failed to find per-collection collection_points"
  # Fallback check if it still uses 'id' (debugging)
  if echo "$METRICS" | grep -q "collection_points{.*id=\"$COLLECTION_NAME\""; then
     echo "Found collection_points with 'id' label instead of 'collection'"
  fi
  exit 1
fi

echo "All per-collection metrics verification passed!"

# Cleanup
echo "Cleaning up..."
curl -X DELETE "http://$QDRANT_HOST/collections/$COLLECTION_NAME" -s

