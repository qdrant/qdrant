#!/usr/bin/env bash
# Creates snapshot for the `facet_test.rs` example.

SNAPSHOT_DIR="./data/facet_test"
QDRANT_URL="http://localhost:6333"
COLLECTION_NAME="test_facet"
CONTAINER_NAME="qdrant-facet-test"

set -euo pipefail
cd "$(dirname "$0")/.."

# Let our logs stand out
log() { printf "\x1b[33m# %s\x1b[0m\n" "$*"; }

DOCKER_STARTED=0
cleanup() {
    log "Dropping collection..."
    curl -sf -X DELETE "$QDRANT_URL/collections/$COLLECTION_NAME?wait=true" || true

    if [ "$DOCKER_STARTED" = "1" ]; then
        DOCKER_STARTED=0
        log "Cleaning up Docker container..."
        docker stop "$CONTAINER_NAME" && docker rm "$CONTAINER_NAME" || true
    fi
}
trap cleanup EXIT

# Add newlines after curl because qdrant's responses aren't newline-terminated
curl() { command curl "$@"; local rc=$?; echo >&2; return $rc; }

mkdir -p "$SNAPSHOT_DIR"

# Check if Qdrant is already running
if curl -sf "$QDRANT_URL" >/dev/null 2>&1; then
    log "Qdrant already running at $QDRANT_URL, skipping docker start"
else
    log "Starting Qdrant in Docker..."
    DOCKER_STARTED=1
    docker run -d --name "$CONTAINER_NAME" -p 6333:6333 -p 6334:6334 qdrant/qdrant:latest
    log "Waiting for Qdrant to start..."
    curl -sf --retry 30 --retry-delay 1 --retry-all-errors "$QDRANT_URL" >/dev/null
fi

log "Dropping collection if it exists..."
curl -sf -X DELETE "$QDRANT_URL/collections/$COLLECTION_NAME?wait=true"

log "Creating collection..."
curl -sf -X PUT "$QDRANT_URL/collections/$COLLECTION_NAME" \
  -H 'Content-Type: application/json' \
  -d '{"vectors": {"size": 4, "distance": "Dot"}}'

log "Inserting points..."
curl -sf -X PUT "$QDRANT_URL/collections/$COLLECTION_NAME/points?wait=true" \
  -H 'Content-Type: application/json' \
  -d '{
    "points": [
      {"id": 1, "vector": [0.1, 0.2, 0.3, 0.4], "payload": {"color": "red", "city": "Berlin"}},
      {"id": 2, "vector": [0.2, 0.3, 0.4, 0.5], "payload": {"color": "blue", "city": "Paris"}},
      {"id": 3, "vector": [0.3, 0.4, 0.5, 0.6], "payload": {"color": "green", "city": "London"}},
      {"id": 4, "vector": [0.4, 0.5, 0.6, 0.7], "payload": {"color": "red", "city": "London"}},
      {"id": 5, "vector": [0.5, 0.6, 0.7, 0.8], "payload": {"color": "blue", "city": "Berlin"}},
      {"id": 6, "vector": [0.6, 0.7, 0.8, 0.9], "payload": {"color": "red", "city": "Paris"}},
      {"id": 7, "vector": [0.7, 0.8, 0.9, 1.0], "payload": {"color": "green", "city": "Berlin"}},
      {"id": 8, "vector": [0.8, 0.9, 1.0, 1.1], "payload": {"color": "yellow", "city": "Tokyo"}},
      {"id": 9, "vector": [0.9, 1.0, 1.1, 1.2], "payload": {"color": "blue", "city": "Seoul"}},
      {"id": 10, "vector": [1.0, 1.1, 1.2, 1.3], "payload": {"color": "red", "city": "Tokyo"}}
    ]
  }'

log "Creating payload indexes..."
curl -sf -X PUT "$QDRANT_URL/collections/$COLLECTION_NAME/index" \
  -H 'Content-Type: application/json' \
  -d '{"field_name": "color", "field_schema": "keyword"}'

curl -sf -X PUT "$QDRANT_URL/collections/$COLLECTION_NAME/index" \
  -H 'Content-Type: application/json' \
  -d '{"field_name": "city", "field_schema": "keyword"}'

log "Creating shard snapshot..."
SNAPSHOT_RESPONSE=$(curl -sf -X POST "$QDRANT_URL/collections/$COLLECTION_NAME/shards/0/snapshots")
SNAPSHOT_NAME=$(echo "$SNAPSHOT_RESPONSE" | jq -r '.result.name')
log "Snapshot name: $SNAPSHOT_NAME"

log "Downloading snapshot..."
curl -sf -o "$SNAPSHOT_DIR/shard.snapshot" \
  "$QDRANT_URL/collections/$COLLECTION_NAME/shards/0/snapshots/$SNAPSHOT_NAME"

log "Snapshot saved to $SNAPSHOT_DIR/shard.snapshot"

# cleanup should be called automatically
