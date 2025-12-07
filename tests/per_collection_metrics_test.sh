#!/usr/bin/env bash
# This test checks that Qdrant properly exposes per-collection metrics.

set -e

# Ensure current path is project root
cd "$(dirname "$0")/../"

# Cleanup function to kill the background process on exit
cleanup() {
    if [ -n "$QDRANT_PID" ]; then
        echo "Stopping Qdrant (PID: $QDRANT_PID)..."
        kill "$QDRANT_PID"
        wait "$QDRANT_PID" || true
    fi
}
trap cleanup EXIT

# Build Qdrant
echo "Building Qdrant..."
cargo build

# Start Qdrant in current shell background
echo "Starting Qdrant..."
./target/debug/qdrant > qdrant.log 2>&1 &
QDRANT_PID=$!

# Wait for Qdrant to be ready
echo "Waiting for Qdrant to start..."
for i in {1..30}; do
    if curl -s http://localhost:6333/healthz > /dev/null; then
        echo "Qdrant is ready."
        break
    fi
    sleep 1
done

if ! curl -s http://localhost:6333/healthz > /dev/null; then
    echo "Qdrant failed to start."
    exit 1
fi

COLLECTION_NAME="metrics_test_collection"

# 1. Create Collection via REST
echo "Creating collection '$COLLECTION_NAME'..."
curl -X PUT "http://localhost:6333/collections/$COLLECTION_NAME" \
    -H 'Content-Type: application/json' \
    -d '{
        "vectors": {
            "size": 4,
            "distance": "Dot"
        }
    }'
echo ""

# 2. Upsert Points via REST
echo "Upserting points..."
curl -X PUT "http://localhost:6333/collections/$COLLECTION_NAME/points?wait=true" \
    -H 'Content-Type: application/json' \
    -d '{
        "points": [
            { "id": 1, "vector": [0.05, 0.61, 0.76, 0.74], "payload": { "city": "Berlin" } },
            { "id": 2, "vector": [0.19, 0.81, 0.75, 0.11], "payload": { "city": "London" } }
        ]
    }'
echo ""

# 3. Search Points via REST
echo "Searching points..."
curl -X POST "http://localhost:6333/collections/$COLLECTION_NAME/points/search" \
    -H 'Content-Type: application/json' \
    -d '{
        "vector": [0.2,0.1,0.9,0.7],
        "limit": 3
    }'
echo ""

# 4. Check Metrics for REST
echo "Checking REST metrics..."
METRICS=$(curl -s http://localhost:6333/metrics)
echo "$METRICS" > metrics_dump.txt

# Check for qdrant_rest_responses_total with collection label
if echo "$METRICS" | grep -q "rest_responses_total{.*collection=\"$COLLECTION_NAME\""; then
    echo "SUCCESS: Found REST metrics for collection '$COLLECTION_NAME'."
else
    echo "FAILURE: Did not find REST metrics for collection '$COLLECTION_NAME'."
    echo "Dumping grep output:"
    echo "$METRICS" | grep "rest_responses_total" | grep "$COLLECTION_NAME" || true
    exit 1
fi

# 5. gRPC Test (using dockerized grpcurl if available)
if command -v docker &> /dev/null; then
    echo "Running gRPC test using dockerized grpcurl..."
    
    docker_grpcurl=("docker" "run" "--rm" "--network=host" "-v" "${PWD}/lib/api/src/grpc/proto:/proto" "fullstorydev/grpcurl" "-plaintext" "-import-path" "/proto" "-proto" "qdrant.proto")
    GRPC_PAYLOAD="{
       \"collection_name\": \"$COLLECTION_NAME\",
       \"vector\": [0.2,0.1,0.9,0.7],
       \"limit\": 3
    }"

    # Try localhost first
    if ! "${docker_grpcurl[@]}" -d "$GRPC_PAYLOAD" localhost:6334 qdrant.Points/Search > /dev/null 2>&1; then
        echo "Retrying gRPC with host.docker.internal..."
        "${docker_grpcurl[@]}" -d "$GRPC_PAYLOAD" host.docker.internal:6334 qdrant.Points/Search > /dev/null
    fi

    # Check Metrics for gRPC
    echo "Checking gRPC metrics..."
    METRICS=$(curl -s http://localhost:6333/metrics)
    echo "$METRICS" > metrics_dump.txt

    # The metric name might vary based on implementation, usually grpc_responses_total or similar
    if echo "$METRICS" | grep -q "grpc_responses_total{.*collection=\"$COLLECTION_NAME\""; then
        echo "SUCCESS: Found gRPC metrics for collection '$COLLECTION_NAME'."
    else
        echo "FAILURE: Did not find gRPC metrics for collection '$COLLECTION_NAME'."
        echo "Dumping grep output:"
        echo "$METRICS" | grep "grpc_responses_total" || true
        exit 1
    fi
else
    echo "Docker not found, skipping gRPC verification."
fi

echo "All verifications passed!"
