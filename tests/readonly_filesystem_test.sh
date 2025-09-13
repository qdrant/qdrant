#!/usr/bin/env bash
# This test checks that Qdrant works correctly when running in read-only mode

set -ex

QDRANT_HOST=${QDRANT_HOST:-'localhost:6337'}
QDRANT_EXECUTABLE="./target/debug/qdrant"

qdrant_host_headers=()

if [ -n "${QDRANT_HOST_HEADERS}" ]; then
  while read h; do
    qdrant_host_headers+=("-H" "$h")
  done <<<  $(echo "${QDRANT_HOST_HEADERS}" | jq -r 'to_entries|map("\(.key): \(.value)")[]')
fi

# cleanup collection if it exists
curl -X DELETE "http://$QDRANT_HOST/collections/test_collection" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  -s || true

# create collection
curl -X PUT "http://$QDRANT_HOST/collections/test_collection" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s \
  --data-raw '{
      "vectors": {
        "size": 4,
        "distance": "Dot"
      }
  }' | jq

curl -X PUT "http://$QDRANT_HOST/collections/test_collection/points?wait=true" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s \
  --data-raw '{
      "points": [
          {
              "id": 1,
              "vector": [0.1, 0.2, 0.3, 0.4],
              "payload": {"city": "Berlin", "test": "readonly_lifecycle"}
          },
          {
              "id": 2,
              "vector": [0.5, 0.6, 0.7, 0.8],
              "payload": {"city": "Paris", "test": "readonly_lifecycle"}
          }
      ]
  }' | jq

TEST_COLLECTION="test_collection"
EXPECTED_COUNT="2"

# kill the current server
if [ -n "$PID" ]; then
    kill -9 $PID || true
fi

# start server in read-only mode with proper environment variables
echo "Starting server in read-only mode..."
export QDRANT__SERVICE__HTTP_PORT="6337"
export QDRANT__SERVICE__GRPC_PORT="6338"
$QDRANT_EXECUTABLE --read-only &
READONLY_PID=$!

cleanup() {
  kill -9 $READONLY_PID
}

trap cleanup EXIT

until curl --output /dev/null --silent --get --fail http://$QDRANT_HOST/collections; do
  printf 'waiting for server to start...'
  sleep 5
done

# check that our collection still exists
curl -X GET "http://$QDRANT_HOST/collections/$TEST_COLLECTION" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s | jq

# verify our points are still there
echo "Verifying points persisted..."
READONLY_POINT_COUNT=$(curl -X POST "http://$QDRANT_HOST/collections/$TEST_COLLECTION/points/count" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s \
  --data-raw '{"exact": true}' | jq -r '.result.count')

if [ "$READONLY_POINT_COUNT" != "$EXPECTED_COUNT" ]; then
    echo "expected $EXPECTED_COUNT persisted points, got $READONLY_POINT_COUNT"
    exit 1
fi

# test that read operations work
curl -X POST "http://$QDRANT_HOST/collections/$TEST_COLLECTION/points/scroll" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s \
  --data-raw '{
      "limit": 10,
      "with_payload": true,
      "with_vector": true
  }' | jq

# test that write operations are forbidden 
echo "Testing point insertion (should fail with 403)..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "http://$QDRANT_HOST/collections/$TEST_COLLECTION/points" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --data-raw '{
      "points": [
          {
              "id": 999,
              "vector": [0.9, 0.8, 0.7, 0.6],
              "payload": {"city": "Tokyo", "test": "should_fail"}
          }
      ]
  }')

if [ "$HTTP_CODE" != "403" ]; then
    echo "expected 403 Forbidden for point insertion, got HTTP $HTTP_CODE"
    exit 1
fi

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://$QDRANT_HOST/collections/$TEST_COLLECTION/points/delete" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --data-raw '{
      "points": [1]
  }')

if [ "$HTTP_CODE" != "403" ]; then
    echo "expected 403 Forbidden for point deletion, got HTTP $HTTP_CODE"
    exit 1
fi

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "http://$QDRANT_HOST/collections/new_readonly_collection" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --data-raw '{
      "vectors": {
        "size": 4,
        "distance": "Dot"
      }
  }')

if [ "$HTTP_CODE" != "403" ]; then
    echo "expected 403 Forbidden for collection creation, got HTTP $HTTP_CODE"
    exit 1
fi

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "http://$QDRANT_HOST/collections/$TEST_COLLECTION" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}")

if [ "$HTTP_CODE" != "403" ]; then
    echo "expected 403 Forbidden for collection deletion, got HTTP $HTTP_CODE"
    exit 1
fi
