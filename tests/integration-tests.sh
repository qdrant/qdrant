#!/usr/bin/env bash
# This runs all integration test in isolation

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

QDRANT_HOST='localhost:6333'
export QDRANT__SERVICE__GRPC_PORT="6334"

MODE=$1
# Enable distributed mode on demand
if [ "$MODE" == "distributed" ]; then
  export QDRANT__CLUSTER__ENABLED="true"
  # Run in background
  ./target/debug/qdrant --uri "http://127.0.0.1:6335" &
else
  # Run in background
  ./target/debug/qdrant &
fi

## Capture PID of the run
PID=$!
echo $PID

function clear_after_tests()
{
  echo "server is going down"
  kill -9 $PID
  echo "END"
}

trap clear_after_tests SIGINT
trap clear_after_tests EXIT

until curl --output /dev/null --silent --get --fail http://$QDRANT_HOST/collections; do
  printf 'waiting for server to start...'
  sleep 5
done

echo "server ready to serve traffic"

# Wait for the peer to establish the leader and commit initial settings
if [ "$MODE" == "distributed" ]; then
  sleep 10
fi

pytest tests/openapi --durations=10

./tests/basic_api_test.sh

./tests/basic_sparse_test.sh

./tests/basic_grpc_test.sh

./tests/basic_sparse_grpc_test.sh

./tests/basic_multivector_grpc_test.sh

./tests/basic_query_grpc_test.sh
