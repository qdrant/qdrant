#!/usr/bin/env bash
# This runs all integration test in isolation

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

MODE=$1
QDRANT_HOST='localhost:6333'
export QDRANT__SERVICE__GRPC_PORT="6334"
export LLVM_PROFILE_FILE="./target/llvm-cov-target/qdrant-openapi-$MODE-%m.profraw"

if [ "$COVERAGE" == "1" ]; then
  QDRANT_EXECUTABLE="./target/llvm-cov-target/debug/qdrant"
else
  QDRANT_EXECUTABLE="./target/debug/qdrant"
fi

# Enable distributed mode on demand
if [ "$MODE" == "distributed" ]; then
  export QDRANT__CLUSTER__ENABLED="true"
  # Run in background
  $QDRANT_EXECUTABLE --uri "http://127.0.0.1:6335" &
else
  # Run in background
  $QDRANT_EXECUTABLE &
fi

## Capture PID of the run
PID=$!
echo $PID

function clear_after_tests()
{
  echo "server is going down"

  if [ "$COVERAGE" == "1" ]; then
    kill -2 $PID # interrupt instead of kill to allow graceful shutdown so we can get the coverage
    wait $PID
  else
    kill -9 $PID
  fi

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
