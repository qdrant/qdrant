#!/bin/bash
# This runs the auth integration tests in isolation

set -eux
set -o pipefail

# Ensure current path is project root
cd "$(dirname "$0")/../"

QDRANT_HOST='localhost:6333'
export QDRANT__SERVICE__GRPC_PORT="6334"

./target/debug/qdrant --config-path config/production_auth.yml &

# Sleep to make sure the process has started (workaround for empty pidof)
sleep 5

## Capture PID of the run
PID=$(pidof "./target/debug/qdrant")
echo $PID

function clear_after_tests()
{
  echo "server is going down"
  kill -9 $PID
  echo "END"
}

trap clear_after_tests EXIT

until curl --output /dev/null --silent --get --fail -H 'api-key: my-ro-secret' http://$QDRANT_HOST/collections; do
  printf 'waiting for server to start...'
  sleep 5
done

echo "server ready to serve traffic"

pytest ./tests/api_key
