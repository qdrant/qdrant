#!/bin/bash
# This runs the auth integration tests in isolation

set -eux
set -o pipefail

# Ensure current path is project root
cd "$(dirname "$0")/../"


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

QDRANT_HOST='localhost'
until curl --output /dev/null --silent --get --fail -H 'api-key: my-ro-secret' http://$QDRANT_HOST:6333/collections; do
  printf 'waiting for server to start...'
  sleep 5
done

echo "server ready to serve traffic"

IMAGE_NAME=$(docker buildx build --load -q "tests/api_key")
docker run --rm \
       -e QDRANT_HOST=host.docker.internal \
       --add-host host.docker.internal:host-gateway \
       $IMAGE_NAME sh -c "pytest /tests"

