#!/bin/bash

set -ex

QDRANT_HOST="localhost:6333"

SCRIPT_DIR=$(realpath "$(dirname "$0")")

# Ensure current path is project root
cd "$(dirname "$0")/../../"

# Delete previous storage
rm -rf ./storage

# Run qdrant
cargo build
./target/debug/qdrant & PID=$!

function teardown()
{
  echo "server is going down"
  kill -9 $PID
  echo "END"
}

trap teardown EXIT

declare retry=0
until curl --output /dev/null --silent --get --fail http://$QDRANT_HOST/collections; do
  if ((retry++ < 30)); then
      printf 'waiting for server to start...'
      sleep 1
  else
      echo "Qdrant failed to boot in ~30 seconds" >&2
      exit 2
  fi
done

# Run python script to populate db
IMAGE_NAME=$(docker buildx build --load -q "${SCRIPT_DIR}/populate_db")
# For osx users, add the replace `--network="host"` with `-e QDRANT_HOST=host.docker.internal:6333`
docker run --rm \
            --network="host" \
            --add-host host.docker.internal:host-gateway \
            $IMAGE_NAME sh -c "python populate_db.py"

# Wait for indexing to finish
sleep 1

# Create snapshot
SNAPSHOT_NAME=$(
    curl -X POST "http://$QDRANT_HOST/snapshots" \
    -H 'Content-Type: application/json' \
    --fail -s | jq .result.name -r
)

# Download snapshot
curl -X GET "http://$QDRANT_HOST/snapshots/$SNAPSHOT_NAME" \
    --fail -s --output "${SCRIPT_DIR}/full-snapshot.snapshot"

gzip "${SCRIPT_DIR}/full-snapshot.snapshot"

# Save current storage folder
tar -cjvf "${SCRIPT_DIR}/storage.tar.bz2" ./storage
