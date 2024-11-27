#!/usr/bin/env bash

set -ex

export QDRANT_HOST="localhost:6333"

SCRIPT_DIR=$(realpath "$(dirname "$0")")

# Ensure current path is project root
cd "$(dirname "$0")/../../"

USE_DOCKER=${USE_DOCKER:-"1"}

QDRANT_VERSION=${QDRANT_VERSION:-""}

# Ask for version if not provided

if [ -z "$QDRANT_VERSION" ]; then
  read -p "Enter the version of qdrant that was used to generate this compatibility data (example - v1.7.4): " QDRANT_VERSION
fi

if [ $USE_DOCKER -eq 0 ]; then
  # Delete previous storage
  rm -rf ./storage

  # Run qdrant
  cargo build
  ./target/debug/qdrant & PID=$!
else
  docker run --rm -it -v $(pwd)/storage:/qdrant/storage debian:12-slim bash -c "rm -rf /qdrant/storage/*"
  docker run -d --rm --network=host -v $(pwd)/storage:/qdrant/storage --name=gen-storage-compatibility  qdrant/qdrant:$QDRANT_VERSION
fi

function teardown()
{
  echo "server is going down"

  if [ $USE_DOCKER -eq 0 ]; then
    kill $PID || true
  else
    docker kill gen-storage-compatibility || true
  fi
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
tests/storage-compat/populate_db.py

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

teardown

rm "${SCRIPT_DIR}/full-snapshot.snapshot.gz" || true

gzip "${SCRIPT_DIR}/full-snapshot.snapshot"

rm "${SCRIPT_DIR}/storage.tar.bz2" || true

sudo chown -R $(whoami) ./storage

# Save current storage folder
tar -cjvf "${SCRIPT_DIR}/storage.tar.bz2" ./storage

cd "${SCRIPT_DIR}"
tar -cvf "./compatibility-${QDRANT_VERSION}.tar" "storage.tar.bz2" "full-snapshot.snapshot.gz"
cd -

echo "Compatibility data saved to ${SCRIPT_DIR}/compatibility-${QDRANT_VERSION}.tar"
echo "Upload it to 'qdrant-backward-compatibility' gcs bucket (requires access rights)"
