#!/bin/bash
# This runs validates the storage compatibility

set -ex
echo $PWD
# Ensure current path is project root
cd "$(dirname "$0")/../../"

QDRANT_HOST='localhost:6333'

# Build
cargo build

# Sync git large file
git lfs pull

# Uncompress snapshot storage
tar -xvjf ./tests/storage-compat/storage.tar.bz2

# Run in background
./target/debug/qdrant &

# Sleep to make sure the process has started (workaround for empty pidof)
sleep 5

## Capture PID of the run
PID=$(pidof "./target/debug/qdrant")
echo $PID

until curl --output /dev/null --silent --get --fail http://$QDRANT_HOST/collections; do
  printf 'waiting for server to start...'
  sleep 5
done

echo "server ready to serve traffic"

echo "server is going down"
kill -9 $PID
echo "END"
