#!/usr/bin/env bash
# This runs validates the storage compatibility

set -ex
echo $PWD
# Ensure current path is project root
cd "$(dirname "$0")/../../"

QDRANT_HOST='localhost:6333'
LEGACY_QDRANT_VERSION='v1.7.4'

# Build
cargo build

wget "https://storage.googleapis.com/qdrant-backward-compatibility/compatibility-${LEGACY_QDRANT_VERSION}.tar" -O ./tests/storage-compat/compatibility.tar

# Uncompress compatibility
tar -xvf ./tests/storage-compat/compatibility.tar -C ./tests/storage-compat/

# Uncompress snapshot storage
tar -xvjf ./tests/storage-compat/storage.tar.bz2

# Test it boots up fine with the old storage
./target/debug/qdrant & PID=$!

sleep 1

declare retry=0
until curl --output /dev/null --silent --get --fail http://$QDRANT_HOST/collections; do
  if ((retry++ < 30)); then
      printf 'waiting for server to start...'
      sleep 1
  else
      echo "Collections failed to load in ~30 seconds" >&2
      exit 2
  fi
done

echo "server ready to serve traffic"

# make sure all collections are loaded properly
collections=$(curl http://$QDRANT_HOST/collections | jq -r .result.collections[].name)
for collection in $collections; do
    info=$(curl -s -o /dev/null -w "%{http_code}" "http://$QDRANT_HOST/collections/$collection")
    if [ "$info" -ne 200 ]; then
        echo "Storage compatibility failed for $collection"
        kill -9 $PID
        exit 1
    fi
done

echo "server is going down"
kill -9 $PID
echo "END"


# Test recovering from an old snapshot
gzip -f -d --keep ./tests/storage-compat/full-snapshot.snapshot.gz

rm -rf ./storage
./target/debug/qdrant \
  --storage-snapshot ./tests/storage-compat/full-snapshot.snapshot \
  & PID=$!

declare retry=0
until curl --output /dev/null --silent --get --fail http://$QDRANT_HOST/readyz; do
  if ((retry++ < 30)); then
      printf 'waiting for server to start...'
      sleep 1
  else
      echo "Collection failed to load in ~30 seconds" >&2
      exit 2
  fi
done

echo "server ready to serve traffic"

# make sure all collections are loaded properly
collections=$(curl http://$QDRANT_HOST/collections | jq -r .result.collections[].name)
for collection in $collections; do
    info=$(curl -s -o /dev/null -w "%{http_code}" "http://$QDRANT_HOST/collections/$collection")
    if [ "$info" -ne 200 ]; then
        echo "Snapshot compatibility failed for $collection"
        kill -9 $PID
        exit 1
    fi
done

echo "server is going down"
kill -9 $PID

rm tests/storage-compat/full-snapshot.snapshot

echo "END"
