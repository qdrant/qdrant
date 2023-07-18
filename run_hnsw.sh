#!/bin/bash
# This test checks that Qdrant answers to all API mentioned in README.md as expected

# laion-768-6m-mmap
# laion-768-6m-mmap-m128
# laion-768-6m-mmap-m128-noheuristic
# laion-768-6m-mmap-m128-combined

set -ex

QDRANT_HOST=${QDRANT_HOST:-'localhost:6333'}

# cleanup collection if it exists
curl -X DELETE "http://$QDRANT_HOST/collections/laion-768-6m-sq-mmap" \
  -H 'Content-Type: application/json' \
  --fail -s | jq

# create collection
curl -X PUT "http://$QDRANT_HOST/collections/laion-768-6m-sq-mmap/snapshots/recover?wait=true" \
  -H 'Content-Type: application/json' \
  --data-raw '{
      "location": "https://storage.googleapis.com/common-datasets-snapshots/laion-768-6m-sq-mmap.shapshot"
    }' | jq

# cleanup collection if it exists
#curl -X DELETE "http://$QDRANT_HOST/collections/laion-768-6m-sq-mmap" \
#  -H 'Content-Type: application/json' \
#  --fail -s | jq

# create collection
#curl -X PUT "http://$QDRANT_HOST/collections/laion-768-6m-sq-mmap/snapshots/recover?wait=true" \
#  -H 'Content-Type: application/json' \
#  --data-raw '{
#      "location": "https://storage.googleapis.com/common-datasets-snapshots/laion-768-6m-sq-mmap.shapshot"
#    }' | jq

#curl -X PATCH "http://$QDRANT_HOST/collections/laion-768-6m-sq-mmap/rebuild_hnsw" \
#  -H 'Content-Type: application/json' \
#  --data-raw '{
#      "m": 64,
#      "ef": 512
#    }' | jq
