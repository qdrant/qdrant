#!/bin/bash
# This test checks that Qdrant answers to all API mentioned in README.md as expected

# laion-768-6m-mmap
# laion-768-6m-mmap-m128
# laion-768-6m-mmap-m128-noheuristic
# laion-768-6m-mmap-m128-combined

set -ex

QDRANT_HOST=${QDRANT_HOST:-'localhost:6333'}

# create collection
curl -X POST "http://$QDRANT_HOST/collections/laion-768-6m-mmap-m64-nearest/snapshots?wait=true" \
  -H 'Content-Type: application/json' \
  --data-raw '{}' | jq

time seq 1000 | xargs -P 8 -I {} curl -L -X POST "http://127.0.0.1:6333/collections/laion-768-6m-mmap-m64-combined/points/recommend" -H 'Content-Type: application/json' --data-raw '{ "limit": 10, "positive": [{}], "params": { "quantization": { "rescore": false  } } }' -s | jq .status | wc -l