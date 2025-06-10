#!/usr/bin/env bash
# This test checks that Qdrant answers to all API mentioned in README.md as expected

set -ex

QDRANT_HOST=${QDRANT_HOST:-'localhost:6333'}

qdrant_host_headers=()

if [ -n "${QDRANT_HOST_HEADERS}" ]; then
  while read h; do
    qdrant_host_headers+=("-H" "$h")
  done <<<  $(echo "${QDRANT_HOST_HEADERS}" | jq -r 'to_entries|map("\(.key): \(.value)")[]')
fi

curl -X PATCH "http://$QDRANT_HOST/collections/benchmark" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --data-raw '{
      "quantization_config": {
        "binary": {
          "always_ram": true,
          "encoding": "two_bits"
        }
      }
    }' | jq
