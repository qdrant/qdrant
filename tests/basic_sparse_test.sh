#!/usr/bin/env bash
# This test checks that Qdrant answers to all API mentioned in README.md as expected

set -ex

QDRANT_HOST=${QDRANT_HOST:-'localhost:6333'}
COLLECTION_NAME=${COLLECTION_NAME:-'sparse_collection'}

qdrant_host_headers=()

if [ -n "${QDRANT_HOST_HEADERS}" ]; then
  while read h; do
    qdrant_host_headers+=("-H" "$h")
  done <<<  $(echo "${QDRANT_HOST_HEADERS}" | jq -r 'to_entries|map("\(.key): \(.value)")[]')
fi

# cleanup collection if it exists
curl -X DELETE "http://$QDRANT_HOST/collections/$COLLECTION_NAME" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s \
  --fail -s | jq

# TODO(sparse) add validation and tests for case where dense and sparse vectors have the same name
# create collection
curl -X PUT "http://$QDRANT_HOST/collections/$COLLECTION_NAME" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s \
  --data-raw '{
      "sparse_vectors": {
        "text": { }
      },
      "optimizers_config": {
        "default_segment_number": 2
      },
      "replication_factor": 2
    }' | jq

curl -L -X PUT  "http://$QDRANT_HOST/collections/$COLLECTION_NAME/index" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s \
  --data-raw '{
      "field_name": "city",
      "field_schema": "keyword"
    }' | jq

curl -L -X PUT  "http://$QDRANT_HOST/collections/$COLLECTION_NAME/index" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s \
  --data-raw '{
      "field_name": "count",
      "field_schema": "integer"
    }' | jq

curl -L -X PUT  "http://$QDRANT_HOST/collections/$COLLECTION_NAME/index" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s \
  --data-raw '{
      "field_name": "coords",
      "field_schema": "geo"
    }' | jq

curl --fail -s "http://$QDRANT_HOST/collections/$COLLECTION_NAME" "${qdrant_host_headers[@]}" | jq

# insert points
curl -L -X PUT "http://$QDRANT_HOST/collections/$COLLECTION_NAME/points?wait=true" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s \
  --data-raw '{
      "points": [
        {
          "id": 1,
          "vector": {
            "text": {
              "values": [0.05, 0.61, 0.76, 0.74],
              "indices": [0, 1, 2, 3]
            }
          },
          "payload": {
            "city": "Berlin",
            "country": "Germany" ,
            "count": 1000000,
            "square": 12.5,
            "coords": { "lat": 1.0, "lon": 2.0 }
          }
        },
        {
          "id": 2,
          "vector": {
            "text": {
              "values": [0.19, 0.81, 0.75, 0.11],
              "indices": [0, 1, 2, 3]
            }
          },
          "payload": {"city": ["Berlin", "London"]}
        },
        {
          "id": 3,
          "vector": {
            "text": {
              "values": [0.36, 0.55, 0.47, 0.94],
              "indices": [0, 1, 2, 3]
            }
          },
          "payload": {"city": ["Berlin", "Moscow"]}
        },
        {
          "id": 4,
          "vector": {
            "text": {
              "values": [0.18, 0.01, 0.85, 0.80],
              "indices": [0, 1, 2, 3]
            }
          },
          "payload": {"city": ["London", "Moscow"]}
        },
        {
          "id": "98a9a4b1-4ef2-46fb-8315-a97d874fe1d7",
          "vector": {
            "text": {
              "values": [0.24, 0.18, 0.22, 0.44],
              "indices": [0, 1, 2, 3]
            }
          },
          "payload": {"count": [0]}
        },
        {
          "id": "f0e09527-b096-42a8-94e9-ea94d342b925",
          "vector": {
            "text": {
              "values": [0.35, 0.08, 0.11, 0.44],
              "indices": [0, 1, 2, 3]
            }
          }
        }
      ]
    }' | jq

# retrieve point
curl -L -X GET "http://$QDRANT_HOST/collections/$COLLECTION_NAME/points/2" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s | jq

# retrieve points
curl -L -X POST "http://$QDRANT_HOST/collections/$COLLECTION_NAME/points" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s \
  --data-raw '{
      "ids": [1, 2],
      "with_vectors": true
    }' | jq

SAVED_POINTS_COUNT=$(curl --fail -s "http://$QDRANT_HOST/collections/$COLLECTION_NAME" "${qdrant_host_headers[@]}" | jq '.result.points_count')
[[ "$SAVED_POINTS_COUNT" == "6" ]] || {
  echo 'check failed - 6 points expected'
  exit 1
}

# search points
curl -L -X POST "http://$QDRANT_HOST/collections/$COLLECTION_NAME/points/search" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s \
  --data-raw '{
        "vector": {
          "name": "text",
          "vector": {
            "values": [0.2, 0.1, 0.9, 0.7],
            "indices": [0, 1, 2, 3]
          }
        },
        "top": 3
    }' | jq

# search points batch
curl -L -X POST "http://$QDRANT_HOST/collections/$COLLECTION_NAME/points/search/batch" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s \
  --data-raw '{
    "searches": [
      {
        "vector": {
          "name": "text",
          "vector": {
            "values": [0.2, 0.1, 0.9, 0.7],
            "indices": [0, 1, 2, 3]
          }
        },
        "top": 3
      },
      {
        "vector": {
          "name": "text",
          "vector": {
            "values": [0.2 ,0.1, 0.9, 0.7],
            "indices": [0, 1, 2, 3]
          }
        },
        "top": 3
      }
    ]
  }' | jq

curl -L -X POST "http://$QDRANT_HOST/collections/$COLLECTION_NAME/points/search" \
  -H 'Content-Type: application/json' "${qdrant_host_headers[@]}" \
  --fail -s \
  --data-raw '{
      "filter": {
          "should": [
              {
                  "key": "city",
                  "match": {
                      "value": "London"
                  }
              }
          ]
      },
      "vector": {
        "name": "text",
        "vector": {
          "values": [0.2, 0.1, 0.9, 0.7],
          "indices": [0, 1, 2, 3]
        }
      },
      "top": 3
  }' | jq
