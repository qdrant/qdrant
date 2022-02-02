#!/bin/bash
# This test checks that Qdrant answers to all API mentioned in README.md as expected

set -ex

QDRANT_HOST='localhost:6333'

# cleanup collection if it exists
curl -X DELETE "http://$QDRANT_HOST/collections/test_collection" \
  -H 'Content-Type: application/json' \
  --fail -s | jq

# create collection
curl -X PUT "http://$QDRANT_HOST/collections/test_collection" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
      "vector_size": 4,
      "distance": "Dot"
    }' | jq

curl --fail -s "http://$QDRANT_HOST/collections/test_collection" | jq

# insert points
curl -L -X PUT "http://$QDRANT_HOST/collections/test_collection/points?wait=true" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
      "points": [
        {"id": 1, "vector": [0.05, 0.61, 0.76, 0.74], "payload": {"city": {"type": "keyword", "value": "Berlin"}}},
        {"id": 2, "vector": [0.19, 0.81, 0.75, 0.11], "payload": {"city": {"type": "keyword", "value": ["Berlin", "London"] }}},
        {"id": 3, "vector": [0.36, 0.55, 0.47, 0.94], "payload": {"city": {"type": "keyword", "value": ["Berlin", "Moscow"] }}},
        {"id": 4, "vector": [0.18, 0.01, 0.85, 0.80], "payload": {"city": {"type": "keyword", "value": ["London", "Moscow"]}}},
        {"id": 5, "vector": [0.24, 0.18, 0.22, 0.44], "payload": {"count": {"type": "integer", "value": [0]}}},
        {"id": 6, "vector": [0.35, 0.08, 0.11, 0.44]}
      ]
    }' | jq

# retrieve point
curl -L -X GET "http://$QDRANT_HOST/collections/test_collection/points/2" \
  -H 'Content-Type: application/json' \
  --fail -s | jq

# retrieve points
curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
      "ids": [1, 2]
    }' | jq

SAVED_VECTORS_COUNT=$(curl --fail -s "http://$QDRANT_HOST/collections/test_collection" | jq '.result.vectors_count')
[[ "$SAVED_VECTORS_COUNT" == "6" ]] || {
  echo 'check failed - 6 points expected'
  exit 1
}

# search points
curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/search" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
        "vector": [0.2,0.1,0.9,0.7],
        "top": 3
    }' | jq

curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/search" \
  --fail -s \
  -H 'Content-Type: application/json' \
  --data-raw '{
      "filter": {
          "should": [
              {
                  "key": "city",
                  "match": {
                      "keyword": "London"
                  }
              }
          ]
      },
      "vector": [0.2, 0.1, 0.9, 0.7],
      "top": 3
  }' | jq


# scroll points
curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/scroll" \
  --fail -s \
  -H 'Content-Type: application/json' \
  --data-raw '{ "offset": 2, "limit": 2, "with_vector": true }' | jq

# change aliases
curl -L -X POST "http://$QDRANT_HOST/collections/aliases" \
  --fail -s \
  -H 'Content-Type: application/json' \
  --data-raw '{
      "actions": [
          {
              "create_alias": {
                  "alias_name": "test_alias",
                  "collection_name": "test_collection"
              }
          }
      ]
  }' | jq

# search points
curl -L -X POST "http://$QDRANT_HOST/collections/test_alias/points/search" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
        "vector": [0.2,0.1,0.9,0.7],
        "top": 3
    }' | jq

# delete point by filter (has_id)
curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/delete?wait=true" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
    "filter": {
      "must": [
        { "has_id": [5] }
      ]
    }
  }' | jq

# quantity check if the above point id was deleted
SAVED_VECTORS_COUNT=$(curl --fail -s "http://$QDRANT_HOST/collections/test_collection" | jq '.result.vectors_count')
[[ "$SAVED_VECTORS_COUNT" == "5" ]] || {
  echo 'check failed - 5 points expected'
  exit 1
}

# delete points
curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/delete?wait=true" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
    "points" : [ 1, 2, 3, 4 ]
  }' | jq

SAVED_VECTORS_COUNT=$(curl --fail -s "http://$QDRANT_HOST/collections/test_collection" | jq '.result.vectors_count')
[[ "$SAVED_VECTORS_COUNT" == "1" ]] || {
  echo 'check failed - 1 points expected'
  exit 1
}

# create payload
curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/payload?wait=true" \
  -H 'Content-Type: application/json' \
  --data-raw '{
    "payload": { "test_payload" : "keyword" },
    "points": [ 6 ]
  }' \
  --fail -s | jq

# check payload
PAYLOAD_LEN=$(curl --fail -s "http://$QDRANT_HOST/collections/test_collection/points/6" | jq '.result.payload | length')
[[ "$PAYLOAD_LEN" == "1" ]] || {
  echo 'check failed - payload data expected'
  exit 1
}

# clean payload by filter
curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/payload/clear?wait=true" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
    "filter": {
      "must": [
        { "has_id": [6] }
      ]
    }
  }' | jq .

# check empty payload
PAYLOAD_LEN=$(curl --fail -s "http://$QDRANT_HOST/collections/test_collection/points/6" | jq '.result.payload | length')
[[ "$PAYLOAD_LEN" == "0" ]] || {
  echo 'check failed - empty payload expected'
  exit 1
}

# create payload
curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/payload?wait=true" \
  -H 'Content-Type: application/json' \
  --data-raw '{
    "payload": { "test_payload" : "keyword" },
    "points": [ 6 ]
  }' \
  --fail -s | jq


# index payload
INDEXED_FIELD=$(curl --fail -s "http://$QDRANT_HOST/collections/test_collection" | jq '.result.payload_schema.test_payload.indexed')
[[ "$INDEXED_FIELD" == "false" ]] || {
  echo 'check failed - field should not be indexed'
  exit 1
}

curl -X PUT "http://$QDRANT_HOST/collections/test_collection/index?wait=true" \
  -H 'Content-Type: application/json' \
  --data-raw '{
    "field_name": "test_payload"
  }' \
  --fail -s | jq

INDEXED_FIELD=$(curl --fail -s "http://$QDRANT_HOST/collections/test_collection" | jq '.result.payload_schema.test_payload.indexed')
[[ "$INDEXED_FIELD" == "true" ]] || {
  echo 'check failed - field should be indexed'
  exit 1
}

# delete index on payload
curl -X DELETE "http://$QDRANT_HOST/collections/test_collection/index/test_payload?wait=true" \
  -H 'Content-Type: application/json' \
  --fail -s | jq

INDEXED_FIELD=$(curl --fail -s "http://$QDRANT_HOST/collections/test_collection" | jq '.result.payload_schema.test_payload.indexed')
[[ "$INDEXED_FIELD" == "false" ]] || {
  echo 'check failed - field should not be indexed'
  exit 1
}

# delete payload
curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/payload/delete?wait=true" \
  -H 'Content-Type: application/json' \
  --data-raw '{
    "keys": [ "test_payload" ],
    "points": [ 6 ]
  }' \
  --fail -s | jq


# UUID points write
curl -L -X PUT "http://$QDRANT_HOST/collections/test_collection/points?wait=true" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
      "points": [
        {"id": "b524a3c4-c568-4383-8019-c9ca08243d6a", "vector": [0.15, 0.21, 0.96, 0.04], "payload": {"city": {"type": "keyword", "value": "Berlin"}}},
        {"id": "1d675313-d3dd-4646-8b98-8052364872da", "vector": [0.19, 0.81, 0.75, 0.11], "payload": {"city": {"type": "keyword", "value": ["Berlin", "London"] }}}
      ]
    }' | jq

# UUID points retrieve
curl -L -X GET "http://$QDRANT_HOST/collections/test_collection/points/1d675313-d3dd-4646-8b98-8052364872da" \
  -H 'Content-Type: application/json' \
  --fail -s | jq
