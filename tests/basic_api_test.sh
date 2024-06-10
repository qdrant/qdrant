#!/usr/bin/env bash
# This test checks that Qdrant answers to all API mentioned in README.md as expected

set -ex

QDRANT_HOST=${QDRANT_HOST:-'localhost:6333'}

# cleanup collection if it exists
curl -X DELETE "http://$QDRANT_HOST/collections/test_collection" \
  -H 'Content-Type: application/json' \
  --fail -s | jq

# create collection
curl -X PUT "http://localhost:6333/collections/test_collection" -H 'Content-Type: application/json' --data-raw '{ "vectors": { "size": 4, "distance": "Dot" } }'

curl -L -X PUT  "http://$QDRANT_HOST/collections/test_collection/index" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
      "field_name": "city",
      "field_schema": "keyword"
    }' | jq

curl -L -X PUT  "http://$QDRANT_HOST/collections/test_collection/index" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
      "field_name": "count",
      "field_schema": "integer"
    }' | jq

curl -L -X PUT  "http://$QDRANT_HOST/collections/test_collection/index" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
      "field_name": "coords",
      "field_schema": "geo"
    }' | jq

curl --fail -s "http://$QDRANT_HOST/collections/test_collection" | jq

# insert points
curl -L -X PUT "http://$QDRANT_HOST/collections/test_collection/points?wait=true" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
      "points": [
        {
          "id": 1,
          "vector": [0.05, 0.61, 0.76, 0.74],
          "payload": {
            "city": "Berlin",
            "country": "Germany" ,
            "count": 1000000,
            "square": 12.5,
            "coords": { "lat": 1.0, "lon": 2.0 }
          }
        },
        {"id": 2, "vector": [0.19, 0.81, 0.75, 0.11], "payload": {"city": ["Berlin", "London"]}},
        {"id": 3, "vector": [0.36, 0.55, 0.47, 0.94], "payload": {"city": ["Berlin", "Moscow"]}},
        {"id": 4, "vector": [0.18, 0.01, 0.85, 0.80], "payload": {"city": ["London", "Moscow"]}},
        {"id": "98a9a4b1-4ef2-46fb-8315-a97d874fe1d7", "vector": [0.24, 0.18, 0.22, 0.44], "payload": {"count": [0]}},
        {"id": "f0e09527-b096-42a8-94e9-ea94d342b925", "vector": [0.35, 0.08, 0.11, 0.44]}
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

SAVED_POINTS_COUNT=$(curl --fail -s "http://$QDRANT_HOST/collections/test_collection" | jq '.result.points_count')
[[ "$SAVED_POINTS_COUNT" == "6" ]] || {
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

# search points batch
curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/search/batch" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw '{
    "searches": [
      {
        "vector": [0.2,0.1,0.9,0.7],
        "top": 3
      },
      {
        "vector": [0.2,0.1,0.9,0.7],
        "top": 3
      }
    ]
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
                      "value": "London"
                  }
              }
          ]
      },
      "vector": [0.2, 0.1, 0.9, 0.7],
      "top": 3
  }' | jq
