#!/usr/bin/env bash
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
      "vectors": {
        "size": 4,
        "distance": "Dot"
      },
      "optimizers_config": {
        "default_segment_number": 2
      }
    }' | jq


for i in {1..100}
do
  IDX=$i

  IDX2=$(expr $IDX + 1000)

  PAYLOAD=$( jq -n \
     --argjson point_id "$IDX" \
     --argjson point_id_2 "$IDX2" \
     '{ "points": [
        {"id": $point_id, "vector": [0.19, 0.81, 0.75, 0.11], "payload": {"city":  "London" }},
        {"id": $point_id_2, "vector": [0.05, 0.61, 0.76, 0.74], "payload": {"city":  "Berlin" }}
      ]}')

  # insert points
  curl -L -X PUT "http://$QDRANT_HOST/collections/test_collection/points?wait=true" \
    -H 'Content-Type: application/json' \
    --fail -s \
    --data-raw "$PAYLOAD" | jq
done

PAYLOAD='
  {
    "filter": {
      "should": [
        {
          "key": "city",
          "match": {
              "value": "London"
          }
        }
      ]
    }
  }
';

# insert points
curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/delete?wait=true" \
  -H 'Content-Type: application/json' \
  --fail -s \
  --data-raw "$PAYLOAD" | jq


for i in {1..10}
do
  IDX=$i

  IDX2=$(expr $IDX + 1000)

  PAYLOAD=$( jq -n \
     --argjson point_id "$IDX" \
     --argjson point_id_2 "$IDX2" \
     '{ "points": [
        {"id": $point_id, "vector": [0.19, 0.81, 0.75, 0.11], "payload": {"city":  "London" }},
        {"id": $point_id_2, "vector": [0.05, 0.61, 0.76, 0.74], "payload": {"city":  "Berlin" }}
      ]}')

  # insert points
  curl -L -X PUT "http://$QDRANT_HOST/collections/test_collection/points?wait=true" \
    -H 'Content-Type: application/json' \
    --fail -s \
    --data-raw "$PAYLOAD" | jq
done
