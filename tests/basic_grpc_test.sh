#!/bin/bash
# This test checks that Qdrant answers to all API mentioned in README.md as expected

set -ex

QDRANT_HOST='localhost:6334'

docker_grpcurl="docker run --rm -it --network=host -v ${PWD}/src/tonic/proto:/proto fullstorydev/grpcurl -plaintext -import-path /proto -proto qdrant.proto"

$docker_grpcurl -d '{
   "name": "test_collection"
}' $QDRANT_HOST qdrant.Collections/Delete


$docker_grpcurl -d '{
   "name": "test_collection",
   "vector_size": 4,
   "distance": "Dot"
}' $QDRANT_HOST qdrant.Collections/Create

$docker_grpcurl -d '{}' $QDRANT_HOST qdrant.Collections/List

$docker_grpcurl -d '{ "name": "test_collection" }' $QDRANT_HOST qdrant.Collections/Get

$docker_grpcurl -d '{
  "collection": "test_collection",
  "wait": true,
  "points": [
    {
      "id": 1,
      "vector": [0.05, 0.61, 0.76, 0.74],
      "payload": {
        "city": { "keyword": { "value": ["Berlin"] }},
        "country": { "keyword": { "value": ["Germany"] }},
        "population": { "integer": { "value": [1000000] }},
        "square": { "float": { "value": [12.5] }},
        "coords": { "geo": { "value": [{ "lat": 1.0, "lon": 2.0 }]}}
      }
    },
    {"id": 2, "vector": [0.18, 0.01, 0.85, 0.80], "payload": {"square": {"float": { "value": [10, 11]}}}},
    {"id": 3, "vector": [0.24, 0.18, 0.22, 0.45], "payload": {"count": {"integer": {"value": [0]}}}},
    {"id": 4, "vector": [0.24, 0.18, 0.22, 0.45], "payload": {"coords": {"geo": {"value": [{ "lat": 1.0, "lon": 2.0}, { "lat": 3.0, "lon": 4.0}]}}}},
    {"id": 5, "vector": [0.35, 0.08, 0.11, 0.44]}
  ]
}' $QDRANT_HOST qdrant.Points/Upsert


#SAVED_VECTORS_COUNT=$(curl --fail -s "http://$QDRANT_HOST/collections/test_collection" | jq '.result.vectors_count')
#[[ "$SAVED_VECTORS_COUNT" == "6" ]] || {
#  echo 'check failed'
#  exit 1
#}
#
#curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/search" \
#  -H 'Content-Type: application/json' \
#  --fail -s \
#  --data-raw '{
#        "vector": [0.2,0.1,0.9,0.7],
#        "top": 3
#    }' | jq
#
#curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/search" \
#  --fail -s \
#  -H 'Content-Type: application/json' \
#  --data-raw '{
#      "filter": {
#          "should": [
#              {
#                  "key": "city",
#                  "match": {
#                      "keyword": "London"
#                  }
#              }
#          ]
#      },
#      "vector": [0.2, 0.1, 0.9, 0.7],
#      "top": 3
#  }' | jq
#
#
#curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/scroll" \
#  --fail -s \
#  -H 'Content-Type: application/json' \
#  --data-raw '{ "offset": 2, "limit": 2, "with_vector": true }' | jq
#
#curl -L -X POST "http://$QDRANT_HOST/collections" \
#  --fail -s \
#  -H 'Content-Type: application/json' \
#  --data-raw '{
#      "change_aliases": {
#          "actions": [
#              {
#                  "create_alias": {
#                      "alias_name": "test_alias",
#                      "collection_name": "test_collection"
#                  }
#              }
#          ]
#      }
#  }' | jq
#
#curl -L -X POST "http://$QDRANT_HOST/collections/test_alias/points/search" \
#  -H 'Content-Type: application/json' \
#  --fail -s \
#  --data-raw '{
#        "vector": [0.2,0.1,0.9,0.7],
#        "top": 3
#    }' | jq