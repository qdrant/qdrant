#!/usr/bin/env bash
# This test checks that Qdrant answers to all API mentioned in README.md as expected

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

QDRANT_HOST=${QDRANT_HOST:-'localhost:6334'}

docker_grpcurl=("docker" "run" "--rm" "--network=host" "-v" "${PWD}/lib/api/src/grpc/proto:/proto" "fullstorydev/grpcurl" "-plaintext" "-import-path" "/proto" "-proto" "qdrant.proto")

if [ -n "${QDRANT_HOST_HEADERS}" ]; then
  while read h; do
    docker_grpcurl+=("-H" "$h")
  done <<<  $(echo "${QDRANT_HOST_HEADERS}" | jq -r 'to_entries|map("\(.key): \(.value)")[]')
fi

"${docker_grpcurl[@]}" -d '{
   "collection_name": "test_collection"
}' $QDRANT_HOST qdrant.Collections/Delete

"${docker_grpcurl[@]}" -d '{
   "collection_name": "test_collection",
   "vectors_config": {
      "params": {
        "size": 4,
        "distance": "Dot"
      }
   }
}' $QDRANT_HOST qdrant.Collections/Create

"${docker_grpcurl[@]}" -d '{}' $QDRANT_HOST qdrant.Collections/List

"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_collection",
  "wait": true,
  "ordering": null,
  "points": [
    {
      "id": { "num": 1 },
      "vectors": {"vector": {"data": [0.05, 0.61, 0.76, 0.74] }},
      "payload": {
        "city": { "string_value": "Berlin" },
        "country":  { "string_value": "Germany" },
        "population": { "integer_value":  1000000 },
        "square": { "double_value": 12.5 },
        "coords": { "struct_value": { "fields": { "lat": { "double_value": 1.0 }, "lon": { "double_value": 2.0 } } } }
      }
    },
    {"id": { "num": 2 }, "vectors": {"vector": {"data": [0.19, 0.81, 0.75, 0.11]}}, "payload": {"city": {"list_value": {"values": [{ "string_value": "Berlin" }, { "string_value": "London" }]}}}},
    {"id": { "num": 3 }, "vectors": {"vector": {"data": [0.36, 0.55, 0.47, 0.94]}}, "payload": {"city": {"list_value": {"values": [{ "string_value": "Berlin" }, { "string_value": "Moscow" }]}}}},
    {"id": { "num": 4 }, "vectors": {"vector": {"data": [0.18, 0.01, 0.85, 0.80]}}, "payload": {"city": {"list_value": {"values": [{ "string_value": "London" }, { "string_value": "Moscow" }]}}}},
    {"id": { "uuid": "98a9a4b1-4ef2-46fb-8315-a97d874fe1d7" }, "vectors": {"vector": {"data": [0.24, 0.18, 0.22, 0.44]}}, "payload": {"count":{"list_value": {"values": [{ "integer_value": 0 }]}}}},
    {"id": { "uuid": "f0e09527-b096-42a8-94e9-ea94d342b925" }, "vectors": {"vector": {"data": [0.35, 0.08, 0.11, 0.44]}}}
  ]
}' $QDRANT_HOST qdrant.Points/Upsert

# Create payload index
"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_collection",
  "field_name": "city",
  "field_type": 0,
  "field_index_params": { "keyword_index_params": {} },
  "wait": true
}' $QDRANT_HOST qdrant.Points/CreateFieldIndex

"${docker_grpcurl[@]}" -d '{ "collection_name": "test_collection" }' $QDRANT_HOST qdrant.Collections/Get

"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_collection",
  "vector": [0.2,0.1,0.9,0.7],
  "limit": 3
}' $QDRANT_HOST qdrant.Points/Search

"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_collection",
  "filter": {
    "should": [
      {
        "field": {
          "key": "city",
          "match": {
            "keyword": "London"
          }
        }
      }
    ]
  },
  "vector": [0.2,0.1,0.9,0.7],
  "limit": 3
}' $QDRANT_HOST qdrant.Points/Search

"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_collection",
  "limit": 2,
  "with_vectors": {"enable": true},
  "filter": {
    "should": [
      {
        "field": {
          "key": "city",
          "match": {
            "keyword": "London"
          }
        }
      }
    ]
  }
}' $QDRANT_HOST qdrant.Points/Scroll

"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_collection",
  "with_vectors": {"enable": true},
  "ids": [{ "num": 2 }, { "num": 3 }, { "num": 4 }]
}' $QDRANT_HOST qdrant.Points/Get

"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_collection",
  "positive": [{ "num": 1 }],
  "negative": [{ "num": 2 }]
}' $QDRANT_HOST qdrant.Points/Recommend

# city facet
"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_collection",
  "key": "city"
}' $QDRANT_HOST qdrant.Points/Facet

# create alias
"${docker_grpcurl[@]}" -d '{
  "actions": [
    {
      "create_alias": {
        "alias_name": "test_alias",
        "collection_name": "test_collection"
      }
    }
  ]
}' $QDRANT_HOST qdrant.Collections/UpdateAliases

# search via alias
"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_alias",
  "vector": [0.2,0.1,0.9,0.7],
  "limit": 3
}' $QDRANT_HOST qdrant.Points/Search

# rename alias
"${docker_grpcurl[@]}" -d '{
  "actions": [
    {
      "rename_alias": {
        "old_alias_name": "test_alias",
        "new_alias_name": "new_test_alias"
      }
    }
  ]
}' $QDRANT_HOST qdrant.Collections/UpdateAliases

# search via renamed alias
"${docker_grpcurl[@]}" -d '{
  "collection_name": "new_test_alias",
  "vector": [0.2,0.1,0.9,0.7],
  "limit": 3
}' $QDRANT_HOST qdrant.Points/Search

# delete alias
"${docker_grpcurl[@]}" -d '{
  "actions": [
    {
      "delete_alias": {
        "alias_name": "new_test_alias"
      }
    }
  ]
}' $QDRANT_HOST qdrant.Collections/UpdateAliases

# create bool index
"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_collection",
  "field_name": "bool_field",
  "field_type": 5,
  "field_index_params": { "bool_index_params": {} }
}' $QDRANT_HOST qdrant.Points/CreateFieldIndex

"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_collection",
  "with_vectors": {"enable": false},
  "with_payload": {
    "include": {"fields": ["population"]}
  },
  "ids": [{ "num": 1 }]
}' $QDRANT_HOST qdrant.Points/Get

# The following must return a validation error
set +e
response=$(
    "${docker_grpcurl[@]}" -d '{
        "collection_name": "test_collection",
        "recommend_points": [
            {
                "positive": [{ "num": 1 }]
            },
            {
                "positive": [{ "num": 1 }]
            }
        ]
    }' $QDRANT_HOST qdrant.Points/RecommendBatch 2>&1
)
if [[ $response != *"Validation error in body"* ]]; then
    echo Unexpected response, expected validation error: $response
    exit 1
fi
set -e

# use the reflection service to inspect the full API
"${docker_grpcurl[@]}" $QDRANT_HOST describe

# use the reflection service to inspect each advertised service
"${docker_grpcurl[@]}" $QDRANT_HOST describe qdrant.Collections
"${docker_grpcurl[@]}" $QDRANT_HOST describe qdrant.Points
"${docker_grpcurl[@]}" $QDRANT_HOST describe qdrant.Snapshots
"${docker_grpcurl[@]}" $QDRANT_HOST describe qdrant.Qdrant
"${docker_grpcurl[@]}" $QDRANT_HOST describe grpc.health.v1.Health

# use the reflection service to get the shape of a specific message
"${docker_grpcurl[@]}" $QDRANT_HOST describe qdrant.UpsertPoints

# grpc protocol compliant health check
"${docker_grpcurl[@]}" $QDRANT_HOST grpc.health.v1.Health/Check

#SAVED_POINTS_COUNT=$(curl --fail -s "http://$QDRANT_HOST/collections/test_collection" | jq '.result.points_count')
#[[ "$SAVED_POINTS_COUNT" == "6" ]] || {
#  echo 'check failed'
#  exit 1
#}
#
#curl -L -X POST "http://$QDRANT_HOST/collections/test_collection/points/search" \
#  -H 'Content-Type: application/json' \
#  --fail -s \
#  --data-raw '{
#        "vector": [0.2,0.1,0.9,0.7],
#        "limit": 3
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
#      "limit": 3
#  }' | jq
