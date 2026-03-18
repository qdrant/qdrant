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

"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_collection",
  "query": {
    "nearest": {
      "dense": {
        "data": [0.2,0.1,0.9,0.7]
      }
    }
  },
  "limit": 3
}' $QDRANT_HOST qdrant.Points/Query

"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_collection",
  "query_points": [
    {
      "collection_name": "test_collection",
      "query": {
        "nearest": {
          "dense": {
            "data": [0.2,0.1,0.9,0.7]
          }
        }
      },
      "limit": 3
    }
  ]
}' $QDRANT_HOST qdrant.Points/QueryBatch

"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_collection",
  "query": {
    "nearest": {
      "dense": {
        "data": [0.2,0.1,0.9,0.7]
      }
    }
  },
  "limit": 3,
  "group_size": 2,
  "group_by": "city"
}' $QDRANT_HOST qdrant.Points/QueryGroups


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
  "query": {
    "nearest": {
      "dense": {
        "data": [0.2,0.1,0.9,0.7]
      }
    }
  },
  "limit": 3
}' $QDRANT_HOST qdrant.Points/Query

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
}' $QDRANT_HOST qdrant.Points/Query

"${docker_grpcurl[@]}" -d '{
  "collection_name": "test_collection",
  "query": {
    "recommend": {
      "positive": [
        { "id": { "num": 1 } }
      ],
      "negative": [
        { "id": {"num": 2 } }
      ]
    }
  }
}' $QDRANT_HOST qdrant.Points/Query
