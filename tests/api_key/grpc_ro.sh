#!/bin/bash
# This test checks that Read-only API keys only allow Read operations

set -e
set -u
set -o pipefail
# set -x # Uncomment to debug a failing test

# Ensure current path is project root
cd "$(dirname "$0")/../../"

QDRANT_HOST='localhost:6334'

docker_grpcurl="docker run --rm --network=host -v ${PWD}/lib/api/src/grpc/proto:/proto fullstorydev/grpcurl -plaintext -import-path /proto -proto qdrant.proto"

(($docker_grpcurl -H 'api-key: my-ro-secret' -d '{
   "collection_name": "test_collection"
}' $QDRANT_HOST qdrant.Collections/Delete &> /dev/null
) && echo "Failed") || echo "Succeeded"

(($docker_grpcurl -H 'api-key: my-ro-secret' -d '{
   "collection_name": "test_collection",
   "vectors_config": {
      "params": {
        "size": 4,
        "distance": "Dot"
      }
   }
}' $QDRANT_HOST qdrant.Collections/Create &> /dev/null
) && echo "Failed") || echo "Succeeded"

(($docker_grpcurl -H 'api-key: my-ro-secret' -d '{}' $QDRANT_HOST qdrant.Collections/List &> /dev/null
) && echo "Succeeded") || echo "Failed"

(($docker_grpcurl -H 'api-key: my-ro-secret' -d '{
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
}' $QDRANT_HOST qdrant.Points/Upsert &> /dev/null
) && echo "Failed") || echo "Succeeded"

(($docker_grpcurl -H 'api-key: my-ro-secret' -d '{ "collection_name": "test_collection" }' $QDRANT_HOST qdrant.Collections/Get &> /dev/null
) && echo "Succeeded") || echo "Failed"

(($docker_grpcurl -H 'api-key: my-ro-secret' -d '{
  "collection_name": "test_collection",
  "vector": [0.2,0.1,0.9,0.7],
  "limit": 3
}' $QDRANT_HOST qdrant.Points/Search &> /dev/null
) && echo "Succeeded") || echo "Failed"

(($docker_grpcurl -H 'api-key: my-ro-secret' -d '{
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
}' $QDRANT_HOST qdrant.Points/Search &> /dev/null
) && echo "Succeeded") || echo "Failed"

(($docker_grpcurl -H 'api-key: my-ro-secret' -d '{
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
}' $QDRANT_HOST qdrant.Points/Scroll &> /dev/null
) && echo "Succeeded") || echo "Failed"

(($docker_grpcurl -H 'api-key: my-ro-secret' -d '{
  "collection_name": "test_collection",
  "with_vectors": {"enable": true},
  "ids": [{ "num": 2 }, { "num": 3 }, { "num": 4 }]
}' $QDRANT_HOST qdrant.Points/Get &> /dev/null
) && echo "Succeeded") || echo "Failed"

(($docker_grpcurl -H 'api-key: my-ro-secret' -d '{
  "collection_name": "test_collection",
  "positive": [{ "num": 1 }],
  "negative": [{ "num": 2 }]
}' $QDRANT_HOST qdrant.Points/Recommend &> /dev/null
) && echo "Succeeded") || echo "Failed"

# create alias
(($docker_grpcurl -H 'api-key: my-ro-secret' -d '{
  "actions": [
    {
      "create_alias": {
        "alias_name": "test_alias",
        "collection_name": "test_collection"
      }
    }
  ]
}' $QDRANT_HOST qdrant.Collections/UpdateAliases &> /dev/null
) && echo "Failed") || echo "Succeeded"

(($docker_grpcurl -H 'api-key: my-ro-secret' -d '{
  "collection_name": "test_collection",
  "with_vectors": {"enable": false},
  "with_payload": {
    "include": {"fields": ["population"]}
  },
  "ids": [{ "num": 1 }]
}' $QDRANT_HOST qdrant.Points/Get &> /dev/null
) && echo "Succeeded") || echo "Failed"
