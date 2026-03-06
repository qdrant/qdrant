#!/usr/bin/env bash
# This test checks that Qdrant answers to all API mentioned in README.md as expected

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

QDRANT_HOST=${QDRANT_HOST:-'127.0.0.1:6334'}
GRPCURL_PATH="/opt/homebrew/bin/grpcurl"

# Check if local grpcurl exists and is executable
if [ -x "$GRPCURL_PATH" ]; then
    echo "Using local grpcurl installation"
    grpcurl_base=(
        "$GRPCURL_PATH"
        "-plaintext"
        "-import-path"
        "./lib/api/src/grpc/proto"
        "-proto"
        "./lib/api/src/grpc/proto/qdrant.proto"
    )
else
    echo "Local grpcurl not found, using Docker container"
    # Define base grpcurl command for Docker in case local grpcurl is not found
    grpcurl_base=(
        "docker" "run" "--rm" "--network=host"
        "-v" "${PWD}/lib/api/src/grpc/proto:/proto"
        "fullstorydev/grpcurl"
        "-plaintext"
        "-import-path" "/proto"
        "-proto" "qdrant.proto"
    )
fi

# Add headers if they exist
if [ -n "${QDRANT_HOST_HEADERS}" ]; then
    while read h; do
        grpcurl_base+=("-H" "$h")
    done <<< $(echo "${QDRANT_HOST_HEADERS}" | jq -r 'to_entries|map("\(.key): \(.value)")[]')
fi

# Function to execute grpcurl commands
execute_grpcurl() {
    "${grpcurl_base[@]}" "$@"
}

# Upsert first point
execute_grpcurl -d '{
"collection_name": "sparse_charts",
"wait": true,
"ordering": null,
"points": [
 {
   "id": { "num": 1 },
   "vectors": {
     "vectors": {
       "vectors": {
         "keywords": {
           "document": {
             "text": "my text",
             "model": "Qdrant/bm25"
           }
         }
       }
     }
   },
   "payload": {
     "city": { "string_value": "Berlin" }
   }
 }
]
}' $QDRANT_HOST qdrant.Points/Upsert

# Upsert multiple points
execute_grpcurl -d '{
"collection_name": "sparse_charts",
"wait": true,
"ordering": null,
"points": [
 {
   "id": { "num": 1 },
   "vectors": {
     "vectors": {
       "vectors": {
         "keywords": {
           "document": {
             "text": "my text",
             "model": "Qdrant/bm25"
           }
         }
       }
     }
   },
   "payload": {
     "city": { "string_value": "Berlin" }
   }
 },
 {
   "id": { "num": 2 },
   "vectors": {
     "vectors": {
       "vectors": {
         "keywords": {
           "document": {
             "text": "my text another",
             "model": "Qdrant/bm25"
           }
         }
       }
     }
   },
   "payload": {
     "city": { "string_value": "Amsterdam" }
   }
 }
]
}' $QDRANT_HOST qdrant.Points/Upsert
