#!/bin/bash
# This script generate model definitions for OpenAPI 3.0 documentation

set -e

# Ensure current path is project root
cd "$(dirname "$0")/../"

cargo run --package qdrant --bin schema_generator > ./openapi/schemas/AllDefinitions.json

(
    cd tools/schema2openapi/
    docker build . --tag schema2openapi
)

docker run --rm \
    -v "$(pwd)/openapi/schemas/AllDefinitions.json:/app/schema.json" \
    schema2openapi | sed -e 's%#/definitions/%#/components/schemas/%g' >./openapi/models.json


# Creating of a single merged openapi file. It might be useful for doc-generation tools.
cat ./openapi/openapi.yaml | sed -e 's%./models.json#%#%g' > ./openapi/openapi-merged.yaml

docker run --rm -i simplealpine/json2yaml <./openapi/models.json | tail -n +3 >> ./openapi/openapi-merged.yaml

docker run --rm -i simplealpine/yaml2json <./openapi/openapi-merged.yaml | jq > ./openapi/openapi-merged.json

cp ./openapi/openapi-merged.json ./docs/redoc/openapi.json
