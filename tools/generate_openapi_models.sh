#!/bin/bash
# This script generate model definitions for OpenAPI 3.0 documentation

set -e

# Ensure current path is project root
cd "$(dirname "$0")/../"

cargo run --package qdrant --bin schema_generator

(
    cd tools/schema2openapi/
    docker build . --tag schema2openapi
)

docker run --rm \
    -v $(pwd)/openapi/schemas/AllDefinitions.json:/app/schema.json \
    schema2openapi | sed -e 's/"type": "null"/"nullable": true/g' >./openapi/models.json

# (cd tools/openapi-merge/ ; docker build . --tag merge-openapi)

echo "WARNING: This file is auto-generated. Do NOT edit it manually!" >./openapi/openapi-merged.yaml

docker run \
    -v $(pwd)/openapi:/project \
    wework/speccy resolve /project/openapi.yaml >>./openapi/openapi-merged.yaml
