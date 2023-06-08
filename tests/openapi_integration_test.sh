#!/usr/bin/env bash

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

cp docs/redoc/master/openapi.json openapi/tests/openapi.json

docker run --rm \
            -e QDRANT_HOST=http://host.docker.internal:6333 \
            --add-host host.docker.internal:host-gateway \
            -e OPENAPI_FILE='openapi.json' \
            -v "${PWD}"/openapi/tests:/code \
            "$(docker buildx build --load -q ./openapi/tests)" sh -c /code/run_docker.sh
