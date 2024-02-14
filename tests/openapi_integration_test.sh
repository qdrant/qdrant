#!/usr/bin/env bash

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

cp docs/redoc/master/openapi.json tests/openapi/openapi.json

docker buildx build --load -q ./openapi/tests --tag=qdrant_openapi_test

DOCKER_ARGS=$([ -t 0 ] && echo "-it" || echo "")
docker run $DOCKER_ARGS \
    --rm \
    --network=host \
    -e OPENAPI_FILE='openapi.json' \
    -v "${PWD}"/openapi/tests:/code \
    qdrant_openapi_test sh -c /code/run_docker.sh
