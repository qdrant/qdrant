#!/bin/bash

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

cp docs/redoc/master/openapi.json openapi/tests/openapi.json

WITH_DOCKER=${WITH_DOCKER:-"true"}

if [ "$WITH_DOCKER" == "true" ]; then
    docker run --rm \
      --network=host \
      -e OPENAPI_FILE='openapi.json' \
      -v "${PWD}"/openapi/tests:/code \
      "$(docker buildx build --load -q ./openapi/tests)" sh -c /code/run_docker.sh

else
  cd openapi/tests
  pip install -r requirements-freeze.txt
  bash -x run_docker.sh
fi
