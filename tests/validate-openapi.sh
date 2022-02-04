#!/bin/bash
# This builds a rust client from the openapi spec

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

# Generate spec
./tools/generate_openapi_models.sh

# Build rust client
docker run --rm -v "${PWD}"/openapi:/local openapitools/openapi-generator-cli generate \
      -i /local/openapi-merged.yaml \
      -g rust \
      -o /local/rust-client
