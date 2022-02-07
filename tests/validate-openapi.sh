#!/bin/bash
# This builds a rust client from the openapi spec

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

# Generate spec
./tools/generate_openapi_models.sh

# Build rust client
docker run --user $(id -u):$(id -g) --rm \
      -v "${PWD}"/openapi:/local openapitools/openapi-generator-cli generate \
      -i /local/openapi-merged.yaml \
      -g rust \
      -o /local/rust-client

cd openapi/rust-client

# master is not a valid version
sed -i 's/master/0.1.0/g' Cargo.toml

# should not belong to current workspace
printf "[workspace]" >> Cargo.toml

cargo build