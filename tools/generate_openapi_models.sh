#!/usr/bin/env bash
# This script generate model definitions for OpenAPI 3.0 documentation

set -e

# Ensure current path is project root
cd "$(dirname "$0")/../"

# Fallback to dockerized utilities if they are not installed locally

if command -v ytt &>/dev/null; then
    sh_with_ytt() { sh -c "$1"; }
else
    sh_with_ytt() { docker run --rm -v "${PWD}":/workspace --entrypoint sh gerritk/ytt -c "$1"; }
fi

if ! yq --version 2>&1 | grep -q mikefarah; then
    yq() { docker run --rm -v "${PWD}":/workdir mikefarah/yq "$@"; }
fi

# Apply `ytt` template engine to obtain OpenAPI definitions for REST endpoints

sh_with_ytt '
    set -e
    ytt -f ./openapi/openapi.lib.yml -f ./openapi/openapi-shards.ytt.yaml > ./openapi/openapi-shards.yaml
    ytt -f ./openapi/openapi.lib.yml -f ./openapi/openapi-service.ytt.yaml > ./openapi/openapi-service.yaml
    ytt -f ./openapi/openapi.lib.yml -f ./openapi/openapi-cluster.ytt.yaml > ./openapi/openapi-cluster.yaml
    ytt -f ./openapi/openapi.lib.yml -f ./openapi/openapi-collections.ytt.yaml > ./openapi/openapi-collections.yaml
    ytt -f ./openapi/openapi.lib.yml -f ./openapi/openapi-snapshots.ytt.yaml > ./openapi/openapi-snapshots.yaml
    ytt -f ./openapi/openapi.lib.yml -f ./openapi/openapi-shard-snapshots.ytt.yaml > ./openapi/openapi-shard-snapshots.yaml
    ytt -f ./openapi/openapi.lib.yml -f ./openapi/openapi-points.ytt.yaml > ./openapi/openapi-points.yaml
    ytt -f ./openapi/openapi.lib.yml -f ./openapi/openapi-main.ytt.yaml > ./openapi/openapi-main.yaml
'

# Generates models from internal service structures
cargo run --package qdrant --features="service_debug" --bin schema_generator > ./openapi/schemas/AllDefinitions.json

docker build tools/schema2openapi --tag schema2openapi

docker run --rm \
    -v "$PWD/openapi/schemas/AllDefinitions.json:/app/schema.json" \
    schema2openapi | sed -e 's%#/definitions/%#/components/schemas/%g' >./openapi/models.json

yq eval -o yaml -P ./openapi/models.json > ./openapi/models.yaml

# Merge all *.yaml files together into a single-file OpenAPI definition
yq eval-all '. as $item ireduce ({}; . *+ $item)' \
  ./openapi/openapi-shards.yaml \
  ./openapi/openapi-service.yaml \
  ./openapi/openapi-cluster.yaml \
  ./openapi/openapi-collections.yaml \
  ./openapi/openapi-snapshots.yaml \
  ./openapi/openapi-shard-snapshots.yaml \
  ./openapi/openapi-points.yaml \
  ./openapi/openapi-main.yaml \
  ./openapi/models.yaml > ./openapi/openapi-merged.yaml

docker run --rm -v "${PWD}"/openapi:/spec redocly/openapi-cli:v1.0.0-beta.88 lint openapi-merged.yaml

yq eval -o=json ./openapi/openapi-merged.yaml | jq > ./openapi/openapi-merged.json

cp ./openapi/openapi-merged.json ./docs/redoc/master/openapi.json
