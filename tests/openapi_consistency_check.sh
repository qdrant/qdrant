#!/bin/bash

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

# Keep current version of files to check
cp ./openapi/openapi-merged.yaml ./openapi/.repo.openapi-merged.yaml
cp ./openapi/openapi-merged.json ./openapi/.repo.openapi-merged.json
cp ./docs/redoc/master/openapi.json ./docs/redoc/master/.repo.openapi.json

# Regenerate OpenAPI files
tools/generate_openapi_models.sh

# Ensure generated files are the same as files in this repository
if diff -Zwa ./openapi/openapi-merged.yaml ./openapi/.repo.openapi-merged.yaml \
&& diff -Zwa ./openapi/openapi-merged.json ./openapi/.repo.openapi-merged.json \
&& diff -Zwa ./docs/redoc/master/openapi.json ./docs/redoc/master/.repo.openapi.json
then
    set +x
    echo "No diffs found."
else
    set +x
    echo "ERROR: Generated OpenAPI files are not consistent with files in this repository, see diff above."
    echo "ERROR: See: https://github.com/qdrant/qdrant/blob/master/docs/DEVELOPMENT.md#rest"
    exit 1
fi

# Cleanup
rm -f ./openapi/.repo.openapi-merged.{json,yaml} ./docs/redoc/master/.repo.openapi.json
