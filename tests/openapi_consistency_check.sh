#!/bin/bash

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

# Keep current version of file to check
cp ./docs/redoc/master/{,.diff.}openapi.json

# Regenerate OpenAPI files
tools/generate_openapi_models.sh

# Ensure generated files are the same as files in this repository
if diff -Zwa ./docs/redoc/master/{,.diff.}openapi.json
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
rm -f ./docs/redoc/master/.diff.openapi.json
