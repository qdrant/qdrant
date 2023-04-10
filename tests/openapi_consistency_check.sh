#!/bin/bash
# Some OpenAPI files in this repository are generated and based upon other
# sources. When these sources change, the generated files must be generated
# (and committed) again. It is the task of the contributing user to do this
# properly.
#
# This tests makes sure the generated OpenAPI files are consistent with its
# sources. If this fails, you probably have to generate the OpenAPI files again.
#
# Read more here: https://github.com/qdrant/qdrant/blob/master/docs/DEVELOPMENT.md#rest

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
