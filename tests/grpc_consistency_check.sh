#!/bin/bash

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

# Keep current version of file to check
cp ./lib/api/src/grpc/{,.diff.}qdrant.rs
cp ./docs/grpc/{,.diff.}docs.md

# Regenerate gRPC files
touch ./lib/api/src/grpc/proto/.build-trigger.proto
cargo build --package api

# Regenerate gRPC docs
./tools/generate_grpc_docs.sh

# Ensure generated files are the same as files in this repository
if diff -Zwa ./lib/api/src/grpc/{,.diff.}qdrant.rs \
&& diff -Zwa ./docs/grpc/{,.diff.}docs.md
then
    set +x
    echo "No diff found."
else
    set +x
    echo "ERROR: Generated gRPC file is not consistent with files in this repository, see diff above."
    echo "ERROR: See: https://github.com/qdrant/qdrant/blob/master/docs/DEVELOPMENT.md#grpc"
    exit 1
fi

# Cleanup
rm -f ./lib/api/src/grpc/{.diff.qdrant.rs,proto/.build-trigger.proto} ./docs/grpc/.diff.docs.md
