#!/usr/bin/env bash
# Some gRPC files in this repository are generated and based upon other
# sources. When these sources change, the generated files must be generated
# (and committed) again. It is the task of the contributing user to do this
# properly.
#
# This tests makes sure the generated gRPC files are consistent with its
# sources. If this fails, you probably have to generate the gRPC files again.
#
# Read more here: https://github.com/qdrant/qdrant/blob/master/docs/DEVELOPMENT.md#grpc

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

# Generated files to check, and the proto that triggers their regeneration
GENERATED=(
    ./lib/api/src/grpc/qdrant.rs
    ./lib/uio-grpc-client/src/generated/qdrant.rs
    ./lib/uio-grpc-client/proto/storage_read_service.proto
)
TRIGGERS=(
    ./lib/api/src/grpc/proto/.build-trigger.proto
    ./lib/uio-grpc-client/proto/storage_read_service.proto
)

# Keep current version of files to check
for file in "${GENERATED[@]}"; do
    cp "$file" "$(dirname "$file")/.diff.$(basename "$file")"
done

# Regenerate gRPC files
touch "${TRIGGERS[@]}"
cargo build --package api --package uio-grpc-client

# Ensure generated files are the same as files in this repository
for file in "${GENERATED[@]}"; do
    if diff -Zwa "$file" "$(dirname "$file")/.diff.$(basename "$file")"
    then
        echo "No diff found in $file."
    else
        set +x
        echo "ERROR: Generated gRPC file $file is not consistent with files in this repository, see diff above."
        echo "ERROR: See: https://github.com/qdrant/qdrant/blob/master/docs/DEVELOPMENT.md#grpc"
        exit 1
    fi
done

# Cleanup
for file in "${GENERATED[@]}"; do
    rm -f "$(dirname "$file")/.diff.$(basename "$file")"
done
rm -f ./lib/api/src/grpc/proto/.build-trigger.proto
