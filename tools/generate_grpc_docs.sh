#!/usr/bin/env bash

set -e

# Ensure current path is project root
cd "$(dirname "$0")/../"

# Create a temporary directory and store its name in a variable.
TEMPD=$(mktemp -d -t qdrant_docs.XXXXXXXXXX)
trap 'rm -rf -- "$TEMPD"' EXIT

cp -r "$PWD"/lib/api/src/grpc/proto/* "$TEMPD"

# Do not generate docs for internal services
rm "$TEMPD/collections_internal_service.proto"
rm "$TEMPD/points_internal_service.proto"
rm "$TEMPD/shard_snapshots_service.proto"
rm "$TEMPD/raft_service.proto"

cat "$TEMPD/qdrant.proto" \
  | grep -v 'collections_internal_service.proto' \
  | grep -v 'points_internal_service.proto' \
  | grep -v 'shard_snapshots_service.proto' \
  | grep -v 'raft_service.proto'\
   > "$TEMPD/qdrant.proto.tmp"
mv "$TEMPD/qdrant.proto.tmp" "$TEMPD/qdrant.proto"

docker run --rm \
  -u "$(id -u):$(id -g)" \
  -v "$PWD/docs/grpc":/out \
  -v "$TEMPD":/protos \
  pseudomuto/protoc-gen-doc --doc_opt=markdown,docs.md
