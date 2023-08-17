#!/bin/bash

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

QDRANT_HOST='localhost:6335'

docker_grpcurl="docker run --rm --network=host -v ${PWD}/lib/api/src/grpc/proto:/proto fullstorydev/grpcurl -plaintext -import-path /proto -proto qdrant.proto"

$docker_grpcurl $QDRANT_HOST qdrant.QdrantInternal/GetHttpPort
