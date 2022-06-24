#!/bin/bash

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

# SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROTO_DIR=./lib/api/src/grpc/proto
OUT_DIR=./golang/qdrant
PACKAGE_NAME=github.com/qdrant/go-client

protoc \
    --proto_path=$PROTO_DIR/ \
    --go_out=$OUT_DIR \
    --go-grpc_out=$OUT_DIR \
    --go_opt=paths=source_relative \
    --go-grpc_opt=paths=source_relative \
    \
    --go_opt=Mcollections_internal_service.proto=$PACKAGE_NAME \
    --go_opt=Mcollections_service.proto=$PACKAGE_NAME \
    --go_opt=Mcollections.proto=$PACKAGE_NAME \
    --go_opt=Mjson_with_int.proto=$PACKAGE_NAME \
    --go_opt=Mpoints_internal_service.proto=$PACKAGE_NAME \
    --go_opt=Mpoints_service.proto=$PACKAGE_NAME \
    --go_opt=Mpoints.proto=$PACKAGE_NAME \
    --go_opt=Mqdrant.proto=$PACKAGE_NAME \
    --go_opt=Mraft_service.proto=$PACKAGE_NAME \
    \
    --go-grpc_opt=Mcollections_internal_service.proto=$PACKAGE_NAME \
    --go-grpc_opt=Mcollections_service.proto=$PACKAGE_NAME \
    --go-grpc_opt=Mcollections.proto=$PACKAGE_NAME \
    --go-grpc_opt=Mjson_with_int.proto=$PACKAGE_NAME \
    --go-grpc_opt=Mpoints_internal_service.proto=$PACKAGE_NAME \
    --go-grpc_opt=Mpoints_service.proto=$PACKAGE_NAME \
    --go-grpc_opt=Mpoints.proto=$PACKAGE_NAME \
    --go-grpc_opt=Mqdrant.proto=$PACKAGE_NAME \
    --go-grpc_opt=Mraft_service.proto=$PACKAGE_NAME \
    \
    $PROTO_DIR/collections_internal_service.proto \
    $PROTO_DIR/collections_service.proto \
    $PROTO_DIR/collections.proto \
    $PROTO_DIR/json_with_int.proto \
    $PROTO_DIR/points_internal_service.proto \
    $PROTO_DIR/points_service.proto \
    $PROTO_DIR/points.proto \
    $PROTO_DIR/qdrant.proto \
    $PROTO_DIR/raft_service.proto
