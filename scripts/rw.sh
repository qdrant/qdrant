#!/bin/bash

set -e

QDRANT_BIN=${QDRANT_BIN:-./target/debug/qdrant}

# Validate mode argument
if [[ $1 != "read" && $1 != "write" ]]; then
    echo "Usage: $0 [read|write] <node_id> [additional_args]"
    exit 1
fi

MODE=$1
shift

# Validate node ID
if [[ -z $1 || ! $1 =~ ^([0-9]+|rm)$ ]]; then
    echo "Please provide a valid node ID"
    exit 1
fi

ID=$1
shift

# Select base paths and IP based on mode
if [[ $MODE == "read" ]]; then
    BASE_IP=127.0.1
    STORAGE_BASE=/tmp/read/storage_
    SNAPSHOTS_BASE=/tmp/read/snapshots_
    BOOTSTRAP_HOST=$BASE_IP.1:6335  # Different bootstrap for read cluster
    PEER_ID=20
else  # write mode
    BASE_IP=127.0.0
    STORAGE_BASE=/tmp/write/storage_
    SNAPSHOTS_BASE=/tmp/write/snapshots_
    BOOTSTRAP_HOST=$BASE_IP.1:6335  # Different bootstrap for write cluster
    PEER_ID=10
fi

IP=$BASE_IP.$ID
PEER_ID="$PEER_ID$ID" # 101, 102, 103; 201, 202, 203

# Remove storage option
if [[ $ID == "rm" ]]; then
    rm -rf $STORAGE_BASE* $SNAPSHOTS_BASE*
    echo "All $MODE cluster storages removed!"
    exit 0
fi

# Export cluster configuration
export QDRANT__CLUSTER__ENABLED=true
export QDRANT__SERVICE__HOST=$IP
export QDRANT__STORAGE__STORAGE_PATH=$STORAGE_BASE$ID
export QDRANT__STORAGE__SNAPSHOTS_PATH=$SNAPSHOTS_BASE$ID
export QDRANT__LOG_LEVEL=debug
export QDRANT__STORAGE__HANDLE_COLLECTION_LOAD_ERRORS=false
export QDRANT__STORAGE__SHARD_TRANSFER_METHOD=stream_records
export QDRANT__CLUSTER__PEER_ID=$PEER_ID

set -x

# Start Qdrant node
if [[ $ID == 1 ]]; then
    $QDRANT_BIN --uri http://$IP:6335 "$@"
else
    $QDRANT_BIN --bootstrap http://$BOOTSTRAP_HOST --uri http://$IP:6335 "$@"
fi
