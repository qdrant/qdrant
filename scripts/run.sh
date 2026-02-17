#!/bin/bash

set -e

QDRANT_BIN=${QDRANT_BIN:-./target/debug/qdrant}
STORAGE_BASE=/tmp/storage_
SNAPSHOTS_BASE=/tmp/snapshots_

if [[ $1 == "rm" ]]; then
    rm -rf $STORAGE_BASE* $SNAPSHOTS_BASE*
    echo "All storages removed!"
    exit 0
fi

ID=$1
IP=127.0.0.$ID
# IP=127.0.1.$ID # for rw segregation to work

shift

export QDRANT__CLUSTER__ENABLED=true
export QDRANT__NUM_CPU=2
export QDRANT__CLUSTER__PEER_ID="10$ID" # 101, 102, etc
export QDRANT__SERVICE__HOST=$IP
export QDRANT__STORAGE__STORAGE_PATH=$STORAGE_BASE$ID
export QDRANT__STORAGE__SNAPSHOTS_PATH=$SNAPSHOTS_BASE$ID
export QDRANT__LOG_LEVEL=debug # ,qdrant=trace
# export QDRANT__LOG_LEVEL=debug # ,raft=trace # ,qdrant=trace
# export QDRANT__LOG_LEVEL=debug,qdrant=trace
export QDRANT__STORAGE__HANDLE_COLLECTION_LOAD_ERRORS=false
export QDRANT__STORAGE__SHARD_TRANSFER_METHOD=stream_records

# TEMPORARY: TO REMOVE
export QDRANT__CLUSTER__CONSENSUS__COMPACT_WAL_ENTRIES=5

# export QDRANT__CLUSTER__CONSENSUS__TICK_PERIOD_MS=3600000

if [[ $ID == 1 ]]; then
    $QDRANT_BIN --uri http://127.0.0.1:6335 $@
else
    $QDRANT_BIN --bootstrap http://127.0.0.1:6335 --uri http://$IP:6335 $@
fi
