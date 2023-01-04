#!/bin/bash

set -euo pipefail


QDRANT_IMAGE="${QDRANT_IMAGE:-qdrant-mmap-advice-bench}"

BFB_PATH="${BFB_PATH:-}"
BFB_IMAGE="${BFB_IMAGE:-bfb}"

ON_DISK_INDEX="${ON_DISK_INDEX:-1}"
MEMMAP_THRESHOLD="${MEMMAP_THRESHOLD:-20000}"

VECTORS_COUNT="${VECTORS_COUNT:-1000000}"
VECTORS_DIM="${VECTORS_DIM:-100}"

MEMORY="${MEMORY:-790mb}"
STORAGE_PATH="${STORAGE_PATH:-$PWD/storage}"

STARTUP_DELAY="${STARTUP_DELAY:-15}"


function prepare {
    if [[ -e $STORAGE_PATH && ! -d $STORAGE_PATH ]]
    then
        echo "Path `$STORAGE_PATH` already exists and is not a directory" >&2
        return 1
    elif [[ -d $STORAGE_PATH ]]
    then
        echo "Directory `$STORAGE_PATH` already exists" >&2
        return 2
    fi

    declare qdrant_container_id; qdrant_container_id="$(MEMORY=2048mb qdrant-run)"

    qdrant-create-collection
    bfb --skip-create --num-vectors "${VECTORS_COUNT}" --dim "${VECTORS_DIM}"

    docker-stop "$qdrant_container_id"
}

function benchmark-normal {
    :
}

function benchmark-random {
    :
}

function benchmark {
    :
}

function bfb {
    declare args=( "$@" )

    if [[ -n $BFB_PATH && -x $BFB_PATH ]]
    then
        $BFB_PATH "${args[@]}"
    elif whence -p bfb &>/dev/null
    then
        bfb "${args[@]}"
    else docker-check-image-exists "$BFB_IMAGE"
    then
        docker run --interactive --tty --rm --network=host "$BFB_IMAGE" ./bfb "${args[@]}"
    else
        echo "Unable to run `bfb`" >&2
        return 1
    fi
}

function qdrant-run {
    declare args=( "$@" )

    declare qdrant_container_id; qdrant_container_id="$(
        docker run --detach --rm \
            --network=host \
            --memory "$MEMORY" \
            -v "$STORAGE_PATH:/qdrant/storage" \
            "$QDRANT_IMAGE" \
            ./qdrant "${args[@]}"
    )"

    sleep $STARTUP_DELAY

    docker-check-container-alive "$qdrant_container_id"

    echo "$qdrant_container_id"
}

function qdrant-create-collection {
    curl 'http://127.0.0.1:6333/collections/benchmark' \
        -X PUT \
        -H 'Content-Type: application/json' \
        --data-raw "$(qdrant-collection-config)"
}

function qdrant-collection-config {
    cat <<-EOF
        {
            "vectors": {
              "size": $VECTORS_DIM,
              "distance": "Cosine"
            }
            $(qdrant-hnsw-config)
            $(qdrant-optimizers-config)
        }
    EOF
}

function qdrant-hnsw-config {
    if [[ -n "$ON_DISK_INDEX" && "$ON_DISK_INDEX" != 0 ]]
    then
        echo ', "hnsw_config": { "on_disk": true }'
    fi
}

function qdrant-optimizers-config {
    if [[ -n "$MEMMAP_THRESHOLD" ]]
    then
        echo ', "optimizers_config": { "memmap_threshold_kb": 20000 }'
    fi
}

function docker-stop {
    decalre container_id="$1"

    docker stop "$container_id"
}

function docker-check-image-exists {
    declare tag="$1"

    [[ -n "$(docker images -q "$tag" 2>/dev/null)" ]]
}

function docker-check-container-alive {
    declare container_id="$1"

    [[ -n "$(docker ps -q -f id="$container_id" 2>/dev/null)" ]]
}
