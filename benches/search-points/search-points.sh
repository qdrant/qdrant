#!/bin/bash

set -euo pipefail


QDRANT_IMAGE="${QDRANT_IMAGE:-qdrant-mmap-advice-bench}"

BFB_PATH="${BFB_PATH:-}"
BFB_IMAGE="${BFB_IMAGE:-bfb}"

MMAP_ADVICE_PATH="${MMAP_ADVICE_PATH:-$(dirname "$(realpath -L "${BASH_SOURCE[${#BASH_SOURCE[@]} - 1]}")")}"

CRITERION_BASELINE="${CRITERION_BASELINE:-}"
CRITERION_WARMUP_TIME="${CRITERION_WARMUP_TIME:-20}"
CRITERION_MEASUREMENT_TIME="${CRITERION_MEASUREMENT_TIME:-300}"
CRITERION_SAMPLE_SIZE="${CRITERION_SAMPLE_SIZE:-35}"

ON_DISK_INDEX="${ON_DISK_INDEX:-1}"
MEMMAP_THRESHOLD="${MEMMAP_THRESHOLD:-20000}"

VECTORS_COUNT="${VECTORS_COUNT:-1000000}"
VECTORS_DIM="${VECTORS_DIM:-100}"

MEMORY="${MEMORY:-220mb}"
STORAGE_PATH="${STORAGE_PATH:-$PWD/storage}"

STARTUP_DELAY="${STARTUP_DELAY:-15}"


if [[ "$STORAGE_PATH" != "$(realpath -L $STORAGE_PATH)" ]]
then
	echo "Storage path `{}` is not an absolute path" >&2
	exit 1
fi


function benchmark-normal {
	benchmark
}

function benchmark-random {
	benchmark --mmap-random
}

function benchmark {
	declare args=( "$@" )

	if [[ ! -d $STORAGE_PATH ]]
	then
		echo "Storage path `$STORAGE_PATH` does not exist or is not a directory" >&2
		return 1
	fi

	sudo bash -c 'sync; echo 1 > /proc/sys/vm/drop_caches'

	declare qdrant_container_id; qdrant_container_id="$(qdrant-run "${args[@]}")"

	cargo-bench

	# TODO: Use a trap?
	docker-stop "$qdrant_container_id"
}

function cargo-bench {
	declare args=(
		${CRITERION_BASELINE:+--save-baseline} "$CRITERION_BASELINE"
		${CRITERION_WARMUP_TIME:+--warm-up-time} "$CRITERION_WARMUP_TIME"
		${CRITERION_MEASUREMENT_TIME:+--measurement-time} "$CRITERION_MEASUREMENT_TIME"
		${CRITERION_SAMPLE_SIZE:+--sample-size} "$CRITERION_SAMPLE_SIZE"
	)

	cd "$MMAP_ADVICE_PATH"
	QDRANT_DIM="$VECTORS_DIM" cargo bench -p mmap-advice --bench search-points -- "${args[@]}"
	cd - >/dev/null
}


function prepare {
	if [[ -e $STORAGE_PATH && ! -d $STORAGE_PATH ]]
	then
		echo "Storage path `$STORAGE_PATH` already exists and is not a directory" >&2
		return 1
	elif [[ -d $STORAGE_PATH ]]
	then
		echo "Storage directory `$STORAGE_PATH` already exists" >&2
		return 2
	fi

	declare qdrant_container_id; qdrant_container_id="$(MEMORY=2048mb STARTUP_DELAY=0 qdrant-run)"

	qdrant-create-collection
	bfb --skip-create --num-vectors "$VECTORS_COUNT" --dim "$VECTORS_DIM"

	# TODO: Use a trap?
	docker-stop "$qdrant_container_id"
}


function bfb {
	declare args=( "$@" )

	if [[ -n $BFB_PATH && -x $BFB_PATH ]]
	then
		$BFB_PATH "${args[@]}"
	elif whence -p bfb &>/dev/null
	then
		bfb "${args[@]}"
	elif docker-check-image-exists "$BFB_IMAGE"
	then
		sudo docker run --interactive --tty --rm --network=host "$BFB_IMAGE" ./bfb "${args[@]}"
	else
		echo "Unable to run `bfb`" >&2
		return 1
	fi
}


function qdrant-run {
	declare args=( "$@" )

	declare qdrant_container_id; qdrant_container_id="$(
		sudo docker run --detach --rm \
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
			"vectors": { "size": $VECTORS_DIM, "distance": "Cosine" }
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
	declare container_id="$1"

	sudo docker stop "$container_id" >/dev/null
}

function docker-check-image-exists {
	declare tag="$1"

	[[ -n "$(sudo docker images -q "$tag" 2>/dev/null)" ]]
}

function docker-check-container-alive {
	declare container_id="$1"

	[[ -n "$(sudo docker ps -q -f id="$container_id" 2>/dev/null)" ]]
}


if ! (return 0 &>/dev/null)
then
	"$@"
fi
