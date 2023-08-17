#!/usr/bin/env bash

set -euo pipefail


declare QDRANT_HOST="${QDRANT_HOST:-localhost}"

declare QDRANT_HTTP_PORT="${QDRANT_HTTP_PORT:-6333}"
declare QDRANT_GRPC_PORT="${QDRANT_GRPC_PORT:-6334}"


declare BFB="${BFB-}"


declare FILESERVER_ADDR="${FILESERVER_ADDR:-127.0.0.1}"
declare FILESERVER_PORT="${FILESERVER_PORT:-8080}"

# When running Docker on macOS, use `host.docker.internal` to access *macOS* localhost
declare FILESERVER_URL="${FILESEVER_URL:-http://localhost:8080}"


declare COLLECTION_CREATED=0
declare POINTS_UPSERTED=0

declare SNAPSHOT=''

declare DOWNLOADED_SNAPSHOT=''


declare FILESERVER_PID=''


function main {
	load-collection-status
	trap cleanup EXIT
	"$@"
}

function load-collection-status {
	declare POINTS_COUNT

	if POINTS_COUNT="$(curl-ok "$(url)" | jq .result.points_count)"
	then
		COLLECTION_CREATED=1

		if (( POINTS_COUNT > 0 ))
		then
			POINTS_UPSERTED=1
		fi
	fi

	return 0
}

function cleanup {
	if [[ $DOWNLOADED_SNAPSHOT && -f $DOWNLOADED_SNAPSHOT ]]
	then
		rm "$DOWNLOADED_SNAPSHOT"
	fi

	if [[ $FILESERVER_PID ]]
	then
		kill -TERM "$FILESERVER_PID"
	fi
}


declare TESTS=(
	list
	list-invalid-collection
	list-invalid-shard

	create
	create-concurrent
	create-invalid-collection
	create-invalid-shard

	recover-local
	recover-local-concurrent
	recover-local-invalid-collection
	recover-local-invalid-shard
	recover-local-invalid-snapshot

	recover-remote
	recover-remote-concurrent
	recover-remote-invalid-collection
	recover-remote-invalid-shard
	recover-remote-invalid-snapshot

	upload
	upload-concurrent
	upload-invalid-collection
	upload-invalid-shard

	download
	download-invalid-collection
	download-invalid-shard
	download-invalid-snapshot

	delete
	delete-invalid-collection
	delete-invalid-shard
	delete-invalid-snapshot
)

function all {
	for TEST in "${TESTS[@]}"
	do
		echo "Running $TEST..."
		"$TEST"
	done
}


function list {
	fixture-with-collection
	curl-ok "$(url - -)"
}

function list-invalid-collection {
	curl-status 404 "$(url invalid-collection -)"
}

function list-invalid-shard {
	fixture-with-collection
	curl-status 404 "$(url - 1)"
}


function create {
	fixture-with-points

	declare RESPONSE ; RESPONSE="$(curl-ok -X POST "$(url - -)")"
	SNAPSHOT="$(echo "$RESPONSE" | jq -r .result.name)"
	echo "$RESPONSE" | jq
}

function create-concurrent {
	fixture-with-points
	concurrent - create
}

function create-invalid-collection {
	curl-status 404 -X POST "$(url invalid-collection -)"
}

function create-invalid-shard {
	fixture-with-collection
	curl-status 404 -X POST "$(url - 1)"
}


function recover-local {
	fixture-with-snapshot
	fixture-with-empty-collection

	curl-ok \
		-X PUT "$(url - -)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw "{ \"location\": \"$SNAPSHOT\" }"

	# TODO: Check that shard was successfully restored!
	POINTS_UPSERTED=1
}

function recover-local-concurrent {
	fixture-with-snapshot
	fixture-with-empty-collection

	concurrent - recover-local
	# TODO: Check that shard was successfully restored!
	POINTS_UPSERTED=1
}

function recover-local-invalid-collection {
	curl-status 404 \
		-X PUT "$(url invalid-collection -)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw '{ "location": "invalid.snapshot" }'
}

function recover-local-invalid-shard {
	fixture-with-collection

	curl-status 404 \
		-X PUT "$(url - 1)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw '{ "location": "invalid.snapshot" }'
}

function recover-local-invalid-snapshot {
	fixture-with-collection

	curl-status 404 \
		-X PUT "$(url - -)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw '{ "location": "invalid.snapshot" }'
}


function recover-remote {
	fixture-with-remote-snapshot
	fixture-with-empty-collection

	curl-ok \
		-X PUT "$(url - -)/recover" \
		-H 'Content-Type: application/json' \
		--data-raw "{ \"location\": \"$FILESERVER_URL/$DOWNLOADED_SNAPSHOT\" }"

	# TODO: Check that shard was successfully restored!
	POINTS_UPSERTED=1
}

function recover-remote-concurrent {
	fixture-with-remote-snapshot
	fixture-with-empty-collection

	concurrent - recover-remote
	# TODO: Check that shard was successfully restored!
	POINTS_UPSERTED=1
}

function recover-remote-invalid-collection {
	curl-status 404 \
		-X PUT "$(url invalid-collection -)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw '{ "location": "http://localhost:8080/invalid.snapshot" }'
}

function recover-remote-invalid-shard {
	fixture-with-collection

	curl-status 404 \
		-X PUT "$(url - 1)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw '{ "location": "http://localhost:8080/invalid.snapshot" }'
}

function recover-remote-invalid-snapshot {
	fixture-with-collection

	curl-status 404 \
		-X PUT "$(url - 1)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw '{ "location": "http://localhost:8080/invalid.snapshot" }'
}


function upload {
	fixture-with-downloaded-snapshot
	fixture-with-empty-collection

	curl-ok -X POST "$(url - -)/upload" -F snapshot=@"$DOWNLOADED_SNAPSHOT"
	# TODO: Check that shard was successfully restored!
	POINTS_UPSERTED=1
}

function upload-concurrent {
	fixture-with-downloaded-snapshot
	fixture-with-empty-collection

	concurrent - upload
	# TODO: Check that shard was successfully restored!
	POINTS_UPSERTED=1
}

function upload-invalid-collection {
	curl-status 404 -X POST "$(url invalid-collection -)/upload" -F snapshot=invalid-snapshot-data
}

function upload-invalid-shard {
	fixture-with-collection
	curl-status 404 -X POST "$(url - 1)/upload" -F snapshot=invalid-snapshot-data
}


function download {
	fixture-with-collection
	fixture-with-snapshot

	DOWNLOADED_SNAPSHOT=downloaded.snapshot
	curl-ok "$(url - - -)" -o "$DOWNLOADED_SNAPSHOT"
}

function download-invalid-collection {
	curl-status 404 "$(url invalid-collection - invalid.snapshot)"
}

function download-invalid-shard {
	fixture-with-collection
	curl-status 404 "$(url - 1 invalid.snapshot)"
}

function download-invalid-snapshot {
	fixture-with-collection
	curl-status 404 "$(url - - invalid.snapshot)"
}


function delete {
	fixture-with-collection
	fixture-with-snapshot

	curl-ok -X DELETE "$(url - - -)"
	# TODO: Check that snapshot was successfully deleted!
	SNAPSHOT=''
}

function delete-invalid-collection {
	curl-status 404 -X DELETE "$(url invalid-collection - invalid.snapshot)"
}

function delete-invalid-shard {
	fixture-with-collection
	curl-status 404 -X DELETE "$(url - 1 invalid.snapshot)"
}

function delete-invalid-snapshot {
	fixture-with-collection
	curl-status 404 -X DELETE "$(url - - invalid.snapshot)"
}


function fixture-with-collection {
	if (( ! COLLECTION_CREATED ))
	then
		curl-ok \
			-X PUT "$(url)" \
			-H 'Content-Type: application/json' \
			--data-raw '{
				"vectors": {
					"size": 4,
					"distance": "Cosine"
				}
			}'

		COLLECTION_CREATED=1
	fi
}

function fixture-with-points {
	fixture-with-collection

	if (( ! POINTS_UPSERTED ))
	then
		curl-ok \
			-X PUT "$(url)/points" \
			-H 'Content-Type: application/json' \
			--data-raw '{
				"points": [
					{ "id": 1, "vector": [0.1, 0.1, 0.1, 0.1] },
					{ "id": 2, "vector": [0.2, 0.2, 0.2, 0.2] },
					{ "id": 3, "vector": [0.3, 0.3, 0.3, 0.3] },
					{ "id": 4, "vector": [0.4, 0.4, 0.4, 0.4] }
				]
			}'

		POINTS_UPSERTED=1
	fi
}

function fixture-with-snapshot {
	if [[ ! $SNAPSHOT ]]
	then
		create
	fi
}

function fixture-without-collection {
	if (( COLLECTION_CREATED ))
	then
		curl-ok -X DELETE "$(url)"
		COLLECTION_CREATED=0
		POINTS_UPSERTED=0
	fi
}

function fixture-with-empty-collection {
	if (( POINTS_UPSERTED ))
	then
		fixture-without-collection
	fi

	fixture-with-collection
}

function fixture-with-downloaded-snapshot {
	if [[ ! $DOWNLOADED_SNAPSHOT || ! -f $DOWNLOADED_SNAPSHOT ]]
	then
		download
	fi
}

function fixture-with-remote-snapshot {
	fixture-with-downloaded-snapshot

	if [[ ! $FILESERVER_PID ]]
	then
		python3 -m http.server -b "$FILESERVER_ADDR" "$FILESERVER_PORT" &
		FILESERVER_PID="$!"

		sleep 0.5
	fi
}


function curl-ok {
	if [[ -t 1 ]]
	then
		curl -sS --fail-with-body "$@" | jq
	else
		curl -sS --fail-with-body "$@"
	fi
}

function curl-status {
	declare EXPECTED="$1"
	declare ARGS=( "${@:2}" )

	declare STATUS ; STATUS="$(curl -sS -w '%{http_code}' -o /dev/null "${ARGS[@]}")"
	[[ $STATUS == "$EXPECTED" ]]
}

function concurrent {
	declare PARALLEL ; PARALLEL="$(or-default "$1" 2)"
	declare CMD=( "${@:2}" )

	declare -A JOBS

	for _ in $(seq "$PARALLEL")
	do
		"${CMD[@]}" &
		JOBS[$!]=$!
	done

	declare JOB

	for _ in $(seq "$PARALLEL")
	do
		wait -n -p JOB "${JOBS[@]}"
		unset JOBS[$JOB]
	done
}


function url {
	declare QDRANT_PORT="${PORT:-$QDRANT_HTTP_PORT}"

	declare URL="$QDRANT_HOST:$QDRANT_PORT"


	declare COLLECTION=test-collection
	declare SHARD=0
	declare SNAPSHOT="${SNAPSHOT-}"

	if (( $# >= 1 ))
	then
		COLLECTION="$(or-default "$1" "$COLLECTION")"
	fi

	if (( $# >= 2 ))
	then
		SHARD="$(or-default "$2" "$SHARD")"
	fi

	if (( $# >= 3 ))
	then
		SNAPSHOT="$(or-default "$3" "$SNAPSHOT")"
	fi


	if (( $# <= 1 ))
	then
		echo "$URL/collections/$COLLECTION"
	elif (( $# == 2 ))
	then
		echo "$URL/collections/$COLLECTION/shards/$SHARD/snapshots"
	elif (( $# == 3 ))
	then
		echo "$URL/collections/$COLLECTION/shards/$SHARD/snapshots/$SNAPSHOT"
	else
		echo "url: expected no more than 3 arguments (collection, shard, snapshot), but received $# (${*:4})" >&2
		return 1
	fi
}

function or-default {
	declare ARG="$1"
	declare DEFAULT="$2"

	if [[ ! $ARG ]]
	then
		echo 'or-default: ARG is null' >&2
		return 1
	elif [[ $ARG != - ]]
	then
		echo "$ARG"
	elif [[ ! $DEFAULT ]]
	then
		echo 'or-default: DEFAULT is null' >&2
		return 2
	else
		echo "$DEFAULT"
	fi
}


main "$@"
