#!/usr/bin/env bash

set -euo pipefail


declare URL="${URL:-localhost:6333}"

declare COLLECTION_CREATED=0
declare POINTS_UPSERTED=0

declare SNAPSHOT=''


function main {
	load-collection-status
	"$@"
}

function load-collection-status {
	declare POINTS_COUNT

	if POINTS_COUNT="$(curl-ok "$(url)" | jq .result.points_count)" || true
	then
		COLLECTION_CREATED=1

		if (( POINTS_COUNT > 0 ))
		then
			POINTS_UPSERTED=1
		fi
	fi

	return 0
}


declare TESTS=(
	list-snapshots
	list-snapshots-wrong-collection
	list-snapshots-wrong-shard

	create-snapshot
	create-snapshot-concurrent
	create-snapshot-wrong-collection
	create-snapshot-wrong-shard

	# recover-local-snapshot
	# recover-local-concurrent
	recover-local-snapshot-wrong-collection
	recover-local-snapshot-wrong-shard
	# recover-local-snapshot-wrong-snapshot

	# recover-remote
	# recover-remote-concurrent
	recover-remote-wrong-collection
	recover-remote-wrong-shard
	# recover-remote-wrong-snapshot

	upload
	upload-concurrent
	upload-invalid-collection
	upload-invalid-shard

	download-snapshot
	download-snapshot-wrong-collection
	download-snapshot-wrong-shard
	download-snapshot-wrong-snapshot

	delete-snapshot
	delete-snapshot-wrong-collection
	delete-snapshot-wrong-shard
	delete-snapshot-wrong-snapshot
)

function all {
	for TEST in "${TESTS[@]}"
	do
		echo "Running $TEST..."
		"$TEST"
	done
}


function list-snapshots {
	fixture-with-collection
	curl-ok "$(url - -)"
}

function list-snapshots-wrong-collection {
	curl-status 404 "$(url invalid-collection -)"
}

function list-snapshots-wrong-shard {
	fixture-with-collection
	curl-status 404 "$(url - 1)"
}


function create-snapshot {
	fixture-with-points
	curl-ok -X POST "$(url - -)"
}

function create-snapshot-concurrent {
	fixture-with-points
	:
}

function create-snapshot-wrong-collection {
	curl-status 404 -X POST "$(url invalid-collection -)"
}

function create-snapshot-wrong-shard {
	fixture-with-collection
	curl-status 404 -X POST "$(url - 1)"
}


# TODO: Switch from `file://` URL to simple snapshot filename
function recover-local-snapshot {
	fixture-with-snapshot
	fixture-with-empty-collection

	curl-ok \
		-X PUT "$(url - -)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw "{ \"location\": \"$SNAPSHOT\" }"
}

function recover-local-concurrent {
	fixture-with-snapshot
	fixture-with-empty-collection
	:
}

# TODO: Switch from `file://` URL to simple snapshot filename
function recover-local-snapshot-wrong-collection {
	curl-status 404 \
		-X PUT "$(url invalid-collection -)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw '{ "location": "file:///invalid.snapshot" }'
}

# TODO: Switch from `file://` URL to simple snapshot filename
function recover-local-snapshot-wrong-shard {
	fixture-with-collection

	curl-status 404 \
		-X PUT "$(url - 1)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw '{ "location": "file:///invalid.snapshot" }'
}

# TODO: Switch from `file://` URL to simple snapshot filename
# TODO: `file:///invalid.snapshot` returns `400`, not `404`? 🤔
function recover-local-snapshot-wrong-snapshot {
	fixture-with-collection

	curl-status 404 \
		-X PUT "$(url - -)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw '{ "location": "invalid.snapshot" }'
}


function recover-remote {
	fixture-with-remote-snapshot
	fixture-with-empty-collection
	:
}

function recover-remote-concurrent {
	fixture-with-remote-snapshot
	fixture-with-empty-collection
	:
}

function recover-remote-wrong-collection {
	curl-status 404 \
		-X PUT "$(url invalid-collection -)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw '{ "location": "http://invalid-host/invalid.snapshot" }'
}

function recover-remote-wrong-shard {
	fixture-with-collection

	curl-status 404 \
		-X PUT "$(url - 1)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw '{ "location": "http://localhost:8080/invalid.snapshot" }'
}

function recover-remote-wrong-snapshot {
	fixture-with-collection

	curl-status 404 \
		-X PUT "$(url - 1)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw '{ "location": "http://localhost:8080/invalid.snapshot" }'
}


function upload {
	fixture-with-downloaded-snapshot
	fixture-with-empty-collection
	:
}

function upload-concurrent {
	fixture-with-downloaded-snapshot
	fixture-with-empty-collection
	:
}

function upload-invalid-collection {
	:
}

function upload-invalid-shard {
	fixture-with-collection
	:
}


function download-snapshot {
	fixture-with-collection
	fixture-with-snapshot
	curl-ok "$(url - - -)" -o /dev/null
}

function download-snapshot-wrong-collection {
	curl-status 404 "$(url invalid-collection - invalid.snapshot)"
}

function download-snapshot-wrong-shard {
	fixture-with-collection
	curl-status 404 "$(url - 1 invalid.snapshot)"
}

function download-snapshot-wrong-snapshot {
	fixture-with-collection
	curl-status 404 "$(url - - invalid.snapshot)"
}


function delete-snapshot {
	fixture-with-collection
	fixture-with-snapshot
	curl-ok -X DELETE "$(url - - -)"
}

function delete-snapshot-wrong-collection {
	curl-status 404 -X DELETE "$(url invalid-collection - invalid.snapshot)"
}

function delete-snapshot-wrong-shard {
	fixture-with-collection
	curl-status 404 -X DELETE "$(url - 1 invalid.snapshot)"
}

function delete-snapshot-wrong-snapshot {
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
		fixture-with-points
		SNAPSHOT="$(curl-ok -X POST "$(url - -)" | jq -r .result.name)"
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
	fixture-with-collection
	fixture-with-snapshot

	:
}

function fixture-with-remote-snapshot {
	fixture-with-downloaded-snapshot

	:
}


function curl-ok {
	curl -sS --fail-with-body "$@" | jq # TODO!?
}

function curl-status {
	declare EXPECTED="$1"
	declare ARGS=( "${@:2}" )

	declare STATUS ; STATUS="$(curl -sS -w '%{http_code}' -o /dev/null "${ARGS[@]}")"
	[[ $STATUS == "$EXPECTED" ]]
}

function curl-status-with-body {
	declare EXPECTED="$1"
	declare ARGS=( "${@:2}" )

	echo "curl-status-with-body: unimplemented" >&2
	return 1
}


function url {
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
