#!/usr/bin/env bash

set -eEuo pipefail


declare QDRANT_HOST="${QDRANT_HOST:-localhost}"

declare QDRANT_HTTP_PORT="${QDRANT_HTTP_PORT:-6333}"
declare QDRANT_GRPC_PORT="${QDRANT_GRPC_PORT:-6334}"


declare BFB="${BFB-}"


declare FILESERVER_ADDR="${FILESERVER_ADDR:-127.0.0.1}"
declare FILESERVER_PORT="${FILESERVER_PORT:-8080}"

# When running Qdrant in Docker on macOS, use `http://host.docker.internal:8080`
# to access *macOS* localhost from inside the container
declare FILESERVER_URL="${FILESERVER_URL:-http://localhost:8080}"


declare CLUSTER="${CLUSTER-}"
declare REPLICATION_FACTOR="${REPLICATION_FACTOR:-2}"


declare COLLECTION_CREATED=0
declare POINTS_UPSERTED=0

declare SNAPSHOT=''
declare SNAPSHOT_POINTS=0

declare DOWNLOADED_SNAPSHOT=''
declare DOWNLOADED_SNAPSHOT_POINTS=0

declare FILESERVER_PID=''


function main {
	load-qdrant-status
	trap 'failure $LINENO' ERR
	trap cleanup EXIT
	"$@"
}

function load-qdrant-status {
	if [[ ! $CLUSTER ]]
	then
		declare STATUS ; STATUS="$(curl-ok "http://$QDRANT_HOST:$QDRANT_HTTP_PORT/cluster" | jq -r .result.status)"

		case "$STATUS" in
			enabled) CLUSTER=1 ;;
			disabled) CLUSTER=0 ;;

			*)
				echo "load-qdrant-status: invalid cluster status $STATUS" >&2
				return 1
			;;
		esac
	fi

	if POINTS_COUNT="$(points-count)"
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
	kill-jobs

	if [[ $DOWNLOADED_SNAPSHOT && -f $DOWNLOADED_SNAPSHOT ]]
	then
		rm "$DOWNLOADED_SNAPSHOT"
	fi
}

function failure {
	printf "Exit code: %d\n" $? >&2
	declare INDEX
	for (( INDEX=0; INDEX<${#BASH_LINENO[@]}-1; INDEX++ ))
	do
		printf "%s:%d@%s: " \
			"${BASH_SOURCE[INDEX+1]}" \
			"$(( INDEX > 0 ? BASH_LINENO[INDEX] : $1 ))" \
			"${FUNCNAME[INDEX+1]}"
		(( INDEX > 0 )) && \
			printf "%s\n" "${FUNCNAME[INDEX]}" || \
			printf "%s\n" " $BASH_COMMAND"
	done >&2
}

function kill-jobs {
	# shellcheck disable=SC2046
	kill $(jobs -p) &>/dev/null || :
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
	${CLUSTER:+recover-local-priority-snapshot}
	recover-local-concurrent
	${CLUSTER:+recover-local-concurrent-priority-snapshot}
	recover-local-invalid-collection
	recover-local-invalid-shard
	recover-local-invalid-snapshot

	recover-remote
	${CLUSTER:+recover-remote-priority-snapshot}
	recover-remote-concurrent
	${CLUSTER:+recover-remote-concurrent-priority-snapshot}
	recover-remote-invalid-collection
	recover-remote-invalid-shard
	recover-remote-invalid-snapshot

	upload
	${CLUSTER:+upload-priority-snapshot}
	upload-concurrent
	${CLUSTER:+upload-concurrent-priority-snapshot}
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

function test-all {
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
	curl-status 404 "$(url - 99)"
}


function create {
	fixture-with-points

	declare RESPONSE ; RESPONSE="$(curl-ok -X POST "$(url - -)")"
	SNAPSHOT="$(echo "$RESPONSE" | jq -r .result.name)"
	echo "$RESPONSE" | jq

	SNAPSHOT_POINTS="$(points-count)"
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
	curl-status 404 -X POST "$(url - 99)"
}


function do-recover {
	declare LOCATION="$1"
	declare PRIORITY="${2:-replica}"

	curl-ok \
		-X PUT "$(url - -)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw "$(json --arg location "$LOCATION" --arg priority "$PRIORITY")"
}


function recover-local {
	fixture-with-snapshot
	fixture-with-empty-collection

	do-recover "$SNAPSHOT" "$@"
	check-recovered - "$SNAPSHOT_POINTS" "$@"
}

function recover-local-priority-snapshot {
	recover-local snapshot
}

function recover-local-concurrent {
	fixture-with-snapshot
	fixture-with-empty-collection

	concurrent - do-recover "$SNAPSHOT" "$@"
	check-recovered - "$SNAPSHOT_POINTS" "$@"
}

function recover-local-concurrent-priority-snapshot {
	recover-local-concurrent snapshot
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
		-X PUT "$(url - 99)"/recover \
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

	do-recover "$FILESERVER_URL/$DOWNLOADED_SNAPSHOT" "$@"
	check-recovered - "$DOWNLOADED_SNAPSHOT_POINTS" "$@"
}

function recover-remote-priority-snapshot {
	recover-remote snapshot
}

function recover-remote-concurrent {
	fixture-with-remote-snapshot
	fixture-with-empty-collection

	concurrent - do-recover "$FILESERVER_URL/$DOWNLOADED_SNAPSHOT" "$@"
	check-recovered - "$DOWNLOADED_SNAPSHOT_POINTS" "$@"
}

function recover-remote-concurrent-priority-snapshot {
	recover-remote-concurrent snapshot
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
		-X PUT "$(url - 99)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw '{ "location": "http://localhost:8080/invalid.snapshot" }'
}

function recover-remote-invalid-snapshot {
	fixture-with-collection

	curl-status 404 \
		-X PUT "$(url - 99)"/recover \
		-H 'Content-Type: application/json' \
		--data-raw '{ "location": "http://localhost:8080/invalid.snapshot" }'
}


function do-upload {
	declare PRIORITY="${1:-replica}"

	curl-ok -X POST "$(url - -)/upload?priority=$PRIORITY" -F snapshot=@"$DOWNLOADED_SNAPSHOT"
}

function upload {
	fixture-with-downloaded-snapshot
	fixture-with-empty-collection

    # Memory usage before upload
    echo "Memory usage before upload:"
    free -h

	do-upload "$@"
	check-recovered - "$DOWNLOADED_SNAPSHOT_POINTS" "$@"

    # Memory usage after upload
    echo "Memory usage after upload:"
    free -h
}

function upload-priority-snapshot {
	upload snapshot
}

function upload-concurrent {
	fixture-with-downloaded-snapshot
	fixture-with-empty-collection

	concurrent - do-upload "$@"
	check-recovered - "$DOWNLOADED_SNAPSHOT_POINTS" "$@"
}

function upload-concurrent-priority-snapshot {
	upload-concurrent snapshot
}

function upload-invalid-collection {
	curl-status 404 -X POST "$(url invalid-collection -)"/upload -F snapshot=invalid-snapshot-data
}

function upload-invalid-shard {
	fixture-with-collection
	curl-status 404 -X POST "$(url - 99)"/upload -F snapshot=invalid-snapshot-data
}


function download {
	fixture-with-collection
	fixture-with-snapshot

	DOWNLOADED_SNAPSHOT=downloaded.snapshot
	curl-ok "$(url - - -)" -o "$DOWNLOADED_SNAPSHOT"

	DOWNLOADED_SNAPSHOT_POINTS="$SNAPSHOT_POINTS"
}

function download-invalid-collection {
	curl-status 404 "$(url invalid-collection - invalid.snapshot)"
}

function download-invalid-shard {
	fixture-with-collection
	curl-status 404 "$(url - 99 invalid.snapshot)"
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
	SNAPSHOT_POINTS=0
}

function delete-invalid-collection {
	curl-status 404 -X DELETE "$(url invalid-collection - invalid.snapshot)"
}

function delete-invalid-shard {
	fixture-with-collection
	curl-status 404 -X DELETE "$(url - 99 invalid.snapshot)"
}

function delete-invalid-snapshot {
	fixture-with-collection
	curl-status 404 -X DELETE "$(url - - invalid.snapshot)"
}


function fixture-with-collection {
	if (( ! COLLECTION_CREATED ))
	then
		if [[ $BFB ]]
		then
			declare DIM=128
		else
			declare DIM=4
		fi

		declare JSON=(
			--argjson vectors "$(json --argjson size $DIM --arg distance Cosine)"
		)

		(( CLUSTER )) && JSON+=(
			--argjson replication_factor "$REPLICATION_FACTOR"
		)

		curl-ok \
			-X PUT "$(url)" \
			-H 'Content-Type: application/json' \
			--data-raw "$(json "${JSON[@]}")"

		COLLECTION_CREATED=1
	fi
}

function fixture-with-points {
	fixture-with-collection

	if (( ! POINTS_UPSERTED ))
	then
		if [[ $BFB ]]
		then
			"$BFB" \
				--uri "http://$QDRANT_HOST:$QDRANT_GRPC_PORT" \
				--collection-name "$(basename "$(url)")" \
				--dim 128 \
                --num-vectors 100000 \
				--skip-create
		else
			curl-ok \
				-X PUT "$(url)"/points \
				-H 'Content-Type: application/json' \
				--data-raw '{
					"points": [
						{ "id": 1, "vector": [0.1, 0.1, 0.1, 0.1] },
						{ "id": 2, "vector": [0.2, 0.2, 0.2, 0.2] },
						{ "id": 3, "vector": [0.3, 0.3, 0.3, 0.3] },
						{ "id": 4, "vector": [0.4, 0.4, 0.4, 0.4] }
					]
				}'
		fi

		POINTS_UPSERTED=1
	fi
}

function fixture-with-snapshot {
	if [[ ! $SNAPSHOT ]]
	then
		create
		SNAPSHOT_POINTS="$(points-count)"
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


function check-recovered {
	declare POINTS ; POINTS="$(or-default "$1" 0)"
	declare SNAPSHOT_POINTS="$2"
	declare PRIORITY="${3:-replica}"


	(( CLUSTER )) && sleep 5


	if (( CLUSTER )) && [[ $PRIORITY == replica ]]
	then
		declare EXPECTED="$POINTS"
	else
		declare EXPECTED="$SNAPSHOT_POINTS"
	fi

	[[ "$(points-count)" == "$EXPECTED" ]]
	POINTS_UPSERTED=$(( EXPECTED > 0 ))


	(( CLUSTER )) || return 0

	case "$PRIORITY" in
		snapshot)
			:
		;;

		replica)
			:
		;;

		*)
			echo "check-recovered: invalid snapshot priority $PRIORITY" >&2
			return 1
		;;
	esac
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


	declare URL="$QDRANT_HOST:$QDRANT_HTTP_PORT"

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
		echo "url: expected no more than 3 arguments (collection, shard, snapshot), but received $# (${*})" >&2
		return 1
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

function json {
	jq -nc '$ARGS.named' "$@"
}

function points-count {
	curl-ok "$(url)" | jq .result.points_count
}

function concurrent {
	declare PARALLEL ; PARALLEL="$(or-default "$1" 10)"
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

		# shellcheck disable=SC2184
		unset JOBS[$JOB]
	done
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
