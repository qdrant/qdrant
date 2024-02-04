#!/usr/bin/env bash

set -euo pipefail

function main {
	declare SELF

	SELF="$(realpath "$0")"

	docker buildx build --load ../../ --tag=qdrant_tls --build-arg=PROFILE=ci
	docker compose down --volumes
	docker compose up --detach --force-recreate

	function cleanup {
		docker compose down --timeout 20
	}

	trap cleanup EXIT

	sleep 10

	cat "$SELF" | docker container exec -i tls-qdrant_node_1-1 bash -x -s test https://node1.qdrant:6333
}

function test {
	declare URL="$1"

	# Install curl and jq
	apt-get update
	apt-get install -y curl jq

	# Create collection
	curl "$URL"/collections/test-collection \
		-X PUT \
		-H 'Content-Type: application/json' \
		--data-raw '{
			"vectors": { 
				"size": 4, 
				"distance": "Cosine"
			}
		}'

	# Upsert some points
	curl "$URL"/collections/test-collection/points \
		-X PUT \
		-H 'Content-Type: application/json' \
		--data-raw '{
			"points": [
				{ "id": 1, "vector": [0.1, 0.1, 0.1, 0.1] },
				{ "id": 2, "vector": [0.2, 0.2, 0.2, 0.2] },
				{ "id": 3, "vector": [0.3, 0.3, 0.3, 0.3] },
				{ "id": 4, "vector": [0.4, 0.4, 0.4, 0.4] }
			]
		}'

	# Get current peer ID
	declare CURRENT_PEER_ID

	CURRENT_PEER_ID="$(curl "$URL"/cluster | jq .result.peer_id)"

	# Get remote shard info
	declare INFO REMOTE_PEER_ID SHARD_ID

	curl "$URL"/collections/test-collection/cluster

	INFO="$(curl "$URL"/collections/test-collection/cluster | jq '.result.remote_shards[0]')"

	REMOTE_PEER_ID="$(echo "$INFO" | jq .peer_id)"
	SHARD_ID="$(echo "$INFO" | jq .shard_id)"

	# Initiate snapshot shard transfer
	curl "$URL"/collections/test-collection/cluster \
		-X POST \
		-H 'Content-Type: application/json' \
		--data-raw "$(
			cat <<-EOF
			{
				"replicate_shard": {
					"to_peer_id": $CURRENT_PEER_ID,
					"from_peer_id": $REMOTE_PEER_ID,
					"shard_id": $SHARD_ID,
					"method": "snapshot"
				}
			}
			EOF
		)"

	sleep 5

	# Check that snapshot shard transfer succeeded
	declare POINTS

	POINTS="$(curl "$URL"/collections/test-collection/cluster | jq ".result.local_shards[] | select(.shard_id == $SHARD_ID) | .points_count")"

	(( POINTS > 0 ))
}

function curl {
	declare URL="$1"
	declare ARGS=( "${@:2}" )

	declare STATUS=0

	command curl -sS --fail-with-body \
		--cacert ./tls/cacert.pem \
		--cert ./tls/cert.pem \
		--key ./tls/key.pem \
		"$URL" \
		"${ARGS[@]}" || STATUS=$?

	echo && (( STATUS == 0 )) || echo >&2

	return $STATUS
}

if (( $# == 0 ))
then
	main
else
	"$@"
fi
