#!/bin/bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

# docker build ../../ --tag=qdrant_consensus
docker compose up -d --force-recreate

function cleanup {
	docker compose down
}

trap cleanup EXIT

# Wait for the service to start
while [[ "$(curl -sS localhost:6533 -w ''%{http_code}'' -o /dev/null)" != "200" ]]
do
	sleep 1
done

# Pause `qdrant_node_follower_2` container, to trigger HTTP/2 keep-alive timeout
docker container pause consensus_tests-qdrant_node_follower_2-1
sleep 1

# Check that there's a HTTP/2 keep-alive timeout log message in `qdrant_node_1` output
declare ERROR='connection keep-alive timed out'

if ! docker compose logs qdrant_node_1 | grep "$ERROR"
then
	echo "'$ERROR' log message not found in 'qdrant_node_1' logs" >&2
	exit 1
fi
