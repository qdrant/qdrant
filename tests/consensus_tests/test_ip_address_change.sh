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

# Disconnect `qdrant_node_follower_2` container from the network
docker network disconnect consensus_tests_default consensus_tests-qdrant_node_follower_2-1
sleep 1

# Reconnect `qdrant_node_follower_2` container with new IP address
docker network connect consensus_tests_default consensus_tests-qdrant_node_follower_2-1 --alias qdrant_node_follower_2 --ip 10.0.0.5

# Check that there's a DNS lookup error log message in `qdrant_node_1` output
declare ERROR='dns error: failed to lookup address information: Name or service not known'

if ! docker compose logs qdrant_node_1 | grep "$ERROR"
then
    echo "'$ERROR' log message not found in 'qdrant_node_1' logs" >&2
    exit 1
fi

sleep 1

# Check that `qdrant_node_follower_2` successfully reconnected to the cluster
if ! curl -sS --fail-with-body localhost:6533/collections/test_collection \
    -X PUT \
    -H 'Content-Type: application/json' \
    --data-raw '{"vectors": {"size": 4, "distance": "Cosine"}}'
then
    echo "Failed to send create collection request to 'qdrant_node_follower_2' node" >&2
    exit 2
fi
