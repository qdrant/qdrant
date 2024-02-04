#!/usr/bin/env bash

set -ex

# Ensure current path script dir
cd "$(dirname "$0")/"

function clear_after_tests()
{
  docker-compose down
}

# Prevent double building in docker-compose
docker buildx build --build-arg=PROFILE=ci --load ../../ --tag=qdrant_consensus
docker-compose up -d --force-recreate
trap clear_after_tests EXIT

# Wait for service to start
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' localhost:6433)" != "200" ]]; do
  sleep 1;
done

