#!/usr/bin/env bash

set -ex

# Ensure current path script dir
cd "$(dirname "$0")/"

function clear_after_tests()
{
  docker compose down
}

# Prevent double building in docker-compose
docker buildx build --build-arg=PROFILE=ci --load ../../ --tag=qdrant_consensus
docker compose down --volumes
docker compose up -d --force-recreate
trap clear_after_tests EXIT

# Wait for service to start
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' localhost:6533)" != "200" ]]; do
  sleep 1;
done

python3 create_collection_and_check.py test_collection 6433 6333 6533
python3 insert_points.py test_collection 6333
python3 check_points.py test_collection 6433 6333

# Restarting
docker compose stop
docker compose up -d

# Wait for service to start
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' localhost:6533)" != "200" ]]; do
  sleep 1;
done

# Able to create collection after restart
python3 create_collection_and_check.py test_collection_1 6433 6333 6533
# Points from the 1st collection can be retrieved
python3 check_points.py test_collection 6433 6333

curl -X DELETE "http://127.0.0.1:6333/collections/test_collection_1" \
  -H 'Content-Type: application/json' \
  --fail -s

# Transfer shards away from 6433 and disconnect in from cluster
python3 downscale_cluster.py test_collection 6433 6333 6533

# Check points after downscale
python3 check_points.py test_collection 6333
