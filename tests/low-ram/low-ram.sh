#!/usr/bin/env bash

set -xeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

declare DOCKER_IMAGE_NAME=qdrant-recovery

docker buildx build --build-arg=PROFILE=ci --load ../../ --tag=$DOCKER_IMAGE_NAME

if [[ ! -e storage ]]; then
    git lfs pull
    tar -xf storage.tar.xz
fi

declare OOM_CONTAINER_NAME=qdrant-oom

docker rm -f ${OOM_CONTAINER_NAME} || true

docker run \
    -m 128m \
    -p 127.0.0.1:6333:6333 \
    -p 127.0.0.1:6334:6334 \
    -v $PWD/storage:/qdrant/storage \
    -e QDRANT__STORAGE__HANDLE_COLLECTION_LOAD_ERRORS=true \
    --name ${OOM_CONTAINER_NAME} \
    $DOCKER_IMAGE_NAME || true

declare oom_status_code && oom_status_code=$(docker inspect ${OOM_CONTAINER_NAME} --format='{{.State.ExitCode}}')

if ((${oom_status_code} != "137")); then
    echo "Expected qdrant to OOM" >&2
    exit 1
fi

docker rm -f ${OOM_CONTAINER_NAME}

declare container && container=$(
    docker run --rm -d \
        -m 128m \
        -p 127.0.0.1:6333:6333 \
        -p 127.0.0.1:6334:6334 \
        -v $PWD/storage:/qdrant/storage \
        -e QDRANT__STORAGE__HANDLE_COLLECTION_LOAD_ERRORS=true \
        -e QDRANT_ALLOW_RECOVERY_MODE=true \
        $DOCKER_IMAGE_NAME
)

function cleanup {
    docker stop $container
}

trap cleanup EXIT

# Wait (up to ~30 seconds) for the service to start
declare retry=0
while [[ $(curl -sS localhost:6333 -w ''%{http_code}'' -o /dev/null) != 200 ]]; do
    if ((retry++ < 30)); then
        sleep 1
    else
        echo "Service failed to start in ~30 seconds" >&2
        exit 1
    fi
done

# Wait (up to ~10 seconds) until `low-ram` collection is loaded
declare retry=0
while ! curl -sS --fail-with-body localhost:6333/collections | jq -e '.result.collections | index({"name": "low-ram"})' &>/dev/null; do
    if ((retry++ < 10)); then
        sleep 1
    else
        echo "Collection failed to load in ~10 seconds" >&2
        exit 2
    fi
done

# Check that there's a "dummy" shard log message in service logs
declare RECOVERY_MODE_MSG='Qdrant is loaded in recovery mode'

if ! docker logs "$container" 2>&1 | grep "$RECOVERY_MODE_MSG"; then
    echo "'$RECOVERY_MODE_MSG' log message not found in $container container logs" >&2
    exit 3
fi

# Check that `low-ram` collection initialized as a "dummy" shard
# (e.g., collection info request returns HTTP status 500)
declare status
status="$(curl -sS localhost:6333/collections/low-ram -w ''%{http_code}'' -o /dev/null)"

if ((status != 500)); then
    echo "Collection info request returned an unexpected HTTP status: expected 500, but received $STATUS" >&2
    exit 5
fi

declare EXPECTED_ERROR_MESSAGE='Out-of-Memory'

# Check that curl returns a error about recovery mode
CURL_RESPONSE="$(curl -sS localhost:6333/collections/low-ram)"

if ! echo "$CURL_RESPONSE" | grep "${EXPECTED_ERROR_MESSAGE}"; then
    echo "'${EXPECTED_ERROR_MESSAGE}' error message not found in curl output" >&2
    exit 6
fi


echo "Success"
