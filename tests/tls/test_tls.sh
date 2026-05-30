#!/usr/bin/env bash

set -e

# Ensure current path script dir
cd "$(dirname "$0")/"

CERT_DIR="$PWD/cert"

function clear_after_tests()
{
    docker compose down --timeout 20
}

function run_with_retry() 
{
    set +e
    for i in $(seq 1 $1)
    do
        RET=$($2)
        if [ "$?" == 0 ]
        then
            set -e
            return 0
        fi

        printf "Wait before retry\n"
        sleep 10
    done

    printf "ERROR: %s\n" "$3" 1>&2

    set -e
    return 1
}

# Prevent double building in docker-compose
docker buildx build --build-arg=PROFILE=ci --load ../../ --tag=qdrant_tls
docker compose down --volumes
docker compose up -d --force-recreate
trap clear_after_tests EXIT

# Wait for service to start and test http and grpc endpoints with TLS
for node in 1 2
do
    run_with_retry 5 "docker run --rm --network=tls_qdrant -v $CERT_DIR:/tls_path curlimages/curl --cacert /tls_path/cacert.pem --cert /tls_path/cert.pem --key /tls_path/key.pem https://node$node.qdrant:6333/telemetry" "Failed to GET /telemetry of node$node"
    run_with_retry 5 "docker run --rm --network=tls_qdrant -v $CERT_DIR:/tls_path -v ${PWD}/../../lib/api/src/grpc/proto:/proto fullstorydev/grpcurl -cacert /tls_path/cacert.pem -import-path /proto -proto qdrant.proto -d {} node$node.qdrant:6334 qdrant.Qdrant/HealthCheck" "Failed to perform health check via grpc of node$node"
done

printf "Client TLS connection OK\n"

# Verify both nodes are in cluster
CLUSTER_INFO=$(docker run --rm --network=tls_qdrant -v $CERT_DIR:/tls_path curlimages/curl --cacert /tls_path/cacert.pem --cert /tls_path/cert.pem --key /tls_path/key.pem https://node1.qdrant:6333/cluster)
for node in 1 2
do
    grep -q "{\"uri\":\"https://node$node.qdrant:6335/\"}" <<< "$CLUSTER_INFO"
done

printf "Internodal TLS connection OK\n"
