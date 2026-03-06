#!/usr/bin/env bash

# Launches a 3-node Qdrant cluster with TLS on localhost.
# Run gen_certs.sh first (once) to create certificates.
#
# Node 1 (leader):  HTTP 6333, gRPC 6334, P2P 6335
# Node 2:           HTTP 6343, gRPC 6344, P2P 6345
# Node 3:           HTTP 6353, gRPC 6354, P2P 6355

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CERT_DIR="$SCRIPT_DIR/cert"
CONFIG_FILE="$SCRIPT_DIR/tls_config.yaml"
WORK_DIR="$SCRIPT_DIR/work"
BINARY="$PROJECT_ROOT/target/debug/qdrant"

if [[ ! -f "$CERT_DIR/cert.pem" ]]; then
    echo "Certificates not found. Run gen_certs.sh first." >&2
    exit 1
fi

PIDS=()
cleanup() {
    echo ""
    echo "Stopping all nodes..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait
    echo "All nodes stopped."
}
trap cleanup EXIT INT TERM

# Build
echo "Building qdrant..."
cargo build --no-default-features --bin qdrant --manifest-path "$PROJECT_ROOT/Cargo.toml"

# Prepare per-node dirs with tls symlink
for i in 1 2 3; do
    mkdir -p "$WORK_DIR/node$i/storage"
    ln -sfn "$CERT_DIR" "$WORK_DIR/node$i/tls"
done

launch() {
    local name=$1 http=$2 grpc=$3 p2p=$4; shift 4

    echo "Starting $name (HTTP=$http gRPC=$grpc P2P=$p2p)..."
    cd "$WORK_DIR/$name"

    QDRANT__LOG_LEVEL="debug,raft=info,hyper=info,wal=info,h2=info,tower=info,rustls=info" \
    QDRANT__SERVICE__HTTP_PORT="$http" \
    QDRANT__SERVICE__GRPC_PORT="$grpc" \
    QDRANT__SERVICE__STATIC_CONTENT_DIR="$PROJECT_ROOT/static" \
    QDRANT__CLUSTER__P2P__PORT="$p2p" \
    QDRANT__STORAGE__STORAGE_PATH="$WORK_DIR/$name/storage" \
    QDRANT__STORAGE__HNSW_INDEX__MAX_INDEXING_THREADS=1 \
    QDRANT_NUM_CPUS=1 \
    "$BINARY" --config-path "$CONFIG_FILE" --disable-telemetry "$@" \
        > "$WORK_DIR/$name/$name.log" 2>&1 &

    PIDS+=($!)
}

wait_ready() {
    local name=$1 port=$2
    for _ in $(seq 1 30); do
        if curl -sf --cacert "$CERT_DIR/cacert.pem" "https://localhost:$port/readyz" >/dev/null 2>&1; then
            echo "$name is ready."
            return
        fi
        sleep 1
    done
    echo "WARNING: $name did not become ready within 30s"
}

launch node1 6333 6334 6335 --uri "https://localhost:6335"
wait_ready node1 6333

launch node2 6343 6344 6345 --bootstrap "https://localhost:6335" --uri "https://localhost:6345"
launch node3 6353 6354 6355 --bootstrap "https://localhost:6335" --uri "https://localhost:6355"

wait_ready node2 6343
wait_ready node3 6353

echo ""
echo "=== 3-node TLS cluster is running ==="
echo "  Node 1: https://localhost:6333"
echo "  Node 2: https://localhost:6343"
echo "  Node 3: https://localhost:6353"
echo ""
echo "  curl --cacert $CERT_DIR/cacert.pem https://localhost:6333/cluster"
echo ""
echo "Press Ctrl+C to stop."

wait
