#!/usr/bin/env bash
# Start a prebuilt Qdrant binary, wait until it is ready, then run shard snapshot API tests.
# Snapshot storage (local vs s3) is controlled via QDRANT__STORAGE__SNAPSHOTS_CONFIG__* env vars.

set -euo pipefail

cd "$(dirname "$0")/../"

QDRANT_EXECUTABLE="${QDRANT_EXECUTABLE:-./target/debug/qdrant}"
QDRANT_HOST="${QDRANT_HOST:-localhost:6333}"
READY_TIMEOUT_SEC="${READY_TIMEOUT_SEC:-60}"

"$QDRANT_EXECUTABLE" &
PID=$!

clear_after_tests() {
	echo "server is going down"
	kill "$PID" &>/dev/null || :
	wait "$PID" &>/dev/null || :
	echo "END"
}

trap clear_after_tests EXIT

for _ in $(seq 1 "$READY_TIMEOUT_SEC"); do
	if curl --output /dev/null --silent --get --fail "http://$QDRANT_HOST/readyz"; then
		echo "server ready to serve traffic"
		./tests/shard-snapshot-api.sh test-all
		exit 0
	fi
	printf 'waiting for server to start...\n'
	sleep 1
done

echo "server did not become ready within ${READY_TIMEOUT_SEC}s" >&2
exit 1
