"""
A second snapshot recovery of the same shard must be rejected while one is
already running.

Recovering a shard snapshot clears the shard and rewrites it from the snapshot.
Restoring is explicitly *not* cancel safe (`recover_shard_snapshot_impl`) and runs
in a `spawn_blocking` task, so a caller that walks away -- a dropped HTTP request,
or a shard transfer whose `recover` RPC was retried -- cannot stop it. Without
exclusion, the retried recovery calls `clear_local_shard_for_snapshot_recovery`
and wipes the shard directory the previous restore is still writing.

The staging restore delay holds the first recovery inside its restore long enough
for a second request to land on top of it.

Requires the `staging` feature for `QDRANT__STAGING__SHARD_SNAPSHOT_RESTORE_DELAY`:

    cargo build --features staging
    pytest tests/consensus_tests/test_concurrent_shard_recovery.py -s -v
"""

import concurrent.futures
import pathlib

import requests

from .assertions import assert_http_ok
from .fixtures import create_collection, upsert_random_points
from .utils import *

COLLECTION_NAME = "test_collection"
NUM_POINTS = 1_000

# Long enough that the second request certainly lands while the first is inside
# its restore, short enough to keep the test quick.
RESTORE_DELAY_SECS = 15


def test_second_recovery_of_same_shard_is_rejected(tmp_path: pathlib.Path):
    assert_project_root()

    env = {"QDRANT__STAGING__SHARD_SNAPSHOT_RESTORE_DELAY": str(RESTORE_DELAY_SECS)}
    peer_dirs = make_peer_folders(tmp_path, 1)
    peer_uri, _ = start_first_peer(peer_dirs[0], "peer_0_0.log", extra_env=env)
    wait_peer_added(peer_uri)

    create_collection(peer_uri, shard_number=1, replication_factor=1)
    wait_collection_exists_and_active_on_all_peers(COLLECTION_NAME, [peer_uri])
    upsert_random_points(peer_uri, NUM_POINTS, batch_size=100)

    info = get_collection_cluster_info(peer_uri, COLLECTION_NAME)
    shard_id = info["local_shards"][0]["shard_id"]

    # Take a shard snapshot to recover from, so both requests have real work to do.
    r = requests.post(f"{peer_uri}/collections/{COLLECTION_NAME}/shards/{shard_id}/snapshots")
    assert_http_ok(r)
    snapshot_name = r.json()["result"]["name"]

    recover_url = (
        f"{peer_uri}/collections/{COLLECTION_NAME}/shards/{shard_id}/snapshots/recover"
    )

    def recover():
        return requests.put(
            recover_url, json={"location": snapshot_name}, params={"wait": "true"}
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(recover)
        # Give the first request time to take the recovery guard and enter its
        # (delayed) restore before the second one arrives.
        time.sleep(RESTORE_DELAY_SECS / 3)
        second = pool.submit(recover)

        responses = [first.result(timeout=120), second.result(timeout=120)]

    statuses = sorted(r.status_code for r in responses)
    bodies = [r.text for r in responses]

    print(f"\nrecovery response statuses: {statuses}")

    assert statuses == [200, 503], (
        f"expected one recovery to succeed and the overlapping one to be rejected "
        f"with 503, got {statuses}: {bodies}"
    )

    rejected = next(r for r in responses if r.status_code == 503)
    assert "already being recovered" in rejected.text, (
        f"503 should explain the conflict, got: {rejected.text}"
    )

    # The surviving recovery must leave the shard intact.
    r = requests.post(
        f"{peer_uri}/collections/{COLLECTION_NAME}/points/count", json={"exact": True}
    )
    assert_http_ok(r)
    assert r.json()["result"]["count"] == NUM_POINTS
