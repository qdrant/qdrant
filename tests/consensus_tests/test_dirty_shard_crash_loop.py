"""
Regression test for dirty shard crash loop.

A node with a dirty shard (.initializing flag) must boot successfully even
when consensus has pending UpdateCollection entries to replay at init.

The UpdateCollection path calls recreate_optimizers_blocking() which invokes
on_optimizer_config_update() on every shard, including DummyShard. This must
not return a fatal error, otherwise apply_entries() at init panics with:

  "Can't initialize consensus: Failed to apply collection meta operation entry"
  "Dirty shard - shard is not fully initialized"
"""

import json
import pathlib
import time

import pytest
import requests

from consensus_tests.fixtures import create_collection, upsert_random_points
from consensus_tests.utils import (
    get_collection_cluster_info,
    get_pytest_current_test_name,
    processes,
    start_cluster,
    start_peer,
    wait_collection_exists_and_active_on_all_peers,
    wait_for_peer_online,
)

COLLECTION_NAME = "test_dirty_shard"
N_PEERS = 3

PANIC_MESSAGE = "Can't initialize consensus"
DIRTY_SHARD_LOG = "is not fully initialized - loading as dummy shard"


def read_log(log_filename: str) -> str:
    path = pathlib.Path(f"consensus_test_logs/{get_pytest_current_test_name()}/{log_filename}")
    return path.read_text() if path.exists() else ""


def wait_for_peer_online_or_crash(uri: str, peer_process, timeout=30):
    """Wait for the peer to come online or detect process exit."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        exit_code = peer_process.proc.poll()
        if exit_code is not None:
            return False, exit_code
        try:
            r = requests.get(f"{uri}/readyz", timeout=1)
            if r.status_code == 200:
                return True, None
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.2)
    return False, None


def test_dirty_shard_survives_update_collection(tmp_path: pathlib.Path):
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(
        peer_api_uris[0],
        collection=COLLECTION_NAME,
        shard_number=1,
        replication_factor=N_PEERS,
    )
    wait_collection_exists_and_active_on_all_peers(COLLECTION_NAME, peer_api_uris)
    upsert_random_points(peer_api_uris[0], 100, collection_name=COLLECTION_NAME)

    target_idx = N_PEERS - 1
    cluster_info = get_collection_cluster_info(peer_api_uris[target_idx], COLLECTION_NAME)
    local_shard_ids = [s["shard_id"] for s in cluster_info["local_shards"]]
    assert local_shard_ids, "Target node has no local shards"

    # Kill target and record its commit index before issuing UpdateCollection
    processes.pop(target_idx).kill()

    raft_state_path = peer_dirs[target_idx] / "storage" / "raft_state.json"
    raft_state = json.loads(raft_state_path.read_text())
    commit_before = raft_state["state"]["hard_state"]["commit"]

    r = requests.patch(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}",
        json={"optimizers_config": {"indexing_threshold": 30000}},
        timeout=10,
    )
    assert r.status_code == 200, f"UpdateCollection failed: {r.text}"

    # Restart target briefly (no dirty flag) so it syncs the UpdateCollection entry
    sync_uri = start_peer(
        peer_dirs[target_idx],
        f"peer_sync_{target_idx}.log",
        bootstrap_uri,
    )
    wait_for_peer_online(sync_uri)

    # Kill again — the UpdateCollection entry is now committed in its WAL
    processes.pop().kill()

    # Create .initializing flags to simulate crash mid-transfer
    collection_path = peer_dirs[target_idx] / "storage" / "collections" / COLLECTION_NAME
    for shard_id in local_shard_ids:
        (collection_path / f"shard_{shard_id}.initializing").touch()

    # Roll back apply_progress_queue so the UpdateCollection entry appears unapplied.
    # This forces apply_entries() to run during Consensus::run() init.
    raft_state = json.loads(raft_state_path.read_text())
    commit_after = raft_state["state"]["hard_state"]["commit"]
    raft_state["apply_progress_queue"] = [commit_before + 1, commit_after]
    raft_state_path.write_text(json.dumps(raft_state))

    # Restart — apply_entries() at init replays UpdateCollection on the DummyShard
    restart_log = f"peer_restart_{target_idx}.log"
    new_uri = start_peer(
        peer_dirs[target_idx],
        restart_log,
        bootstrap_uri,
    )

    online, exit_code = wait_for_peer_online_or_crash(new_uri, processes[-1])

    if not online:
        log_content = read_log(restart_log)
        pytest.fail(
            f"Node failed to come online (exit code {exit_code}).\n"
            f"Log tail:\n{log_content[-1000:]}"
        )

    # Verify dirty shard was detected and loaded as DummyShard
    log_content = read_log(restart_log)
    assert DIRTY_SHARD_LOG in log_content, \
        f"Expected dirty shard detection in log, got:\n{log_content[-500:]}"
    assert PANIC_MESSAGE not in log_content, \
        f"Node panicked during consensus replay:\n{log_content[-500:]}"

    # Flags still present — shard remains a DummyShard
    for shard_id in local_shard_ids:
        assert (collection_path / f"shard_{shard_id}.initializing").exists()

    # Data still readable via healthy replicas
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/scroll",
        json={"limit": 10},
        timeout=10,
    )
    assert r.status_code == 200
    assert len(r.json()["result"]["points"]) > 0
