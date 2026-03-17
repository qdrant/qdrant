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
import os
import pathlib
import time

import requests

from consensus_tests.fixtures import create_collection, upsert_random_points
from consensus_tests.utils import (
    get_collection_cluster_info,
    start_cluster,
    processes,
    start_peer,
    wait_for_peer_online,
    wait_collection_exists_and_active_on_all_peers,
)

COLLECTION_NAME = "test_dirty_shard"
N_PEERS = 3

PANIC_MESSAGE = "Can't initialize consensus"
DIRTY_SHARD_LOG = "is not fully initialized - loading as dummy shard"


def get_log_path(log_filename: str) -> str:
    test_name = os.environ.get("PYTEST_CURRENT_TEST").split(":")[-1].split(" ")[0]
    return f"consensus_test_logs/{test_name}/{log_filename}"


def read_log(log_filename: str) -> str:
    path = get_log_path(log_filename)
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read()
    return ""


def wait_for_peer_online_or_crash(uri: str, proc, timeout=30):
    """
    Wait for the peer to come online or the process to exit/become unresponsive.
    Sometimes the main thread panics but gRPC threads keep the process alive,
    so we can't rely on process exit — check the log for the panic message.
    """
    start = time.time()
    while time.time() - start < timeout:
        exit_code = proc.poll()
        if exit_code is not None:
            return False, exit_code
        try:
            r = requests.get(f"{uri}/readyz", timeout=1)
            if r.status_code == 200:
                return True, None
        except requests.exceptions.ConnectionError:
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
    assert len(local_shard_ids) > 0

    # Kill target and record its commit index before issuing UpdateCollection
    processes.pop(target_idx).kill()

    raft_state_path = peer_dirs[target_idx] / "storage" / "raft_state.json"
    with open(raft_state_path, "r") as f:
        commit_before = json.load(f)["state"]["hard_state"]["commit"]

    r = requests.patch(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}",
        json={"optimizers_config": {"indexing_threshold": 30000}},
    )
    assert r.status_code == 200, f"UpdateCollection failed: {r.text}"

    # Restart target briefly (no dirty flag) so it syncs the UpdateCollection entry
    sync_uri = start_peer(
        peer_dirs[target_idx],
        f"peer_sync_{target_idx}.log",
        bootstrap_uri,
    )
    wait_for_peer_online(sync_uri)

    # Kill again — the UpdateCollection entry is now committed and applied in its WAL
    processes.pop().kill()

    # Create .initializing flag to simulate crash mid-transfer
    collection_path = peer_dirs[target_idx] / "storage" / "collections" / COLLECTION_NAME
    for shard_id in local_shard_ids:
        (collection_path / f"shard_{shard_id}.initializing").touch()

    # Roll back apply_progress_queue in raft_state.json so ALL entries received during
    # the sync restart appear unapplied. The UpdateCollection entry is among them.
    # So that entries committed in WAL but unapplied
    # at startup → apply_entries() runs during Consensus::run() init.
    with open(raft_state_path, "r") as f:
        raft_state = json.load(f)

    commit_after = raft_state["state"]["hard_state"]["commit"]
    raft_state["apply_progress_queue"] = [commit_before + 1, commit_after]

    with open(raft_state_path, "w") as f:
        json.dump(raft_state, f)

    # Restart — apply_entries() at init finds the unapplied UpdateCollection entry
    # and calls on_optimizer_config_update() on every shard, including the DummyShard.
    restart_log = f"peer_restart_{target_idx}.log"
    new_uri = start_peer(
        peer_dirs[target_idx],
        restart_log,
        bootstrap_uri,
    )
    proc = processes[-1].proc

    online, exit_code = wait_for_peer_online_or_crash(new_uri, proc)

    if not online:
        # Check the log for the panic.
        log_content = read_log(restart_log)
        assert False, \
            f"Node failed to come online (exit code {exit_code}).\n" \
            f"Log tail:\n{log_content[-1000:]}"

    # Verify dirty shard was detected and loaded as DummyShard
    log_content = read_log(restart_log)
    assert DIRTY_SHARD_LOG in log_content, \
        f"Expected dirty shard detection in log, got:\n{log_content[-500:]}"
    assert PANIC_MESSAGE not in log_content, \
        f"Node panicked during consensus replay:\n{log_content[-500:]}"

    # Flags still present — shard is a DummyShard, not a real shard
    for shard_id in local_shard_ids:
        assert (collection_path / f"shard_{shard_id}.initializing").exists()

    # Data still readable via healthy replicas
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/scroll",
        json={"limit": 10},
    )
    assert r.status_code == 200
    assert len(r.json()["result"]["points"]) > 0