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

import fcntl
import json
import os
import pathlib
import time

import pytest
import requests

from .fixtures import create_collection, upsert_random_points
from .utils import *

COLLECTION_NAME = "test_dirty_shard"
N_PEERS = 3

PANIC_MESSAGE = "Can't initialize consensus"
DIRTY_SHARD_LOG = "is not fully initialized - loading as dummy shard"

# Directories that `wal::Wal::open` locks via `flock(LOCK_EX)` on the directory
# fd. After we SIGKILL a peer and reuse the same `peer_dir`, the new peer will
# panic on startup with `Kind(WouldBlock)` if the kernel has not yet released
# the previous process's flock. `subprocess.Popen.wait()` reaps the zombie but
# under pytest-xdist load there is a small window where flock release lags,
# which manifests as the very flake this test was bumped to 60s for in #9124.
#
# Paths are relative to `peer_dir` and mirror how qdrant lays out storage:
# - `storage/collections_meta_wal/` is locked by `ConsensusOpWal::new`
# - `storage/collections/<name>/<shard_id>/wal/` is locked by `SerdeWal::new`
WAL_LOCK_DIRS = (
    pathlib.Path("storage") / "collections_meta_wal",
    pathlib.Path("storage") / "collections" / COLLECTION_NAME / "0" / "wal",
)


def read_log(log_filename: str) -> str:
    path = pathlib.Path(f"consensus_test_logs/{get_pytest_current_test_name()}/{log_filename}")
    return path.read_text() if path.exists() else ""


def _try_flock(path: pathlib.Path) -> bool:
    """Try to take an exclusive non-blocking flock on `path` (matches the lock
    that `wal::Wal::open` uses via `fs4::FileExt::try_lock`)."""
    if not path.exists():
        # Nothing to lock yet; previous process never created it.
        return True
    fd = os.open(path, os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
        return True
    except OSError:
        return False
    finally:
        os.close(fd)


def wait_for_wal_unlocked(peer_dir: pathlib.Path, timeout: float = 10.0) -> None:
    """Block until every WAL directory under `peer_dir` is unlocked.

    Avoids the startup race where a freshly-restarted peer panics with
    `Kind(WouldBlock)` because the just-killed peer's flock has not yet been
    released by the kernel.
    """
    deadline = time.time() + timeout
    pending = [peer_dir / sub for sub in WAL_LOCK_DIRS]
    while pending:
        pending = [p for p in pending if not _try_flock(p)]
        if not pending:
            return
        if time.time() > deadline:
            raise TimeoutError(
                f"WAL directories still locked after {timeout}s: {pending}"
            )
        time.sleep(0.05)


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
    p = processes.pop(target_idx)
    restart_port = p.p2p_port
    p.kill()
    # `p.kill()` does `proc.wait()`, but the kernel may still hold the WAL
    # directory's flock briefly after reap (more visible under pytest-xdist
    # CPU contention). Wait for it to be released so the next `start_peer`
    # doesn't panic on startup with `Wal error: Can't init WAL: Kind(WouldBlock)`.
    wait_for_wal_unlocked(peer_dirs[target_idx])

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
    sync_log = f"peer_sync_{target_idx}.log"
    sync_uri = start_peer(
        peer_dirs[target_idx],
        sync_log,
        bootstrap_uri,
        port=restart_port,
    )
    online, exit_code = wait_for_peer_online_or_crash(sync_uri, processes[-1])
    if not online:
        log_content = read_log(sync_log)
        pytest.fail(
            f"Sync restart failed to come online (exit code {exit_code}).\n"
            f"Log tail:\n{log_content[-1000:]}"
        )

    # Kill again — the UpdateCollection entry is now committed in its WAL
    p = processes.pop()
    restart_port = p.p2p_port
    p.kill()
    wait_for_wal_unlocked(peer_dirs[target_idx])

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
        port=restart_port,
    )

    online, exit_code = wait_for_peer_online_or_crash(new_uri, processes[-1], timeout=30)

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
