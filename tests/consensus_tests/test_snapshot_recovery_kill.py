import threading
import time
from pathlib import Path

import pytest
import requests

from .assertions import assert_http_ok
from .fixtures import create_collection, upsert_random_points
from .utils import (
    assert_project_root,
    every_test,
    processes,
    start_cluster,
    start_peer,
    wait_collection_exists_and_active_on_all_peers,
    wait_for_peer_online, get_uri,
)

COLLECTION = "test_collection"


def get_shard_states(url: str) -> dict[int, str]:
    try:
        r = requests.get(f"{url}/collections/{COLLECTION}/cluster", timeout=2)
        return {s["shard_id"]: s["state"] for s in r.json()["result"]["local_shards"]} if r.ok else {}
    except Exception:
        return {}


def kill_peer(url: str) -> bool:
    for p in processes:
        if get_uri(p.http_port) == url:
            p.kill()
            processes.remove(p)
            return True
    return False


class PartialMonitor(threading.Thread):
    """Monitor for Partial state and kill target peer when detected."""

    def __init__(self, target_url: str):
        super().__init__(daemon=True)
        self.target_url = target_url
        self.killed = False
        self._stop_event = threading.Event()

    def run(self):
        while not self._stop_event.is_set():
            states = set(get_shard_states(self.target_url).values())
            if "Partial" in states or "ManualRecovery" in states:
                self.killed = kill_peer(self.target_url)
                return
            else:
                time.sleep(0.05)

    def stop(self):
        self._stop_event.set()
        self.join(timeout=2)


def test_snapshot_restore_kill_during_partial(tmp_path: Path, every_test):
    assert_project_root()

    env = {
        "QDRANT__LOG_LEVEL": "DEBUG",
        "QDRANT__STAGING__SNAPSHOT_RECOVERY_DELAY": "5.0",
    }

    peer_urls, peer_dirs, bootstrap = start_cluster(tmp_path, 3, port_seed=23000, extra_env=env)
    create_collection(peer_urls[0], shard_number=3, replication_factor=2)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION, peer_api_uris=peer_urls)
    upsert_random_points(peer_urls[0], 10_000, batch_size=100)

    r = requests.post(f"{peer_urls[0]}/collections/{COLLECTION}/snapshots")
    assert_http_ok(r)
    snapshot_url = f"{peer_urls[0]}/collections/{COLLECTION}/snapshots/{r.json()['result']['name']}"

    # Kill target peer
    recovery_url = peer_urls[2]
    recovery_dir = Path(peer_dirs[2])
    recovery_port = processes[2].http_port

    monitor = PartialMonitor(recovery_url)
    monitor.start()

    requests.put(f"{recovery_url}/collections/{COLLECTION}/snapshots/recover?wait=false", json={"location": snapshot_url})

    # Wait for Partial detection
    monitor.join(timeout=10)
    monitor.stop()

    if not monitor.killed:
        kill_peer(recovery_url)
        pytest.fail("Partial state not detected")

    # Restart again
    restarted = start_peer(recovery_dir, "peer_restarted.log", bootstrap, port=recovery_port, extra_env=env)

    # This checks `/readyz` which is supposed to return 200, as there will be no automatic recovery
    wait_for_peer_online(restarted)
