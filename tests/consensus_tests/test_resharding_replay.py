"""Resharding start replay reconciles a partial-apply window where the
config.json save crashed after resharding_state.json was persisted."""

import json
import pathlib

import requests

from .assertions import assert_http_ok
from .fixtures import create_collection
from .utils import (
    get_cluster_info,
    get_collection_cluster_info,
    processes,
    start_cluster,
    start_peer,
    wait_collection_exists_and_active_on_all_peers,
    wait_for,
    wait_for_collection_resharding_operations_count,
    wait_for_peer_online,
)

COLLECTION = "test_resharding_replay"


def test_start_resharding_replay_reconciles_pre_config_save_crash(tmp_path: pathlib.Path):
    peer_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3)

    target_idx = 2
    target_dir = peer_dirs[target_idx]
    target_peer_id = get_cluster_info(peer_uris[target_idx])["peer_id"]

    create_collection(peer_uris[0], collection=COLLECTION, shard_number=2, replication_factor=1)
    wait_collection_exists_and_active_on_all_peers(COLLECTION, peer_uris)

    raft_state = target_dir / "storage" / "raft_state.json"
    config = target_dir / "storage" / "collections" / COLLECTION / "config.json"
    new_shard_dir = target_dir / "storage" / "collections" / COLLECTION / "2"

    commit_before = json.loads(raft_state.read_text())["state"]["hard_state"]["commit"]

    resp = requests.post(
        f"{peer_uris[0]}/collections/{COLLECTION}/cluster",
        json={"start_resharding": {"direction": "up", "peer_id": target_peer_id, "shard_key": None}},
    )
    assert_http_ok(resp)
    wait_for_collection_resharding_operations_count(peer_uris[target_idx], COLLECTION, 1)
    wait_for(lambda: new_shard_dir.exists()
             and json.loads(config.read_text())["params"]["shard_number"] == 3)

    p = processes.pop(target_idx)
    restart_port = p.p2p_port
    p.kill()

    # Simulate a crash between resharding_state.json and config.json saves.
    cfg = json.loads(config.read_text())
    cfg["params"]["shard_number"] = 2
    config.write_text(json.dumps(cfg))

    # Force replay of the start_resharding entry.
    state = json.loads(raft_state.read_text())
    state["apply_progress_queue"] = [commit_before + 1, state["state"]["hard_state"]["commit"]]
    raft_state.write_text(json.dumps(state))

    peer_uris[target_idx] = start_peer(target_dir, f"peer_replay_{target_idx}.log", bootstrap_uri, port=restart_port)
    new_uri = peer_uris[target_idx]
    wait_for_peer_online(new_uri)

    # Pre-fix: matching-state early return short-circuits replay; shard 2 never
    # gets re-registered on this peer. Post-fix: replay falls through and
    # reconciles via contains_shard(2)=false → create_replica_set + config save.
    def has_new_shard():
        try:
            info = get_collection_cluster_info(new_uri, COLLECTION)
        except requests.RequestException:
            return False
        return any(s["shard_id"] == 2 for s in info.get("local_shards", []))

    wait_for(has_new_shard)
    assert json.loads(config.read_text())["params"]["shard_number"] == 3

    resp = requests.post(
        f"{peer_uris[0]}/collections/{COLLECTION}/cluster",
        json={"abort_resharding": {}},
    )
    assert_http_ok(resp)
    for uri in peer_uris:
        wait_for_collection_resharding_operations_count(uri, COLLECTION, 0)
