"""Killing the source peer mid-resharding-transfer must not leave the cluster stuck.

Expected to FAIL until the bug is fixed: the transfer driver dies with the
source process, no Abort reaches consensus, and ReshardingScaleDown persists.
"""

import pathlib
import time

import requests

from .assertions import assert_http_ok
from .fixtures import create_collection, upsert_random_points
from .utils import *

COLLECTION = "test_resharding_source_kill"
N_PEERS = 3
N_SHARDS = 2
N_REPLICA = 2


def _all_replicas(info):
    for local in info["local_shards"]:
        yield {**local, "peer_id": info["peer_id"]}
    for remote in info["remote_shards"]:
        yield remote


def _find_shard_peers(info, shard_id):
    """Return (peer_id, state) pairs for all replicas of a shard."""
    peers = []
    for r in _all_replicas(info):
        if r["shard_id"] == shard_id:
            peers.append((r["peer_id"], r.get("state", "Unknown")))
    return peers


def _has_resharding_scale_down(peer_uri):
    """True if any local shard is in ReshardingScaleDown state."""
    info = get_collection_cluster_info(peer_uri, COLLECTION)
    for shard in info.get("local_shards", []):
        if shard.get("state") == "ReshardingScaleDown":
            return True
    return False


def _resharding_count(peer_uri):
    info = get_collection_cluster_info(peer_uri, COLLECTION)
    ops = info.get("resharding_operations", [])
    return len(ops)


def _transfer_count(peer_uri):
    info = get_collection_cluster_info(peer_uri, COLLECTION)
    return len(info.get("shard_transfers", []))


def test_resharding_source_kill_aborts_resharding(tmp_path: pathlib.Path):
    """Resharding-down + kill source mid-transfer => resharding must abort.

    Expected to FAIL on current codebase (demonstrates the stuck-state bug).
    """
    assert_project_root()

    env = {
        "QDRANT__STORAGE__OPTIMIZERS__INDEXING_THRESHOLD_KB": "0",
    }

    peer_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, extra_env=env)

    peer_ids = [get_cluster_info(uri)["peer_id"] for uri in peer_uris]

    create_collection(
        peer_uris[0],
        collection=COLLECTION,
        shard_number=N_SHARDS,
        replication_factor=N_REPLICA,
    )
    wait_collection_exists_and_active_on_all_peers(COLLECTION, peer_uris)

    # Make transfer non-trivial
    upsert_random_points(peer_uris[0], num=2000, collection_name=COLLECTION)

    wait_for_all_peers_versions(peer_uris)

    info = get_collection_cluster_info(peer_uris[0], COLLECTION)

    # Pick a shard-1 holder as the resharding target
    shard1_peers = [r["peer_id"] for r in _all_replicas(info) if r["shard_id"] == 1]
    resharding_peer_id = shard1_peers[0]
    resharding_peer_idx = peer_ids.index(resharding_peer_id)

    # Start resharding-down
    resp = requests.post(
        f"{peer_uris[0]}/collections/{COLLECTION}/cluster",
        json={
            "start_resharding": {
                "direction": "down",
                "peer_id": resharding_peer_id,
                "shard_key": None,
            }
        },
    )
    assert_http_ok(resp)

    wait_for_collection_resharding_operations_count(peer_uris[0], COLLECTION, 1)

    # Wait for ReshardingScaleDown to appear
    def resharding_scale_down_exists():
        for uri in peer_uris:
            try:
                if _has_resharding_scale_down(uri):
                    return True
            except Exception:
                pass
        return False

    wait_for(resharding_scale_down_exists, wait_for_timeout=30)

    # Find transfer source (shard-1 holder != resharding peer) and destination
    info = get_collection_cluster_info(peer_uris[0], COLLECTION)

    source_peer_id = None
    source_idx = None
    for r in _all_replicas(info):
        if r["shard_id"] == 1 and r["peer_id"] != resharding_peer_id:
            source_peer_id = r["peer_id"]
            source_idx = peer_ids.index(source_peer_id)
            break

    if source_peer_id is None:
        # All shard-1 replicas on resharding peer — use it as source
        source_peer_id = resharding_peer_id
        source_idx = resharding_peer_idx

    dest_peer_id = None
    for r in _all_replicas(info):
        if r["shard_id"] == 0 and r.get("state") == "ReshardingScaleDown":
            dest_peer_id = r["peer_id"]
            break

    if dest_peer_id is None:
        for r in _all_replicas(info):
            if r["shard_id"] == 0 and r["peer_id"] != source_peer_id:
                dest_peer_id = r["peer_id"]
                break

    assert dest_peer_id is not None, "No destination peer found"

    # Start resharding_stream_records transfer: shard 1 -> shard 0
    resp = requests.post(
        f"{peer_uris[0]}/collections/{COLLECTION}/cluster",
        json={
            "replicate_shard": {
                "from_peer_id": source_peer_id,
                "to_peer_id": dest_peer_id,
                "shard_id": 1,
                "to_shard_id": 0,
                "method": "resharding_stream_records",
            }
        },
    )
    assert_http_ok(resp)

    # Let the transfer start
    time.sleep(2)
    assert _transfer_count(peer_uris[0]) > 0, "Transfer not started"

    # Kill the source peer while transfer is in-flight
    p = processes.pop(source_idx)
    restart_port = p.p2p_port
    p.kill()

    time.sleep(5)

    surviving_uris = [uri for idx, uri in enumerate(peer_uris) if idx != source_idx]

    # Drive writes to trigger failure detection
    for _ in range(5):
        try:
            upsert_random_points(
                surviving_uris[0],
                num=50,
                collection_name=COLLECTION,
                fail_on_error=False,
                ordering="weak",
                timeout=5,
            )
        except requests.RequestException:
            pass
        time.sleep(1)

    # Restart the source peer
    peer_uris[source_idx] = start_peer(
        peer_dirs[source_idx],
        f"peer_restarted_{source_idx}.log",
        bootstrap_uri,
        port=restart_port,
    )
    wait_for_peer_online(peer_uris[source_idx])

    time.sleep(5)

    # After restart, resharding should be aborted and state cleaned up

    wait_for(
        lambda: _transfer_count(surviving_uris[0]) == 0,
        wait_for_timeout=60,
        wait_for_interval=2,
    )

    # BUG: on current codebase this times out — resharding state persists forever
    try:
        wait_for(
            lambda: all(_resharding_count(uri) == 0 for uri in peer_uris),
            wait_for_timeout=60,
            wait_for_interval=2,
        )
    except Exception:
        print("\n=== RESHARDING STATE STUCK (BUG REPRODUCED) ===")
        for i, uri in enumerate(peer_uris):
            try:
                info = get_collection_cluster_info(uri, COLLECTION)
                print(f"Peer {i}: resharding={info.get('resharding_operations', [])}, "
                      f"transfers={info.get('shard_transfers', [])}")
            except Exception as e:
                print(f"Peer {i}: ERROR - {e}")
        print("=== END DIAGNOSTIC ===\n")
        raise AssertionError(
            "Resharding state not cleaned up after source kill + restart"
        )

    for uri in peer_uris:
        assert not _has_resharding_scale_down(uri), (
            f"Peer {uri} still in ReshardingScaleDown"
        )
