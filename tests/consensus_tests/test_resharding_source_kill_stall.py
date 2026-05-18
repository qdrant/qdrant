"""Resharding transfer stalls when the source peer is killed mid-transfer.

Reproduces the bug observed in chaos-testing-three (incidents 2026-04-18,
2026-05-01, 2026-05-09, 2026-05-11):

1. A resharding-down operation begins, setting a target shard replica to
   `ReshardingScaleDown` state.
2. A `resharding_stream_records` transfer starts from source to destination.
3. The source peer is killed while the transfer is in progress.
4. The transfer task silently dies with the source process — no Abort is
   proposed to consensus.
5. After the source restarts, the transfer is gone from consensus state but
   `ReshardingScaleDown` persists and no abort_resharding fires.

Expected behavior: after the source restarts and recovers, the system should
detect the orphaned/failed resharding state and either abort the resharding or
retry the transfer. The `ReshardingScaleDown` state should not persist forever.
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
    """Return list of (peer_id, state) for all replicas of a given shard."""
    peers = []
    for r in _all_replicas(info):
        if r["shard_id"] == shard_id:
            peers.append((r["peer_id"], r.get("state", "Unknown")))
    return peers


def _has_resharding_scale_down(peer_uri):
    """Check if any replica on a peer is in ReshardingScaleDown state."""
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
    """Kill the source peer during a resharding transfer and verify recovery.

    The test starts a resharding-down operation, initiates the point migration
    transfer, kills the source peer while the transfer is in flight, restarts
    it, and then asserts that the system eventually cleans up:
    - The resharding operation should be aborted (resharding_operations == 0)
    - No replica should remain in ReshardingScaleDown state

    This test is expected to FAIL on the current codebase, demonstrating the
    bug where killing the source during a resharding transfer leaves the
    cluster in a permanently stuck state.
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

    # Insert enough points to make the transfer take some time
    upsert_random_points(peer_uris[0], num=2000, collection_name=COLLECTION)

    wait_for_all_peers_versions(peer_uris)

    # Identify which peer holds a replica of shard 0 — this will be the
    # "destination" of the resharding-down migration (its shard 0 replica
    # will enter ReshardingScaleDown).
    info = get_collection_cluster_info(peer_uris[0], COLLECTION)

    # Find a peer that holds shard 1 (the shard being removed in resharding-down)
    # and also holds shard 0. This peer's shard 0 will be the resharding target.
    # The peer that holds shard 1 but drives the transfer from shard 1 is our
    # "source" to kill.
    shard1_peers = []
    for r in _all_replicas(info):
        if r["shard_id"] == 1:
            shard1_peers.append(r["peer_id"])

    # Pick a peer that has shard 1 as the resharding target peer
    # (resharding-down removes shard 1 and migrates its data into shard 0)
    resharding_peer_id = shard1_peers[0]
    resharding_peer_idx = peer_ids.index(resharding_peer_id)

    # Start resharding down — this will remove shard 1 and migrate its
    # points into shard 0 on the resharding peer.
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

    # Wait for resharding to start
    wait_for_collection_resharding_operations_count(peer_uris[0], COLLECTION, 1)

    # Wait until we see at least one shard in ReshardingScaleDown state,
    # indicating the migration transfer setup has begun.
    def resharding_scale_down_exists():
        for uri in peer_uris:
            try:
                if _has_resharding_scale_down(uri):
                    return True
            except Exception:
                pass
        return False

    wait_for(resharding_scale_down_exists, wait_for_timeout=30)

    # Now start the actual point migration transfer. We need to find the
    # source shard and destination shard for the transfer.
    # In resharding-down, the removed shard (shard 1) data is transferred
    # into shard 0 on the target peer.
    info = get_collection_cluster_info(peer_uris[0], COLLECTION)

    # Find a peer that holds shard 1 and is NOT the resharding target —
    # this will be our transfer source that we'll kill.
    source_peer_id = None
    source_idx = None
    for r in _all_replicas(info):
        if r["shard_id"] == 1 and r["peer_id"] != resharding_peer_id:
            source_peer_id = r["peer_id"]
            source_idx = peer_ids.index(source_peer_id)
            break

    if source_peer_id is None:
        # All shard 1 replicas are on the resharding peer — use the resharding
        # peer itself as source (it holds shard 1 too).
        source_peer_id = resharding_peer_id
        source_idx = resharding_peer_idx

    # Find a peer that holds shard 0 as the migration destination
    dest_peer_id = None
    for r in _all_replicas(info):
        if r["shard_id"] == 0 and r.get("state") == "ReshardingScaleDown":
            dest_peer_id = r["peer_id"]
            break

    if dest_peer_id is None:
        # Fallback: pick any peer with shard 0 that's not the source
        for r in _all_replicas(info):
            if r["shard_id"] == 0 and r["peer_id"] != source_peer_id:
                dest_peer_id = r["peer_id"]
                break

    assert dest_peer_id is not None, "Could not find destination peer for migration"

    # Initiate the resharding_stream_records transfer: shard 1 -> shard 0
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

    # Wait briefly for the transfer to actually start (consensus must commit
    # the TransferShard(Start(...)) entry and the driver must begin streaming).
    time.sleep(2)

    # Verify the transfer is in progress
    assert _transfer_count(peer_uris[0]) > 0, (
        "Transfer should be in progress before killing source"
    )

    # === KILL THE SOURCE PEER ===
    # This is the critical moment: the transfer driver is running on the
    # source peer. Killing it should eventually trigger an abort, but the
    # bug is that it doesn't.
    p = processes.pop(source_idx)
    restart_port = p.p2p_port
    p.kill()

    # Give the remaining peers time to detect the dead source
    time.sleep(5)

    # The surviving peers should mark the source's shards as Dead
    # and eventually the transfer should be aborted.
    # On a surviving peer, verify the transfer disappeared (it should
    # either finish failing or be cleaned up by sync_local_state).
    surviving_uris = [uri for idx, uri in enumerate(peer_uris) if idx != source_idx]

    # Drive some writes to trigger failure detection on the dead source's shards
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

    # Wait for the restarted peer to catch up with consensus
    time.sleep(5)

    # === ASSERTIONS ===
    # After source restart and recovery, the resharding should be aborted
    # and ReshardingScaleDown state should be cleared.

    # Wait for the transfer to be gone
    wait_for(
        lambda: _transfer_count(surviving_uris[0]) == 0,
        wait_for_timeout=60,
        wait_for_interval=2,
    )

    # This is the key assertion that exposes the bug:
    # The resharding operation should eventually be aborted.
    # On the current codebase, this will timeout because the resharding
    # state persists forever.
    try:
        wait_for(
            lambda: all(_resharding_count(uri) == 0 for uri in peer_uris),
            wait_for_timeout=60,
            wait_for_interval=2,
        )
    except Exception:
        # Print diagnostic info on failure
        print("\n=== RESHARDING STATE STUCK (BUG REPRODUCED) ===")
        for i, uri in enumerate(peer_uris):
            try:
                info = get_collection_cluster_info(uri, COLLECTION)
                print(f"Peer {i} ({uri}):")
                print(f"  resharding_operations: {info.get('resharding_operations', [])}")
                print(f"  shard_transfers: {info.get('shard_transfers', [])}")
                for shard in info.get("local_shards", []):
                    print(f"  shard {shard['shard_id']}: state={shard.get('state')}")
            except Exception as e:
                print(f"Peer {i} ({uri}): ERROR - {e}")
        print("=== END DIAGNOSTIC ===\n")
        raise AssertionError(
            "BUG: Resharding state was not cleaned up after source peer kill + restart. "
            "The ReshardingScaleDown state persists forever because no abort was proposed "
            "when the transfer driver died with the source process."
        )

    # Verify no replica is stuck in ReshardingScaleDown
    for uri in peer_uris:
        assert not _has_resharding_scale_down(uri), (
            f"Peer {uri} still has a replica in ReshardingScaleDown state "
            "after resharding should have been aborted"
        )
