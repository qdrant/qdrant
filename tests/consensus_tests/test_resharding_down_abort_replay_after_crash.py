"""A scale-down abort replays after clearing resharding state, before marking a replica Dead.

Failure detection commits SetReplicaState(Dead) for the scaled-down receiver.
CSM clears resharding state before persisting the receiver replica's Dead state.
Crash a follower between those writes and verify that replay completes the replica update
without leaving resharding state on any peer.
"""

import json
import pathlib
import threading
import time

from .fixtures import create_collection, upsert_random_points
from .test_resharding import all_replicas, start_resharding
from .utils import *

COLLECTION = "test_resharding_down_abort_replay_after_crash"


def _resharding_ops(uri) -> int:
    return len(
        get_collection_cluster_info(uri, COLLECTION).get("resharding_operations") or []
    )


def _replica_state(uri, shard_id, peer_id):
    return next(
        (
            r["state"]
            for r in all_replicas(get_collection_cluster_info(uri, COLLECTION))
            if r["shard_id"] == shard_id and r["peer_id"] == peer_id
        ),
        None,
    )


def test_scale_down_abort_replays_after_state_clear_crash(tmp_path: pathlib.Path):
    env = {"QDRANT_STAGING_RESHARDING_CRASH_AFTER_STATE_CLEAR": "1"}
    peer_uris, peer_dirs, _ = start_cluster(tmp_path, 3, extra_env=env)
    skip_if_no_feature(peer_uris[0], "staging")
    wait_for(all_peers_are_voters, peer_uris)
    peer_ids = [get_cluster_info(uri)["peer_id"] for uri in peer_uris]
    peer_procs = list(processes)  # indexed like peers; `processes` is mutated below

    create_collection(
        peer_uris[0], collection=COLLECTION, shard_number=3, replication_factor=2
    )
    wait_collection_exists_and_active_on_all_peers(COLLECTION, peer_uris)
    upsert_random_points(
        peer_uris[0], num=20_000, collection_name=COLLECTION, batch_size=1_000
    )

    assert_http_ok(
        start_resharding(peer_uris[0], COLLECTION, direction="down", shard_key=None)
    )
    for uri in peer_uris:
        wait_for_collection_resharding_operations_count(uri, COLLECTION, 1)

    # Migrate shard 2 -> shard 0 ourselves (driver is off in tests) and let it finish.
    # The receiver's shard-0 replica then sits in ReshardingScaleDown with no in-flight
    # transfer, so killing it later can't trigger the converging transfer-abort path.
    target, surviving = 2, 0
    info = get_collection_cluster_info(peer_uris[0], COLLECTION)
    target_peers = [r["peer_id"] for r in all_replicas(info) if r["shard_id"] == target]
    surviving_peers = [
        r["peer_id"] for r in all_replicas(info) if r["shard_id"] == surviving
    ]
    from_peer, receiver_peer = next(
        (f, t) for f in target_peers for t in surviving_peers if f != t
    )

    assert_http_ok(
        requests.post(
            f"{peer_uris[0]}/collections/{COLLECTION}/cluster",
            json={
                "replicate_shard": {
                    "from_peer_id": from_peer,
                    "to_peer_id": receiver_peer,
                    "shard_id": target,
                    "to_shard_id": surviving,
                    "method": "resharding_stream_records",
                }
            },
        )
    )
    wait_for_collection_shard_transfers_count(peer_uris[0], COLLECTION, 0)
    wait_for(
        lambda: _replica_state(peer_uris[0], surviving, receiver_peer)
        == "ReshardingScaleDown"
    )

    receiver_idx = peer_ids.index(receiver_peer)
    live_idxs = [i for i in range(3) if i != receiver_idx]
    live_ids = {peer_ids[i] for i in live_idxs}

    # Kill the receiver; failing updates to its shard-0 replica make failure detection
    # propose SetReplicaState(Dead), whose gate aborts resharding on the live peers.
    receiver_proc = peer_procs[receiver_idx]
    processes.remove(receiver_proc)
    receiver_proc.kill()

    # Crash the follower, never the leader, and wait out the election the receiver's
    # death may have triggered first. The staging crash fires *inside* the apply of the
    # `Dead` entry while raft messages leave through an async send queue, so a crashing
    # leader takes the append carrying the new commit index down with it: the other live
    # peer is left holding that entry appended but uncommitted, and as one voter out of
    # three it can never commit it — so it would never abort, and the wait below would
    # hang until the peers are restarted.
    def leader_is_live_and_agreed() -> bool:
        leaders = {get_leader(peer_uris[i]) for i in live_idxs}
        return len(leaders) == 1 and leaders <= live_ids

    wait_for(leader_is_live_and_agreed, wait_for_timeout=60)

    survivor_idx = peer_ids.index(get_leader(peer_uris[live_idxs[0]]))
    victim_idx = next(i for i in live_idxs if i != survivor_idx)
    survivor_uri = peer_uris[survivor_idx]

    # Crash this follower after clearing resharding state, before persisting Dead.
    (peer_dirs[victim_idx] / "crash_after_resharding_state_clear").write_text("")

    stop = threading.Event()

    def drive_failing_updates():
        while not stop.is_set():
            try:
                upsert_random_points(
                    survivor_uri,
                    num=100,
                    collection_name=COLLECTION,
                    fail_on_error=False,
                    ordering="weak",
                    timeout=5,
                )
            except requests.RequestException:
                pass
            time.sleep(0.1)

    driver = threading.Thread(target=drive_failing_updates, daemon=True)
    driver.start()

    victim_proc = peer_procs[victim_idx]
    wait_for(lambda: victim_proc.proc.poll() is not None, wait_for_timeout=60)
    stop.set()
    driver.join(timeout=10)
    assert victim_proc.proc.returncode == 1
    victim_proc.kill()  # already exited; frees its ports for the restart
    processes.remove(victim_proc)

    # Prove the crash reached the intended persisted-state boundary.
    collection_dir = peer_dirs[victim_idx] / "storage" / "collections" / COLLECTION
    assert json.loads((collection_dir / "resharding_state.json").read_text()) is None
    replica_state = json.loads(
        (collection_dir / str(surviving) / "replica_state.json").read_text()
    )
    assert replica_state["peers"][str(receiver_peer)] == "ReshardingScaleDown"

    wait_for(lambda: _resharding_ops(survivor_uri) == 0, wait_for_timeout=30)

    # Restart victim first (restores quorum), then receiver. Liveness only — the
    # receiver's replica may still be recovering when the peer comes online.
    for idx in (victim_idx, receiver_idx):
        peer_uris[idx] = start_peer(
            peer_dirs[idx],
            f"peer_restart_{idx}.log",
            survivor_uri,
            port=peer_procs[idx].p2p_port,
        )
        wait_for_peer_online(peer_uris[idx], path="/healthz", wait_for_timeout=60)

    stuck = {}
    for uri in peer_uris:
        try:
            wait_for_collection_resharding_operations_count(
                uri, COLLECTION, 0, wait_for_timeout=10
            )
        except Exception:
            stuck[uri] = _resharding_ops(uri)
    assert not stuck, f"resharding state diverged after restart: {stuck}"

    wait_for(
        lambda: _replica_state(peer_uris[victim_idx], surviving, receiver_peer)
        not in (None, "ReshardingScaleDown")
    )
