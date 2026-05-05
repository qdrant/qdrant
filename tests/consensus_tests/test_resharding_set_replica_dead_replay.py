"""SetShardReplicaState(Resharding -> Dead) replay must clear resharding_state.

The local `abort_resharding` side-effect inside `Collection::set_shard_replica_state`
is gated on `current_state.is_resharding()` — but `current_state` is captured BEFORE
`ensure_replica_with_state` persists the new state. On replay (after a crash that
persisted the new replica state but didn't run the side-effect), `current_state`
reads back as Dead from disk, so the gate evaluates false and the local
abort_resharding is silently skipped. resharding_state.json then remains stale on
the replaying peer while peers that completed a normal apply have it cleared.

This reproduces the divergence by forging the on-disk state to match a
partial-apply window and forcing the entry to replay.
"""

import json
import pathlib
import shutil

import requests

from .assertions import assert_http_ok
from .fixtures import create_collection, upsert_random_points
from .utils import (
    get_cluster_info,
    get_collection_cluster_info,
    processes,
    start_cluster,
    start_peer,
    wait_collection_exists_and_active_on_all_peers,
    wait_for,
    wait_for_collection_resharding_operations_count,
    wait_for_collection_shard_transfers_count,
    wait_for_peer_online,
)

COLLECTION = "test_resharding_set_replica_dead_replay"


def _all_replicas(info):
    for local in info["local_shards"]:
        yield {**local, "peer_id": info["peer_id"]}
    for remote in info["remote_shards"]:
        yield remote


def _peer_for_shard(info, shard_id):
    return next(r["peer_id"] for r in _all_replicas(info) if r["shard_id"] == shard_id)


def test_set_replica_dead_replay_clears_resharding_state(tmp_path: pathlib.Path):
    peer_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3)

    target_idx = 2
    target_dir = peer_dirs[target_idx]
    peer_ids = [get_cluster_info(uri)["peer_id"] for uri in peer_uris]
    target_peer_id = peer_ids[target_idx]

    # 2 shards (RF=1). Resharding up will add shard 2 on the target peer in
    # `Resharding` state. We then drive a resharding-stream-records transfer
    # from shard 0 to shard 2 on a *non-target* peer so shard 2 gets a
    # second `Resharding` replica there. Without that second replica, the
    # failure-detection path's "last active peer" guard
    # (`add_locally_disabled` -> `active_or_resharding_peers`) refuses to
    # propose `SetShardReplicaState(Dead)` because target holds the only
    # resharding replica, and the bug we're testing never gets exercised.
    create_collection(peer_uris[0], collection=COLLECTION, shard_number=2, replication_factor=1)
    wait_collection_exists_and_active_on_all_peers(COLLECTION, peer_uris)

    raft_state = target_dir / "storage" / "raft_state.json"
    resharding_state = target_dir / "storage" / "collections" / COLLECTION / "resharding_state.json"
    new_shard_dir = target_dir / "storage" / "collections" / COLLECTION / "2"
    new_replica_state = new_shard_dir / "replica_state.json"

    # Trigger resharding up: add shard 2 on target peer.
    resp = requests.post(
        f"{peer_uris[0]}/collections/{COLLECTION}/cluster",
        json={"start_resharding": {"direction": "up", "peer_id": target_peer_id, "shard_key": None}},
    )
    assert_http_ok(resp)

    # Wait for resharding state to be observable on every peer and shard 2 to
    # exist on target with `Resharding` replica state.
    for uri in peer_uris:
        wait_for_collection_resharding_operations_count(uri, COLLECTION, 1)
    wait_for(lambda: new_replica_state.exists())
    wait_for(
        lambda: json.loads(new_replica_state.read_text())["peers"].get(str(target_peer_id))
        == "Resharding"
    )

    # Drive a resharding-stream-records transfer from an existing shard
    # (Active, validation accepts it) to shard 2 on a non-target peer. This
    # creates shard 2 on the receiver with `Resharding` state so shard 2
    # ends up with two resharding replicas. Pick a transfer source on a peer
    # that isn't the target — auto-distribution may put shard 0 on the target
    # peer.
    info = get_collection_cluster_info(peer_uris[0], COLLECTION)
    source_idx = source_peer_id = source_shard = None
    for r in _all_replicas(info):
        replica_peer = r["peer_id"]
        replica_idx = peer_ids.index(replica_peer)
        if replica_idx == target_idx:
            continue
        source_idx = replica_idx
        source_peer_id = replica_peer
        source_shard = r["shard_id"]
        break
    assert source_idx is not None, "no non-target peer holds an existing shard"
    extra_idx = next(i for i in range(3) if i != target_idx and i != source_idx)
    extra_peer_id = peer_ids[extra_idx]
    driver_uri = peer_uris[source_idx]

    resp = requests.post(
        f"{peer_uris[0]}/collections/{COLLECTION}/cluster",
        json={
            "replicate_shard": {
                "from_peer_id": source_peer_id,
                "to_peer_id": extra_peer_id,
                "shard_id": source_shard,
                "to_shard_id": 2,
                "method": "resharding_stream_records",
            }
        },
    )
    assert_http_ok(resp)
    wait_for_collection_shard_transfers_count(peer_uris[0], COLLECTION, 0)

    # Capture the commit just before we trigger the buggy entry. The replay
    # window starts at commit_before + 1.
    commit_before_set = json.loads(raft_state.read_text())["state"]["hard_state"]["commit"]

    # Snapshot target's full collection directory before the deactivation so
    # we can restore it after the natural catchup has dropped shard 2 (which
    # is the correct behavior of `abort_resharding` for resharding-up). We
    # restore the snapshot below to set up the partial-apply forge.
    collection_dir = target_dir / "storage" / "collections" / COLLECTION
    snapshot_dir = tmp_path / "target_collection_snapshot"
    shutil.copytree(collection_dir, snapshot_dir)

    # Kill target peer. Updates to shard 2 will fail on target's Resharding
    # replica; the second peer's Resharding replica keeps the shard "live"
    # in active_or_resharding_peers so the failure handler is permitted to
    # propose `SetShardReplicaState(Resharding -> Dead)` for target.
    p = processes.pop(target_idx)
    restart_port = p.p2p_port
    p.kill()

    # Drive enough updates to land at least one on shard 2 and trigger the
    # deactivation. Use weak ordering with `fail_on_error=False` since some
    # writes targeting shard 2 are expected to fail while target is down.
    upsert_random_points(
        driver_uri,
        num=200,
        collection_name=COLLECTION,
        fail_on_error=False,
        ordering="weak",
    )

    # Wait for the running peers to apply `SetShardReplicaState(Dead)` and the
    # local `abort_resharding` to clear the resharding state on them.
    running_uris = [uri for idx, uri in enumerate(peer_uris) if idx != target_idx]
    for uri in running_uris:
        wait_for_collection_resharding_operations_count(uri, COLLECTION, 0)

    # Restart target so it catches up via raft. With the fix, the natural
    # apply on target also fires the local abort_resharding side-effect, so
    # state converges and shard 2 is dropped on target. We need to wait for
    # this so target's hard_state.commit advances past the SetShardReplicaState
    # entry — that gives us a valid range to set apply_progress_queue over
    # for the replay below.
    peer_uris[target_idx] = start_peer(
        target_dir, f"peer_catchup_{target_idx}.log", bootstrap_uri, port=restart_port
    )
    catchup_uri = peer_uris[target_idx]
    wait_for_peer_online(catchup_uri)
    wait_for_collection_resharding_operations_count(catchup_uri, COLLECTION, 0)
    assert json.loads(resharding_state.read_text()) is None

    # Capture target's advanced commit, then kill again.
    commit_after_apply = json.loads(raft_state.read_text())["state"]["hard_state"]["commit"]
    raft_state_blob = json.loads(raft_state.read_text())
    p = processes.pop(target_idx)
    restart_port = p.p2p_port
    p.kill()

    # Restore the pre-deactivation collection snapshot. This puts back shard 2's
    # directory, replica_state.json (showing target=Resharding), config.json
    # (shard_number=3), and resharding_state.json (Some). Then forge the
    # partial-apply state on top: target's replica becomes Dead — as it would
    # be if `ensure_replica_with_state` had persisted before the crash, while
    # the local abort_resharding side-effect didn't run.
    shutil.rmtree(collection_dir)
    shutil.copytree(snapshot_dir, collection_dir)

    repl_state = json.loads(new_replica_state.read_text())
    repl_state["peers"][str(target_peer_id)] = "Dead"
    new_replica_state.write_text(json.dumps(repl_state))

    # Reset apply_progress_queue to replay every entry from before the
    # SetShardReplicaState through the post-catchup commit. The earlier
    # raft state (from before kill) had a smaller commit than what the
    # cluster has now, so reuse the post-catchup commit value to keep the
    # cluster's view consistent.
    raft_state_blob["apply_progress_queue"] = [
        commit_before_set + 1,
        commit_after_apply,
    ]
    raft_state.write_text(json.dumps(raft_state_blob))

    # Restart and watch the replay.
    peer_uris[target_idx] = start_peer(
        target_dir, f"peer_replay_{target_idx}.log", bootstrap_uri, port=restart_port
    )
    replay_uri = peer_uris[target_idx]
    wait_for_peer_online(replay_uri)

    # Post-fix expectation: the replaying peer reconciles its forged
    # resharding_state to None, matching the rest of the cluster.
    #
    # Pre-fix bug: `is_resharding = current_state.is_resharding()` evaluates
    # false because current_state was reloaded from replica_state.json as Dead,
    # so the local `abort_resharding` is skipped. resharding_state.json stays
    # Some on this peer while peers 0 and 1 have it cleared, and
    # `resharding_operations` in the cluster info diverges.
    wait_for_collection_resharding_operations_count(replay_uri, COLLECTION, 0)

    # All peers must agree — no divergence in resharding_operations.
    for uri in peer_uris:
        info = get_collection_cluster_info(uri, COLLECTION)
        assert not info.get("resharding_operations"), (
            f"Resharding state divergence on peer {uri}: "
            f"resharding_operations={info.get('resharding_operations')!r}"
        )
