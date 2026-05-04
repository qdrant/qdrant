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
    wait_for_peer_online,
)

COLLECTION = "test_resharding_set_replica_dead_replay"


def test_set_replica_dead_replay_clears_resharding_state(tmp_path: pathlib.Path):
    peer_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3)

    target_idx = 2
    target_dir = peer_dirs[target_idx]
    target_peer_id = get_cluster_info(peer_uris[target_idx])["peer_id"]

    # Initial 2 shards on peer 0 / peer 1 (RF=1). Resharding up will add shard 2
    # on peer 2 (target), placing it in `Resharding` replica state. We need a
    # non-target peer to hold the shards that don't move, so updates can be
    # accepted and routed; we need the new shard's only replica to be on a peer
    # we can kill so that updates to it fail and trigger
    # `SetShardReplicaState(Resharding -> Dead)`.
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

    # Snapshot the on-disk state of the resharding key. We'll forge it back in
    # later to simulate `ensure_replica_with_state` persisted the new replica
    # state but local `abort_resharding` didn't run before the crash.
    initial_resharding_state = json.loads(resharding_state.read_text())
    assert initial_resharding_state is not None, "resharding_state.json must hold Some(...)"

    # Capture the commit just before we trigger the buggy entry. The replay
    # window starts at commit_before + 1.
    commit_before_set = json.loads(raft_state.read_text())["state"]["hard_state"]["commit"]

    # Kill target peer to force update failures on shard 2 (its only replica is
    # `Resharding` on target). The first update routed to shard 2 will fail and
    # the failure handler proposes `SetShardReplicaState(Resharding -> Dead)`.
    p = processes.pop(target_idx)
    restart_port = p.p2p_port
    p.kill()

    # Drive enough updates to land at least one on shard 2 and trigger the
    # deactivation. Use weak ordering with `fail_on_error=False` since some
    # writes targeting shard 2 are expected to fail while target is down.
    upsert_random_points(
        peer_uris[0],
        num=200,
        collection_name=COLLECTION,
        fail_on_error=False,
        ordering="weak",
    )

    # Wait for the running peers to apply `SetShardReplicaState(Dead)` and the
    # local `abort_resharding` to clear the resharding state on them.
    for uri in [peer_uris[0], peer_uris[1]]:
        wait_for_collection_resharding_operations_count(uri, COLLECTION, 0)

    # Restart target so it catches up via raft. The natural apply flow does
    # NOT trigger the bug — `current_state` is captured as `Resharding` before
    # `ensure_replica_with_state` runs, so the local `abort_resharding`
    # side-effect fires and resharding_state converges to None on this peer.
    peer_uris[target_idx] = start_peer(
        target_dir, f"peer_catchup_{target_idx}.log", bootstrap_uri, port=restart_port
    )
    catchup_uri = peer_uris[target_idx]
    wait_for_peer_online(catchup_uri)
    wait_for_collection_resharding_operations_count(catchup_uri, COLLECTION, 0)

    # Sanity-check the persisted state on target: replica = Dead, no resharding.
    assert json.loads(resharding_state.read_text()) is None
    assert (
        json.loads(new_replica_state.read_text())["peers"].get(str(target_peer_id)) == "Dead"
    )

    # Now forge the partial-apply state. Kill again, restore resharding_state.json
    # to the active resharding (as it would look mid-apply between the durable
    # replica_state.write and the local abort_resharding), and reset
    # apply_progress_queue to force `SetShardReplicaState(Resharding -> Dead)`
    # to replay. Replica state stays Dead — that's the trap: it makes
    # `current_state` read back as Dead on replay.
    p = processes.pop(target_idx)
    restart_port = p.p2p_port
    p.kill()

    resharding_state.write_text(json.dumps(initial_resharding_state))

    state = json.loads(raft_state.read_text())
    state["apply_progress_queue"] = [
        commit_before_set + 1,
        state["state"]["hard_state"]["commit"],
    ]
    raft_state.write_text(json.dumps(state))

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
