import json
import pathlib

from .utils import *

N_PEERS = 2


def test_reinit_removed_peer(tmp_path: pathlib.Path):
    """
    Regression test for reinitializing a peer that was previously *removed from
    consensus*.

    When a peer is removed, its Raft log gains a committed `RemoveNode(self)`
    conf-change. `--reinit` resets the peer's `conf_state` to a single voter
    (itself), but leaves the Raft log untouched. Without dropping that stale
    entry it gets replayed on top of the reset single-voter config on startup,
    and consensus initialization panics with "removed all voters", so the node
    can never come back.
    """
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    # The peer we are going to remove from consensus and later reinitialize.
    removed_peer_uri = peer_api_uris[1]
    removed_peer_dir = peer_dirs[1]
    removed_peer_id = get_cluster_info(removed_peer_uri)["peer_id"]

    # Gracefully remove the still-alive peer. In a 2-node cluster the removal
    # can only be committed once the removed peer itself acks it, which
    # guarantees the `RemoveNode(self)` entry lands in its own WAL. (There are
    # no collections, so the peer holds no shards and can be removed cleanly.)
    res = requests.delete(f"{peer_api_uris[0]}/cluster/peer/{removed_peer_id}?timeout=60")
    assert_http_ok(res)

    # The first peer is now a single-node cluster - which also confirms the
    # removal was committed (and therefore persisted on the removed peer).
    wait_for(check_cluster_size, peer_api_uris[0], 1)

    # Stop the removed peer (its consensus already stopped on self-removal).
    removed_peer_process = processes[1]
    removed_peer_process.kill()
    processes.remove(removed_peer_process)

    # Reinitialize the removed peer as a fresh first peer, reusing its data
    # directory. Before the fix this panicked on startup with:
    #   Can't initialize consensus: Failed to apply configuration change entry
    #   Caused by: Error in Raft consensus: removed all voters
    reinit_api_uri, reinit_bootstrap_uri = start_first_peer(
        removed_peer_dir, "removed_peer_reinit.log", reinit=True,
    )

    # It must come online and elect itself leader of the new single-voter
    # cluster (reaching this point already proves consensus initialized without
    # the "removed all voters" panic).
    wait_for_peer_online(reinit_api_uri)
    wait_for(leader_is_defined, reinit_api_uri)
    leader = get_leader(reinit_api_uri)

    # It must also still be usable as a cluster seed: a fresh peer can bootstrap
    # onto it, which requires the reinitialized peer to serve a snapshot (the
    # entry at the commit index is kept as the snapshot anchor by the fix).
    new_peer_dir = make_peer_folder(tmp_path, N_PEERS)
    new_peer_uri = start_peer(new_peer_dir, "new_peer.log", reinit_bootstrap_uri)
    wait_for_peer_online(new_peer_uri)
    wait_for(check_leader, new_peer_uri, leader)


def test_reinit_removed_peer_readyz_ignores_old_cluster(tmp_path: pathlib.Path):
    """
    Regression test for `/readyz` of a reinitialized peer racing against the
    old cluster.

    Applying `RemoveNode(self)` prunes all other peers from the removed peer's
    persisted address book. But the peer may be killed after the removal is
    committed and *before it applied the entry* - then the old first peer's
    address survives in `raft_state.json`. After `--reinit` the health checker
    used to treat every address-book entry as a cluster member and would wait
    for the reinitialized peer to reach the *old* cluster's commit index - a
    foreign consensus it can never catch up with, so `/readyz` never passed.

    The kill-before-apply race is simulated by putting the old first peer's
    address back into the killed peer's persisted state, and the old cluster's
    commit index is pushed ahead so the reinitialized single-voter consensus
    stays behind it. `/readyz` must rely on `conf_state` (reset to a single
    voter by `--reinit`) and ignore the old peer.
    """
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    removed_peer_uri = peer_api_uris[1]
    removed_peer_dir = peer_dirs[1]
    removed_peer_id = get_cluster_info(removed_peer_uri)["peer_id"]
    first_peer_id = get_cluster_info(peer_api_uris[0])["peer_id"]

    res = requests.delete(f"{peer_api_uris[0]}/cluster/peer/{removed_peer_id}?timeout=60")
    assert_http_ok(res)
    wait_for(check_cluster_size, peer_api_uris[0], 1)

    removed_peer_process = processes[1]
    removed_peer_process.kill()
    processes.remove(removed_peer_process)

    # Simulate the killed peer not having applied `RemoveNode(self)` yet: the
    # old first peer's address is still in its persisted address book.
    raft_state_path = removed_peer_dir / "storage" / "raft_state.json"
    raft_state = json.loads(raft_state_path.read_text())
    raft_state["peer_address_by_id"][str(first_peer_id)] = bootstrap_uri
    raft_state_path.write_text(json.dumps(raft_state))

    # Push the old cluster's commit index well ahead of anything the removed
    # peer has in its WAL. Each collection creation commits several consensus
    # entries on the (now single-node) old cluster.
    for i in range(3):
        res = requests.put(
            f"{peer_api_uris[0]}/collections/ahead_{i}?timeout=60",
            json={"vectors": {"size": 4, "distance": "Dot"}},
        )
        assert_http_ok(res)

    old_commit = get_cluster_info(peer_api_uris[0])["raft_info"]["commit"]

    # Reinitialize the removed peer as a fresh first peer, with the old first
    # peer still alive and reachable. Its own single-voter consensus can never
    # reach `old_commit`, so `/readyz` must not depend on it.
    reinit_api_uri, _reinit_bootstrap_uri = start_first_peer(
        removed_peer_dir, "removed_peer_reinit.log", reinit=True,
    )

    wait_for_peer_online(reinit_api_uri)

    # The readiness race is only exercised while the reinitialized peer's own
    # commit index stays behind the old cluster's.
    reinit_commit = get_cluster_info(reinit_api_uri)["raft_info"]["commit"]
    assert reinit_commit < old_commit
