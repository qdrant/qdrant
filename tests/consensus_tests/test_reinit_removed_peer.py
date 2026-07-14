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

    Also covers `--reinit` dropping the stale `first_voter` and address book
    of the old cluster (simulating the peer being killed before it applied
    `RemoveNode(self)`, which would have pruned the addresses): both leak into
    peers that later bootstrap onto the reinitialized cluster, and a stale
    `first_voter` corrupts their voter set so they never become ready. The old
    first peer stays alive throughout to catch any such leak.
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

    # Deterministically simulate the peer being killed after `RemoveNode(self)`
    # was committed (guaranteed to be in its WAL, see above) but before it
    # applied *any* of the old cluster's log: the address book still holds the
    # old first peer, `first_voter` still points at it (nothing resets it on
    # removal, so it is stale either way), and no entry is applied. `--reinit`
    # must drop the stale `first_voter` and address book. Both are served to
    # peers bootstrapping onto the reinitialized cluster; with nothing applied
    # the reinitialized leader replicates its log as plain entries (no
    # snapshot), so a stale `first_voter` seeds the joining peer's initial
    # `conf_state` with a peer of the *old* cluster and permanently corrupts
    # its voter set - its `/readyz` then waits for the old cluster's commit
    # index, which its own consensus never reaches.
    first_peer_id = get_cluster_info(peer_api_uris[0])["peer_id"]
    raft_state_path = removed_peer_dir / "storage" / "raft_state.json"
    raft_state = json.loads(raft_state_path.read_text())
    raft_state["peer_address_by_id"][str(first_peer_id)] = bootstrap_uri
    raft_state["first_voter"] = first_peer_id
    raft_state["apply_progress_queue"] = None
    raft_state["state"]["hard_state"]["commit"] = 0
    raft_state_path.write_text(json.dumps(raft_state))

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

    # The old cluster's peers are not part of the reinitialized cluster: the
    # stale address injected above must have been dropped by `--reinit`.
    assert check_cluster_size(reinit_api_uri, 1)

    # It must also still be usable as a cluster seed: a fresh peer can bootstrap
    # onto it, which requires the reinitialized peer to serve a snapshot (the
    # entry at the commit index is kept as the snapshot anchor by the fix).
    new_peer_dir = make_peer_folder(tmp_path, N_PEERS)
    new_peer_uri = start_peer(new_peer_dir, "new_peer.log", reinit_bootstrap_uri)
    wait_for_peer_online(new_peer_uri)
    wait_for(check_leader, new_peer_uri, leader)


def test_reinit_removed_peer_readyz_ignores_old_cluster(tmp_path: pathlib.Path):
    """
    Regression test for `/readyz` treating a stale address-book entry as a
    cluster member.

    `peer_address_by_id` can hold addresses of peers that are not part of this
    peer's consensus. The health checker used to treat every address-book
    entry as a cluster member and would wait for this peer to reach the
    *foreign* peer's commit index, which its own consensus can never catch up
    with, so `/readyz` never passed. It must rely on `conf_state` membership
    instead and ignore the foreign peer.

    Since `--reinit` itself now prunes the address book, the stale address is
    injected on a plain restart (no `--reinit`) of a previously reinitialized
    single-voter peer, and the foreign (old) cluster's commit index is pushed
    ahead so this peer's consensus stays behind it.
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

    # Reinitialize the removed peer as a fresh single-voter first peer.
    reinit_api_uri, _reinit_bootstrap_uri = start_first_peer(
        removed_peer_dir, "removed_peer_reinit.log", reinit=True,
    )
    wait_for_peer_online(reinit_api_uri)

    # Stop it again to inject the stale address below.
    reinit_peer_process = processes[1]
    reinit_peer_process.kill()
    processes.remove(reinit_peer_process)

    # Put the old first peer's address into the persisted address book, while
    # `conf_state` remains this peer only. It doesn't matter how a real
    # cluster ends up in this state (e.g. a peer killed mid-removal and
    # restarted without `--reinit`) - `/readyz` must not treat address-book
    # entries outside `conf_state` as cluster members.
    raft_state_path = removed_peer_dir / "storage" / "raft_state.json"
    raft_state = json.loads(raft_state_path.read_text())
    raft_state["peer_address_by_id"][str(first_peer_id)] = bootstrap_uri
    raft_state_path.write_text(json.dumps(raft_state))

    # Push the old cluster's commit index well ahead of anything the
    # single-voter peer has in its WAL. Each collection creation commits
    # several consensus entries on the (now single-node) old cluster.
    for i in range(3):
        res = requests.put(
            f"{peer_api_uris[0]}/collections/ahead_{i}?timeout=60",
            json={"vectors": {"size": 4, "distance": "Dot"}},
        )
        assert_http_ok(res)

    old_commit = get_cluster_info(peer_api_uris[0])["raft_info"]["commit"]

    # Restart the single-voter peer (no `--reinit`), with the old first peer
    # still alive and reachable at the injected address. This peer's own
    # consensus can never reach `old_commit`, so `/readyz` must not depend
    # on it.
    restart_api_uri, _restart_bootstrap_uri = start_first_peer(
        removed_peer_dir, "removed_peer_restart.log",
    )

    wait_for_peer_online(restart_api_uri)

    # The readiness race is only exercised while this peer's own commit index
    # stays behind the old cluster's.
    restart_commit = get_cluster_info(restart_api_uri)["raft_info"]["commit"]
    assert restart_commit < old_commit
