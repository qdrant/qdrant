"""Consensus coverage for shrinking a cluster by removing the Raft leader.

- 2 voters + delayed Raft/Send: reproduces the CM downscale failure (stuck).
- 2 voters, remove follower: control that non-leader remove stays healthy.
- 3 voters, remove leader: quorum must re-elect and stay writable.
"""

from __future__ import annotations

import pathlib
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests

from .assertions import assert_http_ok
from .utils import *  # noqa: F403

N_PEERS_TWO = 2
N_PEERS_THREE = 3
RAFT_SEND = "/qdrant.Raft/Send"
# Keep the follower Raft sender busy long enough for self-remove apply to wipe
# peer URIs before the async commit notify looks them up.
RAFT_SEND_DELAY_SEC = 0.5
# Settle window: survivor must stay broken, not recover after a brief lag.
STUCK_SETTLE_SEC = 10
# Server-side consensus wait (?timeout=) vs client HTTP bound (must be lower).
REMOVE_PEER_SERVER_TIMEOUT_SEC = 60
REMOVE_PEER_CLIENT_TIMEOUT_SEC = 30


def _leader_index_and_id(peer_api_uris: list[str]) -> tuple[int, int]:
    """Return (leader_idx, leader_peer_id)."""
    wait_for(leader_is_defined, peer_api_uris[0])
    leader_id = get_leader(peer_api_uris[0])
    for idx, uri in enumerate(peer_api_uris):
        if get_cluster_info(uri)["peer_id"] == leader_id:
            return idx, leader_id
    raise AssertionError(f"leader {leader_id} not found among {peer_api_uris}")


def _leader_follower_indices(peer_api_uris: list[str]) -> tuple[int, int, int]:
    """Return (leader_idx, follower_idx, leader_peer_id) for a 2-peer cluster."""
    assert len(peer_api_uris) == 2
    leader_idx, leader_id = _leader_index_and_id(peer_api_uris)
    return leader_idx, 1 - leader_idx, leader_id


def _remove_peer(via_uri: str, peer_id: int):
    res = requests.delete(
        f"{via_uri}/cluster/peer/{peer_id}?timeout={REMOVE_PEER_SERVER_TIMEOUT_SEC}",
        timeout=REMOVE_PEER_CLIENT_TIMEOUT_SEC,
    )
    assert_http_ok(res)


def _wait_for_voter_cluster(peer_api_uris: list[str]):
    """`start_cluster` can return before learners are promoted to voters."""
    wait_for(all_peers_are_voters, peer_api_uris)
    wait_for_same_applied_commit(peer_api_uris)


def _assert_collections_writable(peer_api_uris: list[str], collection_name: str):
    res = requests.put(
        f"{peer_api_uris[0]}/collections/{collection_name}",
        json={"vectors": {"size": 4, "distance": "Dot"}, "shard_number": 1},
        timeout=REMOVE_PEER_CLIENT_TIMEOUT_SEC,
    )
    assert_http_ok(res)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=collection_name,
        peer_api_uris=peer_api_uris,
    )


def _assert_survivor_is_operational(survivor_uri: str):
    """Survivor must shrink to one voter, elect itself, and accept writes."""

    def survivor_is_sole_leader() -> bool:
        try:
            info = get_cluster_info(survivor_uri)
        except requests.exceptions.RequestException as err:
            print(f"survivor not ready: {err}")
            return False
        raft = info["raft_info"]
        ok = (
            len(info["peers"]) == 1
            and raft.get("leader") == info["peer_id"]
            and raft.get("is_voter") is True
        )
        if not ok:
            print(
                f"survivor not sole leader yet: peers={len(info['peers'])} "
                f"leader={raft.get('leader')} self={info['peer_id']} "
                f"is_voter={raft.get('is_voter')} commit={raft.get('commit')}"
            )
        return ok

    wait_for(survivor_is_sole_leader, wait_for_timeout=WAIT_TIME_SEC)
    _assert_collections_writable([survivor_uri], "after_remove")


def _assert_survivors_reelected(survivor_uris: list[str], removed_leader_id: int):
    """Survivors must drop the removed peer, agree on a new leader, and write."""

    def survivors_have_new_leader() -> bool:
        try:
            infos = [get_cluster_info(uri) for uri in survivor_uris]
        except requests.exceptions.RequestException as err:
            print(f"survivor probe failed: {err}")
            return False

        peer_counts = [len(info["peers"]) for info in infos]
        leaders = {info["raft_info"].get("leader") for info in infos}
        survivor_ids = {info["peer_id"] for info in infos}
        print(
            f"survivors after leader remove: peer_counts={peer_counts} "
            f"leaders={leaders} survivors={survivor_ids} removed={removed_leader_id}"
        )
        if any(count != 2 for count in peer_counts):
            return False
        if len(leaders) != 1:
            return False
        leader = next(iter(leaders))
        if leader in (0, None, removed_leader_id):
            return False
        if leader not in survivor_ids:
            return False
        return all(info["raft_info"].get("is_voter") is True for info in infos)

    wait_for(survivors_have_new_leader, wait_for_timeout=WAIT_TIME_SEC)
    _assert_collections_writable(survivor_uris, "after_remove")


def _assert_survivor_stuck_after_leader_remove(survivor_uri: str):
    """Survivor keeps the removed peer and cannot elect (commit notify lost)."""
    deadline = time.time() + STUCK_SETTLE_SEC
    while time.time() < deadline:
        try:
            info = get_cluster_info(survivor_uri)
        except requests.exceptions.RequestException as err:
            print(f"survivor probe failed: {err}")
            time.sleep(RETRY_INTERVAL_SEC)
            continue
        raft = info["raft_info"]
        print(
            f"survivor after leader remove: peers={len(info['peers'])} "
            f"leader={raft.get('leader')} self={info['peer_id']} "
            f"is_voter={raft.get('is_voter')} commit={raft.get('commit')}"
        )
        # If it recovers, the bug is gone (or delay was insufficient).
        if (
            len(info["peers"]) == 1
            and raft.get("leader") == info["peer_id"]
            and raft.get("is_voter") is True
        ):
            raise AssertionError(
                "survivor became sole leader; expected stuck 2-peer / no-leader state"
            )
        time.sleep(RETRY_INTERVAL_SEC)

    # Require a current response instead of a cached successful probe.
    last = get_cluster_info(survivor_uri)
    raft = last["raft_info"]
    assert len(last["peers"]) == 2, (
        f"expected survivor to still list both peers, got peers={list(last['peers'])}"
    )
    assert raft.get("leader") in (0, None), (
        f"expected no elected leader after lost commit notify, got leader={raft.get('leader')}"
    )


def test_remove_follower_from_two_node_cluster(tmp_path: pathlib.Path):
    """Control: removing the follower from a 2-voter cluster must succeed."""
    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(
        tmp_path, N_PEERS_TWO, use_peer_proxy=True
    )
    _wait_for_voter_cluster(peer_api_uris)
    leader_idx, follower_idx, _leader_id = _leader_follower_indices(peer_api_uris)

    follower_id = get_cluster_info(peer_api_uris[follower_idx])["peer_id"]
    _remove_peer(peer_api_uris[leader_idx], follower_id)

    follower_process = processes[follower_idx]
    follower_process.kill()
    processes.remove(follower_process)

    _assert_survivor_is_operational(peer_api_uris[leader_idx])


def test_remove_leader_from_two_node_cluster(tmp_path: pathlib.Path):
    """Removing the Raft leader from a 2-voter cluster leaves the follower stuck.

    Follower P2P is reached through PeerProxy; delaying `/qdrant.Raft/Send`
    makes leader self-remove clear the shared address book before the async
    commit notification is delivered. After apply, a Raft/Send gate on the
    follower times out — proving the notify never arrives — and the survivor
    stays at peers=2 / leader=0.
    """
    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(
        tmp_path, N_PEERS_TWO, use_peer_proxy=True
    )
    _wait_for_voter_cluster(peer_api_uris)

    leader_idx, follower_idx, leader_id = _leader_follower_indices(peer_api_uris)
    # First peer is the bootstrap node (no bootstrap URI for who_is fallback).
    assert leader_idx == 0, "expected bootstrap peer to remain Raft leader"

    follower_proxy = processes[follower_idx].proxy
    assert follower_proxy is not None

    with follower_proxy.delay_rpc(RAFT_SEND, RAFT_SEND_DELAY_SEC):
        # Observe a leader→follower Raft/Send during remove, then release it so
        # the delayed forward still runs under delay_rpc.
        with follower_proxy.hold_rpc(RAFT_SEND) as gate, ThreadPoolExecutor(
            max_workers=1
        ) as pool:
            remove = pool.submit(_remove_peer, peer_api_uris[leader_idx], leader_id)
            try:
                gate.wait_for_request()
                gate.release()
                remove.result(timeout=REMOVE_PEER_SERVER_TIMEOUT_SEC)
            finally:
                # Best-effort only: cancel does not interrupt a running DELETE.
                remove.cancel()

    # Drain any Raft/Send that entered the proxy before the address wipe.
    time.sleep(RAFT_SEND_DELAY_SEC + 0.1)
    # Further leader→follower Raft/Send must not arrive once URIs are gone.
    with follower_proxy.hold_rpc(RAFT_SEND) as gate:
        with pytest.raises(TimeoutError):
            gate.wait_for_request(timeout=2)

    leader_process = processes[leader_idx]
    leader_process.kill()
    processes.remove(leader_process)

    _assert_survivor_stuck_after_leader_remove(peer_api_uris[follower_idx])


def test_remove_leader_from_three_node_cluster(tmp_path: pathlib.Path):
    """Quorum: removing the Raft leader from a 3-voter cluster must re-elect.

    Unlike the 2-voter case, the remaining majority can commit the remove and
    elect a new leader without the departing peer.
    """
    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, N_PEERS_THREE)
    _wait_for_voter_cluster(peer_api_uris)

    leader_idx, leader_id = _leader_index_and_id(peer_api_uris)
    survivor_uris = [uri for idx, uri in enumerate(peer_api_uris) if idx != leader_idx]

    _remove_peer(peer_api_uris[leader_idx], leader_id)

    leader_process = processes[leader_idx]
    leader_process.kill()
    processes.remove(leader_process)

    _assert_survivors_reelected(survivor_uris, removed_leader_id=leader_id)
