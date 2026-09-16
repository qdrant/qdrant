"""Consensus coverage for shrinking a 2-voter cluster by peer removal.

`test_remove_leader_from_two_node_cluster` delays Raft/Send on the follower's
peer proxy so leader→follower delivery lags. That widens the race where leader
self-remove wipes the shared address book before the async commit notification
is delivered — reproducing the CM downscale failure mode.
"""

from __future__ import annotations

import pathlib
import time

import pytest
import requests

from .assertions import assert_http_ok
from .utils import *  # noqa: F403

N_PEERS = 2
RAFT_SEND = "/qdrant.Raft/Send"
# Keep the follower Raft sender busy long enough for self-remove apply to wipe
# peer URIs before the async commit notify looks them up.
RAFT_SEND_DELAY_SEC = 0.5
# Settle window: survivor must stay broken, not recover after a brief lag.
STUCK_SETTLE_SEC = 10


def _leader_follower_indices(peer_api_uris: list[str]) -> tuple[int, int, int]:
    """Return (leader_idx, follower_idx, leader_peer_id)."""
    wait_for(leader_is_defined, peer_api_uris[0])
    leader_id = get_leader(peer_api_uris[0])
    for idx, uri in enumerate(peer_api_uris):
        if get_cluster_info(uri)["peer_id"] == leader_id:
            return idx, 1 - idx, leader_id
    raise AssertionError(f"leader {leader_id} not found among {peer_api_uris}")


def _remove_peer(via_uri: str, peer_id: int):
    res = requests.delete(f"{via_uri}/cluster/peer/{peer_id}?timeout=60")
    assert_http_ok(res)


def _wait_for_two_voter_cluster(peer_api_uris: list[str]):
    """`start_cluster` can return before the learner is promoted to voter."""
    wait_for(all_peers_are_voters, peer_api_uris)
    wait_for_same_applied_commit(peer_api_uris)


def _assert_survivor_is_operational(survivor_uri: str):
    """Survivor must shrink to one voter, elect itself, and accept writes."""

    def survivor_is_sole_leader() -> bool:
        try:
            info = get_cluster_info(survivor_uri)
        except (requests.exceptions.ConnectionError, Exception) as err:
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

    res = requests.put(
        f"{survivor_uri}/collections/after_remove",
        json={"vectors": {"size": 4, "distance": "Dot"}, "shard_number": 1},
    )
    assert_http_ok(res)
    wait_collection_exists_and_active_on_all_peers(
        collection_name="after_remove",
        peer_api_uris=[survivor_uri],
    )


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
        tmp_path, N_PEERS, use_peer_proxy=True
    )
    _wait_for_two_voter_cluster(peer_api_uris)
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
        tmp_path, N_PEERS, use_peer_proxy=True
    )
    _wait_for_two_voter_cluster(peer_api_uris)

    leader_idx, follower_idx, leader_id = _leader_follower_indices(peer_api_uris)
    # First peer is the bootstrap node (no bootstrap URI for who_is fallback).
    assert leader_idx == 0, "expected bootstrap peer to remain Raft leader"

    follower_proxy = processes[follower_idx].proxy
    assert follower_proxy is not None

    with follower_proxy.delay_rpc(RAFT_SEND, RAFT_SEND_DELAY_SEC):
        _remove_peer(peer_api_uris[leader_idx], leader_id)

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
