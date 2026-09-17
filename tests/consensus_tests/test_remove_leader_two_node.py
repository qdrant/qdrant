"""Reproduce the lost commit notification during leader self-removal."""

import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import requests

from .assertions import assert_http_ok
from .raft_messages import MSG_APPEND_RESPONSE, decode_raft_message, removal_entry_index
from .utils import (
    all_peers_are_voters,
    every_test,  # noqa: F401 -- process cleanup fixture
    init_pytest_log_folder,
    processes,
    start_cluster,
    wait_for,
    wait_for_same_applied_commit,
)


RAFT_SEND = "/qdrant.Raft/Send"
TIMEOUT = 15


def _cluster(uri):
    response = requests.get(f"{uri}/cluster", timeout=TIMEOUT)
    assert_http_ok(response)
    return response.json()["result"]


def _remove(uri, peer_id, server_timeout=60):
    params = {} if server_timeout is None else {"timeout": server_timeout}
    response = requests.delete(f"{uri}/cluster/peer/{peer_id}", params=params, timeout=TIMEOUT)
    assert_http_ok(response)


@pytest.mark.parametrize(("remove_leader", "server_timeout"), [
    pytest.param(True, None, id="default-wait-leader"),
    pytest.param(True, 60, id="wait-60s-leader"),
    pytest.param(False, 60, id="wait-60s-follower-control"),
])
def test_remove_peer_from_two_node_cluster(tmp_path, remove_leader, server_timeout):
    # Keep the held RPC alive until the test releases it.
    uris, _, _ = start_cluster(tmp_path, 2, use_peer_proxy=True, extra_env={
        "QDRANT__CLUSTER__CONSENSUS__MESSAGE_TIMEOUT_TICKS": "600",
    })
    wait_for(all_peers_are_voters, uris)
    wait_for_same_applied_commit(uris)
    leader, follower = (_cluster(uri) for uri in uris)
    leader_id, follower_id = leader["peer_id"], follower["peer_id"]
    removed_id = leader_id if remove_leader else follower_id
    assert leader["raft_info"]["leader"] == follower["raft_info"]["leader"] == leader_id
    leader_process, follower_process = processes
    removal_index = None

    def is_removal(request):
        nonlocal removal_index
        message = decode_raft_message(request)
        if message.from_peer != leader_id or message.to != follower_id:
            return False
        index = removal_entry_index(message, removed_id)
        if index is None:
            return False
        # Record the index before forwarding so the ACK matcher can use it.
        removal_index = index
        return True

    def is_new_append_ack(request):
        message = decode_raft_message(request)
        return (message.msg_type == MSG_APPEND_RESPONSE and not message.reject
                and message.from_peer == follower_id and message.to == leader_id
                and message.index == removal_index)

    # The Raft ACK travels separately from the gRPC response. Holding the response
    # keeps A's sender busy while the ACK allows A to commit the removal.
    with ThreadPoolExecutor(max_workers=1) as pool:
        with follower_process.proxy.hold_rpc_response(RAFT_SEND, is_removal) as response_gate:
            with leader_process.proxy.hold_rpc(RAFT_SEND, is_new_append_ack) as ack_gate:
                remove = pool.submit(_remove, uris[0], removed_id, server_timeout)
                append = decode_raft_message(response_gate.wait_for_request(TIMEOUT))
                assert removal_entry_index(append, removed_id) == removal_index
                ack = decode_raft_message(ack_gate.wait_for_request(TIMEOUT))
                assert ack.index == removal_index
                assert append.commit < removal_index
                assert _cluster(uris[1])["raft_info"]["commit"] < removal_index
                ack_gate.release()

                # DELETE succeeds before B commits, even with timeout=60.
                remove.result(timeout=TIMEOUT)
                assert not response_gate.cancelled.is_set()
                detached = _cluster(uris[0])
                assert detached["raft_info"]["commit"] >= removal_index
                assert str(follower_id) not in detached["peers"]
                assert _cluster(uris[1])["raft_info"]["commit"] < removal_index
                response_gate.release()

    if not remove_leader:
        _assert_operational([uris[0]], {leader_id})
        return

    # With both gates released and A still alive, the commit notification must
    # fail because B's address was removed.
    log_path = Path(init_pytest_log_folder()) / "peer_0_0.log"

    def commit_notification_lost_address():
        for line in log_path.read_text().splitlines():
            if ("Failed to send Raft message" in line and "No bootstrap URI provided" in line
                    and f"to: {follower_id}" in line):
                commit = re.search(r"commit: (\d+)", line)
                if commit and int(commit[1]) >= removal_index:
                    return True
        return False

    # NOTE: This reproducer passes when B is stuck. Once leader removal is fixed,
    # replace these failure checks with _assert_operational([uris[1]], {follower_id}).
    wait_for(commit_notification_lost_address, wait_for_timeout=TIMEOUT)
    wait_for(lambda: _cluster(uris[1])["raft_info"]["leader"] in (None, 0), wait_for_timeout=TIMEOUT)
    survivor = _cluster(uris[1])
    assert set(survivor["peers"]) == {str(leader_id), str(follower_id)}
    assert survivor["raft_info"]["leader"] in (None, 0)
    assert survivor["raft_info"]["commit"] < removal_index
    assert survivor["raft_info"]["is_voter"] is True
    assert leader_process.proc.poll() is None


def _assert_operational(uris, peer_ids):
    def peers_agree():
        infos = [_cluster(uri) for uri in uris]
        leaders = {info["raft_info"]["leader"] for info in infos}
        return (len(leaders) == 1 and leaders <= peer_ids
                and all(set(info["peers"]) == {str(peer_id) for peer_id in peer_ids}
                        and info["raft_info"]["is_voter"] for info in infos))

    wait_for(peers_agree, wait_for_timeout=TIMEOUT)
    response = requests.put(
        f"{uris[0]}/collections/after_remove",
        json={"vectors": {"size": 4, "distance": "Dot"}, "shard_number": 1},
        timeout=TIMEOUT,
    )
    assert_http_ok(response)
    for uri in uris:
        def collection_exists():
            response = requests.get(f"{uri}/collections/after_remove", timeout=TIMEOUT)
            return response.status_code == 200
        wait_for(collection_exists, wait_for_timeout=TIMEOUT)


def test_remove_leader_from_three_node_cluster(tmp_path):
    uris, _, _ = start_cluster(tmp_path, 3)
    wait_for(all_peers_are_voters, uris)
    wait_for_same_applied_commit(uris)
    infos = [_cluster(uri) for uri in uris]
    leader_id = infos[0]["raft_info"]["leader"]
    leader_index = next(index for index, info in enumerate(infos) if info["peer_id"] == leader_id)
    _remove(uris[leader_index], leader_id)
    leader_process = processes[leader_index]
    leader_process.kill()
    processes.remove(leader_process)
    survivors = [uri for index, uri in enumerate(uris) if index != leader_index]
    survivor_ids = {info["peer_id"] for info in infos if info["peer_id"] != leader_id}
    _assert_operational(survivors, survivor_ids)
