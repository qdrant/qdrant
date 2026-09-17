"""Verify that peer removal preserves a working surviving cluster."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack

import pytest
import requests

from .assertions import assert_http_ok
from .raft_messages import MSG_APPEND, MSG_APPEND_RESPONSE, MSG_REQUEST_VOTE, decode_raft_message, removal_entry_index
from .utils import (
    all_peers_are_voters,
    every_test,  # noqa: F401 -- process cleanup fixture
    make_peer_folder,
    peers_have_version,
    processes,
    start_cluster,
    start_peer,
    wait_for,
    wait_for_same_applied_commit,
)


RAFT_SEND = "/qdrant.Raft/Send"
TIMEOUT = 15


def _cluster(uri):
    response = requests.get(f"{uri}/cluster", timeout=TIMEOUT)
    assert_http_ok(response)
    return response.json()["result"]


def _remove(uri, peer_id, server_timeout=60, force=False):
    params = {} if server_timeout is None else {"timeout": server_timeout}
    params["force"] = str(force).lower()
    response = requests.delete(f"{uri}/cluster/peer/{peer_id}", params=params, timeout=TIMEOUT)
    assert_http_ok(response)


@pytest.mark.parametrize("via_leader", [True, False], ids=["via-leader", "via-follower"])
@pytest.mark.parametrize("server_timeout", [None, 60], ids=["default-wait", "wait-60s"])
def test_remove_leader_from_two_node_cluster(tmp_path, via_leader, server_timeout):
    # Keep the held RPC alive until the test releases it.
    uris, _, _ = start_cluster(tmp_path, 2, use_peer_proxy=True, extra_env={
        "QDRANT__CLUSTER__CONSENSUS__MESSAGE_TIMEOUT_TICKS": "600",
    })
    wait_for(all_peers_are_voters, uris)
    wait_for(peers_have_version, uris)
    wait_for_same_applied_commit(uris)
    leader, follower = (_cluster(uri) for uri in uris)
    leader_id, follower_id = leader["peer_id"], follower["peer_id"]
    assert leader["raft_info"]["leader"] == follower["raft_info"]["leader"] == leader_id
    leader_process, follower_process = processes
    removal_index = None

    def is_removal(request):
        nonlocal removal_index
        message = decode_raft_message(request)
        if message.from_peer != follower_id or message.to != leader_id:
            return False
        index = removal_entry_index(message, leader_id)
        if index is None:
            return False
        removal_index = index
        return True

    def is_new_term_ack(request):
        message = decode_raft_message(request)
        return (message.msg_type == MSG_APPEND_RESPONSE and not message.reject
                and message.from_peer == leader_id and message.to == follower_id
                and message.term > leader["raft_info"]["term"])

    with ThreadPoolExecutor(max_workers=1) as pool:
        with leader_process.proxy.hold_rpc_response(RAFT_SEND, is_removal) as response_gate:
            with follower_process.proxy.hold_rpc(RAFT_SEND, is_new_term_ack) as election_gate:
                remove = pool.submit(_remove, uris[0 if via_leader else 1], leader_id, server_timeout)
                ack = decode_raft_message(election_gate.wait_for_request(TIMEOUT))
                assert not remove.done()
                # B has won, but cannot commit its new-term no-op without A's ACK.
                survivor = _cluster(uris[1])
                assert survivor["raft_info"]["leader"] == follower_id
                assert survivor["raft_info"]["commit"] < ack.index
                for uri in uris:
                    info = _cluster(uri)
                    assert set(info["peers"]) == {str(leader_id), str(follower_id)}
                assert removal_index is None
                election_gate.release()

            # Hold B's RPC response after A receives Remove(A). A can send its
            # separate Raft ACK, allowing the surviving leader B to commit first.
            append = decode_raft_message(response_gate.wait_for_request(TIMEOUT))
            assert append.from_peer == follower_id
            assert append.commit < removal_index
            wait_for(lambda: _cluster(uris[1])["raft_info"]["commit"] >= removal_index)
            assert _cluster(uris[1])["raft_info"]["leader"] == follower_id
            assert set(_cluster(uris[1])["peers"]) == {str(follower_id)}
            departing = _cluster(uris[0])
            assert departing["raft_info"]["commit"] < removal_index
            assert departing["consensus_thread_status"]["consensus_thread_status"] == "working"
            if via_leader:
                assert not remove.done()
            assert not response_gate.cancelled.is_set()
            response_gate.release()
            remove.result(timeout=TIMEOUT)

    _assert_operational([uris[1]], {follower_id})
    wait_for(lambda: _cluster(uris[0])["consensus_thread_status"]["consensus_thread_status"] == "stopped")
    assert leader_process.proc.poll() is None


@pytest.mark.parametrize("force", [False, True])
def test_cannot_remove_last_voter(tmp_path, force):
    uris, _, _ = start_cluster(tmp_path, 1)
    # Startup metadata also advances the log. Finish it before recording the index.
    wait_for(peers_have_version, uris)
    wait_for_same_applied_commit(uris)
    before = _cluster(uris[0])
    response = requests.delete(
        f"{uris[0]}/cluster/peer/{before['peer_id']}",
        params={"force": str(force).lower(), "timeout": 2}, timeout=TIMEOUT,
    )
    assert response.status_code == 400, response.text
    assert "last voting peer" in response.text
    assert _cluster(uris[0])["raft_info"]["commit"] == before["raft_info"]["commit"]
    _assert_operational(uris, {before["peer_id"]})


def test_remove_follower_from_two_node_cluster(tmp_path):
    uris, _, _ = start_cluster(tmp_path, 2)
    wait_for(all_peers_are_voters, uris)
    leader, follower = (_cluster(uri) for uri in uris)
    assert leader["raft_info"]["leader"] == leader["peer_id"]
    _remove(uris[0], follower["peer_id"])
    _assert_operational([uris[0]], {leader["peer_id"]})


def test_removal_timeout_does_not_queue_removal_after_handoff(tmp_path):
    uris, _, _ = start_cluster(tmp_path, 2, use_peer_proxy=True, extra_env={
        "QDRANT__CLUSTER__CONSENSUS__MESSAGE_TIMEOUT_TICKS": "600",
    })
    wait_for(all_peers_are_voters, uris)
    leader, follower = (_cluster(uri) for uri in uris)
    peer_ids = {leader["peer_id"], follower["peer_id"]}
    with ThreadPoolExecutor(max_workers=1) as pool:
        with processes[0].proxy.hold_rpc(
            RAFT_SEND, lambda request: decode_raft_message(request).msg_type == MSG_REQUEST_VOTE,
        ) as gate:
            remove = pool.submit(requests.delete, f"{uris[0]}/cluster/peer/{leader['peer_id']}",
                                 params={"timeout": 6}, timeout=TIMEOUT)
            gate.wait_for_request(TIMEOUT)
            response = remove.result(timeout=TIMEOUT)
            assert response.status_code == 408, response.text
            assert "timed out" in response.text.lower()
            gate.release()
    # Releasing a delayed vote must not revive the expired removal.
    _assert_operational(uris, peer_ids)


@pytest.mark.parametrize("restart_departing", [False, True])
def test_removal_timeout_after_proposal_still_applies_removal(tmp_path, restart_departing):
    uris, peer_dirs, bootstrap = start_cluster(tmp_path, 2, use_peer_proxy=True, extra_env={
        "QDRANT__CLUSTER__CONSENSUS__MESSAGE_TIMEOUT_TICKS": "600",
    })
    wait_for(all_peers_are_voters, uris)
    wait_for_same_applied_commit(uris)
    leader, follower = (_cluster(uri) for uri in uris)
    leader_id, follower_id = leader["peer_id"], follower["peer_id"]
    leader_process = processes[0]
    removal_index = None

    def is_removal(request):
        nonlocal removal_index
        index = removal_entry_index(decode_raft_message(request), leader_id)
        if index is None:
            return False
        removal_index = index
        return True

    def is_removal_ack(request):
        message = decode_raft_message(request)
        return (message.msg_type == MSG_APPEND_RESPONSE and not message.reject
                and message.from_peer == leader_id and message.to == follower_id
                and removal_index is not None and message.index >= removal_index)

    with ThreadPoolExecutor(max_workers=1) as pool:
        with processes[1].proxy.hold_rpc(RAFT_SEND, is_removal_ack) as gate:
            with leader_process.proxy.hold_rpc_response(RAFT_SEND, is_removal) as response_gate:
                remove = pool.submit(requests.delete, f"{uris[0]}/cluster/peer/{leader_id}",
                                     params={"timeout": 8}, timeout=TIMEOUT)
                append = decode_raft_message(response_gate.wait_for_request(TIMEOUT))
                assert append.from_peer == follower_id
                assert append.commit < removal_index
                response_gate.release()
            gate.wait_for_request(TIMEOUT)

            # Withhold A's ACK while B's heartbeats continue. Submission is not completion.
            response = remove.result(timeout=TIMEOUT)
            assert response.status_code == 408, response.text
            survivor = _cluster(uris[1])
            assert survivor["raft_info"]["leader"] == follower_id
            assert survivor["raft_info"]["commit"] < removal_index
            departing = _cluster(uris[0])
            assert departing["raft_info"]["commit"] < removal_index
            assert set(departing["peers"]) == {str(leader_id), str(follower_id)}
            assert departing["consensus_thread_status"]["consensus_thread_status"] == "working"
            assert not gate.cancelled.is_set()
            if restart_departing:
                leader_process.kill()
                processes.remove(leader_process)
                # Restart before releasing the ACK, while removal is uncommitted.
                uris[0] = start_peer(peer_dirs[0], "restored_departing.log", bootstrap,
                                     port=leader_process.p2p_port, use_peer_proxy=True)
                leader_process = processes[-1]
            gate.release()

    wait_for(lambda: _cluster(uris[0])["consensus_thread_status"]["consensus_thread_status"] == "stopped")
    assert _cluster(uris[0])["raft_info"]["commit"] >= removal_index
    assert leader_process.proc.poll() is None
    _assert_operational([uris[1]], {follower_id})


def test_election_retries_while_departing_leader_stays_a_voter(tmp_path):
    uris, _, _ = start_cluster(tmp_path, 2, use_peer_proxy=True, extra_env={
        "QDRANT__CLUSTER__CONSENSUS__MESSAGE_TIMEOUT_TICKS": "600",
    })
    wait_for(all_peers_are_voters, uris)
    leader, follower = (_cluster(uri) for uri in uris)
    leader_id, follower_id = leader["peer_id"], follower["peer_id"]
    with ThreadPoolExecutor(max_workers=1) as pool:
        with processes[0].proxy.hold_rpc(
            RAFT_SEND, lambda request: decode_raft_message(request).msg_type == MSG_REQUEST_VOTE,
        ) as gate:
            remove = pool.submit(_remove, uris[0], leader_id)
            vote = decode_raft_message(gate.wait_for_request(TIMEOUT))
            # B must retry its election without an application-level vote retry loop.
            wait_for(lambda: _cluster(uris[1])["raft_info"]["term"] > vote.term)
            departing = _cluster(uris[0])
            assert departing["raft_info"]["term"] == leader["raft_info"]["term"]
            assert departing["raft_info"]["is_voter"]
            assert set(departing["peers"]) == {str(leader_id), str(follower_id)}
            assert not remove.done()
            gate.release()
            remove.result(timeout=TIMEOUT)
    _assert_operational([uris[1]], {follower_id})


def test_election_recovers_when_departing_peer_has_newest_log(tmp_path):
    uris, _, _ = start_cluster(tmp_path, 4, use_peer_proxy=True, extra_env={
        "QDRANT__CLUSTER__CONSENSUS__MESSAGE_TIMEOUT_TICKS": "600",
    })
    wait_for(all_peers_are_voters, uris)
    wait_for_same_applied_commit(uris)
    infos = [_cluster(uri) for uri in uris]
    departing_id = infos[0]["peer_id"]
    assert infos[0]["raft_info"]["leader"] == departing_id
    peers = list(processes)
    removal_index = None

    def is_removal(request):
        nonlocal removal_index
        index = removal_entry_index(decode_raft_message(request), departing_id)
        if index is None:
            return False
        removal_index = index
        return True

    def is_removal_ack(request):
        message = decode_raft_message(request)
        return (message.msg_type == MSG_APPEND_RESPONSE and not message.reject
                and message.from_peer == departing_id and removal_index is not None and message.index >= removal_index)

    def is_recovery_ack(request):
        message = decode_raft_message(request)
        return (message.msg_type == MSG_APPEND_RESPONSE and not message.reject
                and message.term > infos[0]["raft_info"]["term"] + 1
                and removal_index is not None and message.index > removal_index)

    with ThreadPoolExecutor(max_workers=1) as pool:
        with ExitStack() as stack:
            recovery_gate = stack.enter_context(peers[0].proxy.hold_rpc(RAFT_SEND, is_recovery_ack))
            # Only A and the new leader receive removal. The other two voters
            # still form a quorum after A is removed, even if the new leader dies.
            gates = {}
            for i in range(1, 4):
                gates[i] = stack.enter_context(peers[i].proxy.hold_rpc(
                    RAFT_SEND, lambda request: is_removal(request) or is_removal_ack(request),
                ))
            gate = stack.enter_context(peers[0].proxy.hold_rpc_response(RAFT_SEND, is_removal))
            remove = pool.submit(requests.delete, f"{uris[0]}/cluster/peer/{departing_id}",
                                 params={"timeout": 8}, timeout=TIMEOUT)
            append = decode_raft_message(gate.wait_for_request(TIMEOUT))
            new_leader = next(i for i, info in enumerate(infos) if info["peer_id"] == append.from_peer)
            for held in gates.values():
                held.wait_for_request(TIMEOUT)

            # Keep one survivor behind so only the other can ACK A's complete log.
            behind = next(i for i in range(1, 4) if i != new_leader)
            behind_gate = stack.enter_context(peers[behind].proxy.hold_rpc(
                RAFT_SEND, lambda request: (decode_raft_message(request).from_peer == departing_id
                                           and decode_raft_message(request).msg_type == MSG_APPEND),
            ))
            peers[new_leader].kill()
            processes.remove(peers[new_leader])
            response = remove.result(timeout=TIMEOUT)
            assert response.status_code == 408, response.text

            # Election retries may be needed before the only up-to-date voter wins.
            recovery_gate.wait_for_request(60)
            departing = _cluster(uris[0])
            assert departing["raft_info"]["leader"] == departing_id
            assert departing["raft_info"]["commit"] < removal_index
            recovery_gate.release()
            wait_for(lambda: _cluster(uris[0])["raft_info"]["leader"] != departing_id)
            behind_gate.release()

    survivor_ids = {info["peer_id"] for info in infos[1:]}
    reachable_survivors = [uri for i, uri in enumerate(uris) if i not in (0, new_leader)]
    wait_for(lambda: all(set(_cluster(uri)["peers"]) == {str(peer) for peer in survivor_ids}
                         for uri in reachable_survivors))
    # Remove the failed voter before collection placement can select it.
    _remove(reachable_survivors[0], infos[new_leader]["peer_id"], force=True)
    _assert_operational(reachable_survivors, survivor_ids - {infos[new_leader]["peer_id"]})


def test_new_leader_disappears_before_removal(tmp_path):
    uris, peer_dirs, bootstrap = start_cluster(tmp_path, 2, use_peer_proxy=True)
    wait_for(all_peers_are_voters, uris)
    leader, follower = (_cluster(uri) for uri in uris)
    leader_id, follower_id = leader["peer_id"], follower["peer_id"]
    leader_process, follower_process = processes

    def new_leader_append(request):
        message = decode_raft_message(request)
        return message.msg_type == MSG_APPEND and message.from_peer == follower_id and message.to == leader_id

    with ThreadPoolExecutor(max_workers=1) as pool:
        # B has won the election, but A has not yet learned that B is leader.
        # Killing B here leaves the removal outside the Raft log.
        with leader_process.proxy.hold_rpc(RAFT_SEND, new_leader_append) as gate:
            remove = pool.submit(requests.delete, f"{uris[0]}/cluster/peer/{leader_id}",
                                 params={"timeout": 8}, timeout=TIMEOUT)
            request = gate.wait_for_request(TIMEOUT)
            assert removal_entry_index(decode_raft_message(request), leader_id) is None
            assert _cluster(uris[1])["raft_info"]["leader"] == follower_id
            follower_process.kill()
            processes.remove(follower_process)
            response = remove.result(timeout=TIMEOUT)
            assert response.status_code == 408, response.text
            assert set(_cluster(uris[0])["peers"]) == {str(leader_id), str(follower_id)}

    uris[1] = start_peer(peer_dirs[1], "restored_follower.log", bootstrap,
                         port=follower_process.p2p_port, use_peer_proxy=True)
    _assert_operational(uris, {leader_id, follower_id})


@pytest.mark.parametrize("remove_leader", [False, True], ids=["faulty-follower", "faulty-leader"])
def test_remove_unavailable_peer_with_surviving_majority(tmp_path, remove_leader):
    uris, _, _ = start_cluster(tmp_path, 3)
    wait_for(all_peers_are_voters, uris)
    infos = [_cluster(uri) for uri in uris]
    leader_id = infos[0]["raft_info"]["leader"]
    removed = next(i for i, info in enumerate(infos) if (info["peer_id"] == leader_id) == remove_leader)
    peer = processes[removed]
    peer.kill()
    processes.remove(peer)
    survivors = [uri for i, uri in enumerate(uris) if i != removed]
    survivor_ids = {info["peer_id"] for i, info in enumerate(infos) if i != removed}
    wait_for(lambda: all(_cluster(uri)["raft_info"]["leader"] in survivor_ids for uri in survivors))
    _remove(survivors[0], infos[removed]["peer_id"], force=True)
    _assert_operational(survivors, survivor_ids)


def test_force_removal_preserves_shard_check_semantics(tmp_path):
    uris, _, bootstrap = start_cluster(tmp_path, 1)
    leader_id = _cluster(uris[0])["peer_id"]
    response = requests.put(f"{uris[0]}/collections/only_copy", json={
        "vectors": {"size": 4, "distance": "Dot"}, "shard_number": 1, "replication_factor": 1,
    }, timeout=TIMEOUT)
    assert_http_ok(response)
    uris.append(start_peer(make_peer_folder(tmp_path, 1), "follower.log", bootstrap))
    wait_for(all_peers_are_voters, uris)
    wait_for_same_applied_commit(uris)
    follower_id = _cluster(uris[1])["peer_id"]

    response = requests.delete(f"{uris[0]}/cluster/peer/{leader_id}",
                               params={"force": "false"}, timeout=TIMEOUT)
    assert response.status_code == 400, response.text
    assert "shards on it" in response.text
    # Force still permits dropping the only copy. Leader resignation must not
    # add a new requirement to replicate or drain those shards first.
    _remove(uris[0], leader_id, force=True)
    _assert_operational([uris[1]], {follower_id})


@pytest.mark.parametrize("different_receivers", [False, True])
def test_concurrent_removals_cannot_remove_all_voters(tmp_path, different_receivers):
    uris, _, _ = start_cluster(tmp_path, 2)
    wait_for(all_peers_are_voters, uris)
    peer_ids = [_cluster(uri)["peer_id"] for uri in uris]
    with ThreadPoolExecutor(max_workers=2) as pool:
        calls = [pool.submit(requests.delete, f"{uris[i if different_receivers else 0]}/cluster/peer/{peer_id}",
                             params={"timeout": 5, "force": "true"}, timeout=TIMEOUT)
                 for i, peer_id in enumerate(peer_ids)]
        responses = [call.result(timeout=TIMEOUT) for call in calls]
    assert sum(response.ok for response in responses) == 1, [response.text for response in responses]
    removed = next(i for i, response in enumerate(responses) if response.ok)
    survivor = 1 - removed
    _assert_operational([uris[survivor]], {peer_ids[survivor]})


def test_remove_leader_after_bootstrap_peer_is_gone(tmp_path):
    uris, _, _ = start_cluster(tmp_path, 3, use_peer_proxy=True, extra_env={
        "QDRANT__CLUSTER__CONSENSUS__MESSAGE_TIMEOUT_TICKS": "600",
    })
    wait_for(all_peers_are_voters, uris)
    infos = [_cluster(uri) for uri in uris]
    peers = list(processes)
    peers[0].kill()
    processes.remove(peers[0])
    survivor_ids = {info["peer_id"] for info in infos[1:]}
    wait_for(lambda: all(_cluster(uri)["raft_info"]["leader"] in survivor_ids for uri in uris[1:]))
    _remove(uris[1], infos[0]["peer_id"], force=True)
    wait_for(lambda: all(set(_cluster(uri)["peers"]) == {str(peer) for peer in survivor_ids} for uri in uris[1:]))
    leader_id = _cluster(uris[1])["raft_info"]["leader"]
    departing = next(i for i in (1, 2) if infos[i]["peer_id"] == leader_id)
    survivor = 3 - departing

    def is_removal(request):
        message = decode_raft_message(request)
        return removal_entry_index(message, leader_id) is not None

    with ThreadPoolExecutor(max_workers=1) as pool:
        # The final notification must not depend on looking up the departing
        # peer through the original bootstrap server, which is no longer alive.
        with peers[departing].proxy.hold_rpc_response(RAFT_SEND, is_removal) as gate:
            remove = pool.submit(_remove, uris[departing], leader_id, 8, True)
            gate.wait_for_request(TIMEOUT)
            wait_for(lambda: set(_cluster(uris[survivor])["peers"]) == {str(infos[survivor]["peer_id"])})
            gate.release()
            remove.result(timeout=TIMEOUT)
    _assert_operational([uris[survivor]], {infos[survivor]["peer_id"]})


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
