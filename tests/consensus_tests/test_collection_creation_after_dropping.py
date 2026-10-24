"""Recreate a collection in a cluster, and prove what happens to a lost forwarded proposal.

A collection operation sent to a follower reaches the leader as a single
forwarded Raft MsgPropose. Nothing re-proposes it if that message is lost,
so the request times out and the operation is never committed. The old
version of this test hit that at random, whenever an unlucky leader change
or a lost message coincided with a follower-routed request. The proxy tests
below drop the forwarded proposal on purpose instead.
"""

import pathlib
from concurrent.futures import ThreadPoolExecutor

import pytest

from .assertions import assert_http_ok
from .fixtures import upsert_random_points
from .raft_messages import MSG_PROPOSE, decode_raft_message
from .utils import *

N_PEERS = 5
RAFT_SEND = "/qdrant.Raft/Send"
# Server-side wait for the collection operation to be applied
OPERATION_TIMEOUT_SEC = 10
REQUEST_TIMEOUT_SEC = OPERATION_TIMEOUT_SEC + 20

COLLECTION_CONFIG = {
    "vectors": {
        "size": 4,
        "distance": "Dot",
    },
    "shard_number": 8,
    "replication_factor": 2,
}


def _leader_uri(peer_api_uris):
    leader_id = get_leader(peer_api_uris[0])
    for uri in peer_api_uris:
        if get_cluster_info(uri)["peer_id"] == leader_id:
            return uri
    raise AssertionError(f"Leader {leader_id} is not among {peer_api_uris}")


def _create(uri, collection_name, timeout=30):
    return requests.put(
        f"{uri}/collections/{collection_name}",
        json=COLLECTION_CONFIG,
        params={"timeout": timeout},
        timeout=timeout + 20,
    )


def _delete(uri, collection_name, timeout=OPERATION_TIMEOUT_SEC):
    return requests.delete(
        f"{uri}/collections/{collection_name}",
        params={"timeout": timeout},
        timeout=timeout + 20,
    )


def _collection_exists(uri, collection_name):
    response = requests.get(f"{uri}/collections/{collection_name}/exists", timeout=10)
    assert_http_ok(response)
    return response.json()["result"]["exists"]


def test_collection_creation_after_dropping(tmp_path: pathlib.Path):
    assert_project_root()
    peer_api_uris, _, _ = start_cluster(tmp_path, N_PEERS)

    for uri in peer_api_uris:
        r = requests.get(f"{uri}/collections", timeout=10)
        assert_http_ok(r)
        assert len(r.json()["result"]["collections"]) == 0

    # Send each collection operation to the current leader so it is proposed
    # locally. Forwarding from a follower is covered by the proxy test below.
    assert_http_ok(_create(_leader_uri(peer_api_uris), "test_collection"))

    for _ in range(10):
        # Drop and immediately recreate the collection
        assert_http_ok(_delete(_leader_uri(peer_api_uris), "test_collection"))
        assert_http_ok(_create(_leader_uri(peer_api_uris), "test_collection"))

        # Upload some points to every peer
        for _ in range(20):
            for peer_api_uri in peer_api_uris:
                upsert_random_points(
                    peer_api_uri,
                    collection_name="test_collection",
                    num=50,
                    wait="false",
                    with_sparse_vector=False,
                )

    for uri in peer_api_uris:
        r = requests.get(f"{uri}/collections/test_collection", timeout=10)
        assert_http_ok(r)

    wait_collection_exists_and_active_on_all_peers(
        collection_name="test_collection", peer_api_uris=peer_api_uris
    )


# Serialized tags of CollectionMetaOperations (serde snake_case), matched in the CBOR entry
@pytest.mark.parametrize("operation", ["create_collection", "delete_collection"])
@pytest.mark.parametrize("delivery", [
    pytest.param("delivered", id="delivered-control"),
    # Fails until a lost forwarded proposal is re-proposed
    pytest.param("dropped", id="dropped"),
])
def test_forwarded_collection_op(tmp_path: pathlib.Path, operation, delivery):
    """A follower-routed collection operation must commit, even if its first
    forwarded MsgPropose is lost.

    In the "dropped" case, the leader's proxy holds the forwarded proposal until
    the follower's Raft send deadline (message_timeout_ticks * tick_period_ms,
    1 s by default) cancels it, so the leader never receives that message.
    Later messages pass through, so a re-proposal would still get through.
    """
    assert_project_root()
    collection_name = "forwarded_collection"
    peer_api_uris, _, _ = start_cluster(tmp_path, 3, use_peer_proxy=True)
    wait_for(all_peers_are_voters, peer_api_uris)
    wait_for_same_applied_commit(peer_api_uris)

    infos = [get_cluster_info(uri) for uri in peer_api_uris]
    leader_id = infos[0]["raft_info"]["leader"]
    assert all(info["raft_info"]["leader"] == leader_id for info in infos)
    leader_index = next(i for i, info in enumerate(infos) if info["peer_id"] == leader_id)
    follower_index = next(i for i, info in enumerate(infos) if info["peer_id"] != leader_id)
    leader_uri, follower_uri = peer_api_uris[leader_index], peer_api_uris[follower_index]
    follower_id = infos[follower_index]["peer_id"]
    leader_proxy = processes[leader_index].proxy

    if operation == "delete_collection":
        assert_http_ok(_create(leader_uri, collection_name))
        wait_collection_exists_and_active_on_all_peers(collection_name, peer_api_uris)
    existed_before = operation == "delete_collection"

    op_name = operation.encode()
    name = collection_name.encode()

    def is_forwarded_operation(request):
        message = decode_raft_message(request)
        return (message.msg_type == MSG_PROPOSE
                and message.from_peer == follower_id and message.to == leader_id
                and any(op_name in entry.data and name in entry.data for entry in message.entries))

    send = _create if operation == "create_collection" else _delete

    with ThreadPoolExecutor(max_workers=1) as pool:
        with leader_proxy.hold_rpc(RAFT_SEND, is_forwarded_operation) as gate:
            pending = pool.submit(send, follower_uri, collection_name, OPERATION_TIMEOUT_SEC)
            proposal = decode_raft_message(gate.wait_for_request(REQUEST_TIMEOUT_SEC))
            assert proposal.from_peer == follower_id

            if delivery == "dropped":
                assert gate.cancelled.wait(REQUEST_TIMEOUT_SEC), \
                    "Follower's Raft send did not hit its deadline while the proposal was held"
            gate.release()

        response = pending.result(timeout=REQUEST_TIMEOUT_SEC)

    wait_for_same_applied_commit(peer_api_uris)
    states = {uri: _collection_exists(uri, collection_name) for uri in peer_api_uris}

    if response.status_code != 200:
        error = response.json()["status"]["error"]
        assert "Waiting for consensus operation commit failed" in error, error
        # The operation must not be partially applied anywhere
        assert all(exists == existed_before for exists in states.values()), states
        pytest.fail(f"Lost forwarded proposal: {operation} via follower was never committed: {error}")

    assert all(exists != existed_before for exists in states.values()), states
    if operation == "create_collection":
        wait_collection_exists_and_active_on_all_peers(collection_name, peer_api_uris)
