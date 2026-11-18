import pathlib
import time

import grpc
import pytest

from .raft_messages import MSG_HEARTBEAT, encode_raft_message
from .utils import (
    assert_project_root,
    get_cluster_info,
    get_leader,
    processes,
    start_cluster,
    wait_for_uniform_cluster_status,
)

N_PEERS = 3
RAFT_SEND = "/qdrant.Raft/Send"
# Peer ids that are not part of the cluster, e.g. peers of another cluster
FOREIGN_PEER_ID = 4242424242424242
UNKNOWN_PEER_ID = FOREIGN_PEER_ID + 1
TERM_JUMP = 1000


def get_raft_info(peer_api_uri: str) -> dict:
    return get_cluster_info(peer_api_uri)["raft_info"]


def send_raft_message(p2p_port: int, request: bytes):
    """Deliver a raw Raft message; fail unless the peer accepted or rejected it."""
    with grpc.insecure_channel(f"127.0.0.1:{p2p_port}") as channel:
        send = channel.unary_unary(RAFT_SEND)
        try:
            send(request, timeout=10)
        except grpc.RpcError as err:
            assert err.code() == grpc.StatusCode.FAILED_PRECONDITION, (
                f"Raft message was not delivered: {err.code()} {err.details()}"
            )


@pytest.mark.parametrize("addressed_to", ["unknown_peer", "sibling_peer"])
def test_raft_message_for_other_peer_is_ignored(tmp_path: pathlib.Path, addressed_to):
    """A peer must ignore a Raft message addressed to another peer id.
    Its higher term must not reach this peer's Raft state."""
    assert_project_root()

    peer_api_uris, _, _ = start_cluster(tmp_path, N_PEERS)
    leader = get_leader(peer_api_uris[0])

    victim_index = next(
        index
        for index, uri in enumerate(peer_api_uris)
        if get_cluster_info(uri)["peer_id"] != leader
    )
    victim_uri = peer_api_uris[victim_index]
    victim_p2p_port = processes[victim_index].p2p_port

    foreign_term = get_raft_info(victim_uri)["term"] + TERM_JUMP
    to = UNKNOWN_PEER_ID if addressed_to == "unknown_peer" else leader

    send_raft_message(
        victim_p2p_port,
        encode_raft_message(
            msg_type=MSG_HEARTBEAT,
            to=to,
            from_peer=FOREIGN_PEER_ID,
            term=foreign_term,
        ),
    )

    # Give the consensus thread a few ticks to step the message
    time.sleep(3)

    for uri in peer_api_uris:
        raft_info = get_raft_info(uri)
        assert raft_info["term"] < foreign_term, (
            f"Peer {uri} took the term of a message for peer {to}: {raft_info}"
        )
        assert raft_info["leader"] == leader, (
            f"Peer {uri} changed leader after a message for peer {to}: {raft_info}"
        )

    wait_for_uniform_cluster_status(peer_api_uris, leader)
