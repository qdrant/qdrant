import pathlib

import pytest
import requests

from .utils import *

N_PEERS = 3


def consensus_status(peer_api_uri: str) -> str | None:
    try:
        return get_cluster_info(peer_api_uri)["consensus_thread_status"]["consensus_thread_status"]
    except Exception:
        return None


@pytest.mark.parametrize("removed_while", ["running", "down"])
def test_removed_peer_restart_stops_consensus(tmp_path: pathlib.Path, removed_while: str):
    """A peer removed from consensus must stop consensus after a restart with its old storage."""
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)
    removed_uri = peer_api_uris[-1]
    removed_id = get_cluster_info(removed_uri)["peer_id"]

    if removed_while == "down":
        # The peer never sees its own `RemoveNode`
        victim = processes.pop()
        victim.kill()
        port = victim.p2p_port

    res = requests.delete(f"{peer_api_uris[0]}/cluster/peer/{removed_id}?timeout=60")
    assert_http_ok(res)

    if removed_while == "running":
        wait_for(lambda: consensus_status(removed_uri) == "stopped")
        victim = processes.pop()
        victim.kill()
        port = victim.p2p_port

    removed_uri = start_peer(peer_dirs[-1], "peer_removed_restarted.log", bootstrap_uri, port=port)
    wait_for(lambda: consensus_status(removed_uri) is not None)

    # The restarted peer must notice it is no longer a member
    wait_for(lambda: consensus_status(removed_uri) == "stopped", wait_for_timeout=30)

    # And the cluster must not take it back
    peers = get_cluster_info(peer_api_uris[0])["peers"]
    assert str(removed_id) not in peers
    assert len(peers) == N_PEERS - 1
