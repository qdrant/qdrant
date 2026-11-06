"""A peer joining while another conf change is uncommitted must not be dropped by the leader."""

import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from .assertions import assert_http_ok
from .utils import *


def test_join_while_peer_removal_is_pending(tmp_path):
    assert_project_root()

    # Election timeout is 20-40 ticks: keep it longer than the follower is stopped below. Otherwise
    # the resumed follower campaigns with its shorter log and can keep the cluster leaderless.
    peer_api_uris, peer_dirs, _bootstrap_uri = start_cluster(
        tmp_path, 2, extra_env={"QDRANT__CLUSTER__CONSENSUS__TICK_PERIOD_MS": "500"},
    )
    wait_for(all_peers_are_voters, peer_api_uris)
    wait_for_same_applied_commit(peer_api_uris)

    infos = [get_cluster_info(uri) for uri in peer_api_uris]
    leader_index = next(index for index, info in enumerate(infos) if info["peer_id"] == infos[0]["raft_info"]["leader"])
    follower_index = 1 - leader_index
    leader_uri = peer_api_uris[leader_index]
    leader_info = infos[leader_index]
    leader_p2p_uri = leader_info["peers"][str(leader_info["peer_id"])]["uri"]
    follower_id = get_cluster_info(peer_api_uris[follower_index])["peer_id"]
    follower_pid = processes[follower_index].proc.pid

    # With the follower stopped, its removal can't commit, so it stays a pending conf change on the leader
    os.kill(follower_pid, signal.SIGSTOP)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            removal = pool.submit(
                requests.delete,
                f"{leader_uri}/cluster/peer/{follower_id}",
                params={"timeout": 60},
                timeout=90,
            )
            time.sleep(1)

            # The new peer's AddPeer reaches the leader while the removal is pending
            new_peer_dir = make_peer_folder(tmp_path, 2)
            new_peer_uri = start_peer(new_peer_dir, "peer_0_2.log", leader_p2p_uri)
            time.sleep(3)

            os.kill(follower_pid, signal.SIGCONT)
            assert_http_ok(removal.result(timeout=90))
    finally:
        os.kill(follower_pid, signal.SIGCONT)

    # The new peer gives up and exits if its AddPeer isn't committed within 10 seconds
    wait_for_peer_online(new_peer_uri)
    new_peer_id = get_cluster_info(new_peer_uri)["peer_id"]
    wait_for(
        lambda: set(get_cluster_info(leader_uri)["peers"]) == {str(leader_info["peer_id"]), str(new_peer_id)}
    )
