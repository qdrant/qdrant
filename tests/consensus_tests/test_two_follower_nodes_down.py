import multiprocessing
import pathlib

from .fixtures import upsert_random_points, create_collection
from .utils import *

N_PEERS = 3
N_SHARDS = 1
N_REPLICA = 3


def update_points_in_loop(peer_url, collection_name):
    offset = 0
    limit = 3
    while True:
        upsert_random_points(peer_url, limit, collection_name, offset=offset)
        offset += limit
        # time.sleep(0.01)


def run_update_points_in_background(peer_url, collection_name):
    p = multiprocessing.Process(target=update_points_in_loop, args=(peer_url, collection_name))
    p.start()
    return p


# Test that a 3 nodes cluster can recover from two follower nodes down.
def test_two_follower_nodes_down(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA)
    wait_collection_exists_and_active_on_all_peers(
        collection_name="test_collection",
        peer_api_uris=peer_api_uris
    )

    leader_index = get_leader_index(peer_api_uris)
    # Descending, so popping one index doesn't shift the other
    follower_indices = sorted((index for index in range(N_PEERS) if index != leader_index), reverse=True)

    # upload points to the leader
    upload_process = run_update_points_in_background(peer_api_uris[leader_index], "test_collection")

    time.sleep(0.3)

    # Kill both followers
    restart_ports = {}
    for index in follower_indices:
        p = processes.pop(index)
        restart_ports[index] = p.p2p_port
        p.kill()
        peer_api_uris.pop(index)

    time.sleep(0.3)

    # Stop pushing points to the leader
    upload_process.kill()

    # Restart followers on same ports
    new_urls = []
    for index in follower_indices:
        new_url = start_peer(peer_dirs[index], f"peer_{index}_restarted.log", bootstrap_uri, restart_ports[index])
        peer_api_uris.append(new_url)
        new_urls.append(new_url)

    # Wait for peers to be online
    for new_url in new_urls:
        wait_for_peer_online(new_url)
        time.sleep(1)

    timeout = 10
    while True:
        if timeout < 0:
            raise Exception("Timeout waiting for all replicas to be active")

        all_active = True
        points_counts = set()
        for peer_api_uri in peer_api_uris:
            res = check_collection_cluster(peer_api_uri, "test_collection")
            points_counts.add(res['points_count'])
            if res['state'] != 'Active':
                all_active = False

        if all_active:
            if len(points_counts) != 1:
                with open("test_triple_replication.log", "w") as f:
                    for peer_api_uri in peer_api_uris:
                        collection_name = "test_collection"
                        res = requests.get(f"{peer_api_uri}/collections/{collection_name}/cluster", timeout=10)
                        f.write(f"{peer_api_uri} {res.json()['result']}\n")
                    for peer_api_uri in peer_api_uris:
                        res = requests.get(f"{peer_api_uri}/cluster", timeout=10)
                        f.write(f"{peer_api_uri} {res.json()['result']}\n")

                for peer_api_uri in peer_api_uris:
                    res = requests.post(f"{peer_api_uri}/collections/test_collection/points/count", json={"exact": True})
                    print(res.json())

                assert False, f"Points count is not equal on all peers: {points_counts}"
            break

        time.sleep(1)
        timeout -= 1

