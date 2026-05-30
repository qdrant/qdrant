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

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, 20000)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA)
    wait_collection_exists_and_active_on_all_peers(
        collection_name="test_collection",
        peer_api_uris=peer_api_uris
    )

    # upload points to the leader
    upload_process = run_update_points_in_background(peer_api_uris[0], "test_collection")

    time.sleep(0.3)

    # Kill third peer
    p = processes.pop()
    p.kill()
    peer_api_uris.pop()

    # Kill second peer
    p = processes.pop()
    p.kill()
    peer_api_uris.pop()

    time.sleep(0.3)

    # Stop pushing points to the leader
    upload_process.kill()

    # Restart third peer on same ports
    new_url_3 = start_peer(peer_dirs[2], f"peer_{3}_restarted.log", bootstrap_uri, 20200)
    peer_api_uris.append(new_url_3)

    # Restart second peer on same ports
    new_url_2 = start_peer(peer_dirs[1], f"peer_{2}_restarted.log", bootstrap_uri, 20100)
    peer_api_uris.append(new_url_2)

    # Wait for peers to be online
    wait_for_peer_online(new_url_3)
    time.sleep(1)
    wait_for_peer_online(new_url_2)

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

