import multiprocessing
import pathlib

from .fixtures import upsert_random_points, create_collection
from .utils import *

N_PEERS = 3
N_SHARDS = 1
N_REPLICA = 3
COLLECTION_NAME = "test_triple_replication"

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


def test_triple_replication(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0], collection=COLLECTION_NAME, shard_number=N_SHARDS, replication_factor=N_REPLICA)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris
    )

    upload_process = run_update_points_in_background(peer_api_uris[1], COLLECTION_NAME)

    time.sleep(0.3)

    # Kill leader peer
    killed_id = 0
    p = processes.pop(killed_id)
    p.kill()
    # Make sure it is completely gone to be able to reuse the data on disk
    if p.proc.returncode is None:
        print(f"Waiting for leader peer {p.pid} to go down")
        p.proc.wait()
    peer_api_uris.pop(killed_id)

    new_url = start_peer(peer_dirs[killed_id], f"peer_{killed_id}_restarted.log", bootstrap_uri)
    peer_api_uris.append(new_url)

    wait_for_peer_online(new_url)

    time.sleep(0.3)

    upload_process.kill()

    timeout = 10
    while True:
        if timeout < 0:
            raise Exception("Timeout waiting for all replicas to be active")

        all_active = True
        points_counts = set()
        for peer_api_uri in peer_api_uris:
            count = get_collection_point_count(peer_api_uri, COLLECTION_NAME, exact=True)
            points_counts.add(count)

            res = check_collection_cluster(peer_api_uri, COLLECTION_NAME)
            if res['state'] != 'Active':
                all_active = False

        if all_active:
            if len(points_counts) != 1:
                with open("test_triple_replication.log", "w") as f:
                    for peer_api_uri in peer_api_uris:
                        collection_name = COLLECTION_NAME
                        res = requests.get(f"{peer_api_uri}/collections/{collection_name}/cluster", timeout=10)
                        f.write(f"{peer_api_uri} {res.json()['result']}\n")
                    for peer_api_uri in peer_api_uris:
                        res = requests.get(f"{peer_api_uri}/cluster", timeout=10)
                        f.write(f"{peer_api_uri} {res.json()['result']}\n")

                for peer_api_uri in peer_api_uris:
                    count = get_collection_point_count(peer_api_uri, COLLECTION_NAME, exact=True)
                    print(count)

                assert False, f"Points count is not equal on all peers: {points_counts}"
            break

        time.sleep(1)
        timeout -= 1
