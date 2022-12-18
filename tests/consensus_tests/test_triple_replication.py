import multiprocessing
import pathlib

from .fixtures import upsert_random_points, create_collection
from .utils import *
from .assertions import assert_http_ok

N_PEERS = 3
N_SHARDS = 1
N_REPLICA = 3


def check_collection_cluster(peer_url, collection_name):
    res = requests.get(f"{peer_url}/collections/{collection_name}/cluster", timeout=10)
    assert_http_ok(res)
    return res.json()["result"]['local_shards'][0]


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


def test_recover_dead_node(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA)
    wait_collection_exists_and_active_on_all_peers(
        collection_name="test_collection",
        peer_api_uris=peer_api_uris
    )

    upload_process = run_update_points_in_background(peer_api_uris[1], "test_collection")

    time.sleep(0.3)

    # Kill last peer
    killed_id = 0
    p = processes.pop(killed_id)
    p.kill()
    peer_api_uris.pop(killed_id)

    new_url = start_peer(peer_dirs[killed_id], f"peer_{killed_id}_restarted.log", bootstrap_uri)
    peer_api_uris.append(new_url)

    wait_for_peer_online(new_url)

    time.sleep(0.3)

    upload_process.terminate()

    all_active = True
    for peer_api_uri in peer_api_uris:
        res = check_collection_cluster(peer_api_uri, "test_collection")
        print(peer_api_uri, res)
        if res['state'] != 'Active':
            all_active = False

    if not all_active:
        time.sleep(2)
        print("-----------------")

        for peer_api_uri in peer_api_uris:
            res = check_collection_cluster(peer_api_uri, "test_collection")
            print(peer_api_uri, res)
