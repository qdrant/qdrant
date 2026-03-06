import logging
import pathlib

from .fixtures import create_collection, upsert_random_points, search, random_dense_vector
from .utils import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

N_PEERS = 3
N_SHARDS = 1
N_REPLICAS = 3
COLLECTION_NAME = "test_collection"


def get_states(peer_api_uri: str, collection_name: str):
    res = requests.get(f"{peer_api_uri}/collections/{collection_name}/cluster", timeout=10)
    assert_http_ok(res)
    cluster = res.json()["result"]
    all_shard_states = []
    for shard in cluster['local_shards']:
        all_shard_states.append(shard['state'])
    for shard in cluster['remote_shards']:
        all_shard_states.append(shard['state'])

    return all_shard_states


def has_listener_shard(peer_api_uri: str, collection_name: str):
    return 'Listener' in get_states(peer_api_uri, collection_name)


def has_no_listener_shard(peer_api_uri: str, collection_name: str):
    return 'Listener' not in get_states(peer_api_uri, collection_name)


def wait_listener_node(peer_api_uri: str, collection_name: str):
    try:
        wait_for(has_listener_shard, peer_api_uri, collection_name)
    except Exception as e:
        print_collection_cluster_info(peer_api_uri, collection_name)
        raise e

def wait_no_listener_node(peer_api_uri: str, collection_name: str):
    try:
        wait_for(has_no_listener_shard, peer_api_uri, collection_name)
    except Exception as e:
        print_collection_cluster_info(peer_api_uri, collection_name)
        raise e


def test_listener_node(tmp_path: pathlib.Path):

    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICAS)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris)

    upsert_random_points(peer_api_uris[0], 100)

    p = processes.pop()
    p.kill()

    peer_api_uris.pop()

    peer_api_uris.append(
        start_peer(
            peer_dirs[-1],
            f"peer_0_{N_PEERS}_restart.log",
            bootstrap_uri,
            port=20000,
            extra_env={
                "QDRANT__STORAGE__NODE_TYPE": "Listener",
            }
        )
    )

    for peer_api_uri in peer_api_uris:
        wait_listener_node(peer_api_uri, COLLECTION_NAME)

    upsert_random_points(peer_api_uris[0], 100)

    query_vector = random_dense_vector()
    res1 = search(peer_api_uris[0], query_vector, city="London", collection=COLLECTION_NAME)

    p = processes.pop()
    p.kill()

    peer_api_uris.pop()

    peer_api_uris.append(
        start_peer(
            peer_dirs[-1],
            f"peer_0_{N_PEERS}_restart_again.log",
            bootstrap_uri,
            port=20000,
            extra_env={
                "QDRANT__STORAGE__NODE_TYPE": "Normal",
            }
        )
    )

    for peer_api_uri in peer_api_uris:
        wait_no_listener_node(peer_api_uri, COLLECTION_NAME)

    res2 = search(peer_api_uris[0], query_vector, city="London", collection=COLLECTION_NAME)

    assert res1 == res2
