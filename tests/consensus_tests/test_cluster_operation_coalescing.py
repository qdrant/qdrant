import multiprocessing
import pathlib

from .fixtures import create_collection, drop_collection
from .utils import *

N_PEERS = 3
N_SHARDS = 1
N_REPLICA = 3


def delete_collection_in_loop(peer_url, collection_name):
    while True:
        # fails if the response is NOT successful
        drop_collection(peer_url, collection_name)


# Triggers repeatedly at most 2 concurrent collection deletions
def run_delete_collection_in_background(peer_url, collection_name):
    p1 = multiprocessing.Process(target=delete_collection_in_loop, args=(peer_url, collection_name))
    p2 = multiprocessing.Process(target=delete_collection_in_loop, args=(peer_url, collection_name))
    p1.start()
    p2.start()
    return [p1, p2]


def test_cluster_operation_coalescing(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA)
    wait_collection_exists_and_active_on_all_peers(
        collection_name="test_collection",
        peer_api_uris=peer_api_uris
    )

    [delete_p1, delete_p2] = run_delete_collection_in_background(peer_api_uris[1], "test_collection")

    time.sleep(3)

    # check that the delete processes did not fail
    if not delete_p1.is_alive() or not delete_p2.is_alive():
        raise Exception("Parallel delete processes failed")

    delete_p1.kill()
    delete_p2.kill()
