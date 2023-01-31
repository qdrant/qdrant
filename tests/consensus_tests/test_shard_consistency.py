import multiprocessing
import pathlib
import random

from .fixtures import create_collection, upsert_random_points
from .utils import *

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

N_PEERS = 3
N_SHARDS = 1
N_REPLICAS = 3
COLLECTION_NAME = "test_collection"


def update_points_in_loop(peer_url, collection_name):
    limit = 5
    while True:
        offset = random.randint(0, 100)
        upsert_random_points(peer_url, limit, collection_name, offset=offset, wait='false', ordering='strong')


def run_update_points_in_background(peer_url, collection_name):
    p = multiprocessing.Process(target=update_points_in_loop, args=(peer_url, collection_name))
    p.start()
    return p


def get_all_points(peer_url, collection_name):
    res = requests.post(
        f"{peer_url}/collections/{collection_name}/points/scroll",
        json={
            "limit": 100,
            "with_vector": True,
            "with_payload": True,
        },
        timeout=10
    )
    assert_http_ok(res)
    return res.json()["result"]


def test_shard_consistency(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICAS)
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)

    # upload points to the leader
    upload_processes = [
        run_update_points_in_background(peer_api_uris[i], "test_collection")
        for i in range(len(peer_api_uris))
    ]

    print("Push points during 5 seconds")
    time.sleep(5)

    # Kill all upload processes
    for p in upload_processes:
        p.kill()

    # make sure replication is done
    time.sleep(1)

    # Validate that all peers have the same data
    results = []
    for url in peer_api_uris:
        res = get_all_points(url, COLLECTION_NAME)
        results.append(res)

    for res in results:
        for idx, row in enumerate(res['points']):
            assert row == results[0]['points'][idx]
