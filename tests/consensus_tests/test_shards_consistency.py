import multiprocessing
import pathlib
import random
from datetime import datetime

from .fixtures import create_collection, upsert_random_points
from .utils import *

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

N_PEERS = 5
N_SHARDS = 1
N_REPLICAS = 5
COLLECTION_NAME = "test_collection"


def update_points_in_loop(peer_url, collection_name):
    limit = 5
    while True:
        offset = random.randint(0, 10)
        upsert_random_points(peer_url, limit, collection_name, offset=offset, wait='false')

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
    upload_process1 = run_update_points_in_background(peer_api_uris[0], "test_collection")

    # upload points to the follower
    upload_process2 = run_update_points_in_background(peer_api_uris[1], "test_collection")

    print("Waiting for 5 seconds")
    time.sleep(5)

    upload_process1.kill()
    upload_process2.kill()

    res1 = get_all_points(peer_api_uris[0], COLLECTION_NAME)
    res2 = get_all_points(peer_api_uris[1], COLLECTION_NAME)
    res3 = get_all_points(peer_api_uris[2], COLLECTION_NAME)
    res4 = get_all_points(peer_api_uris[3], COLLECTION_NAME)
    res5 = get_all_points(peer_api_uris[4], COLLECTION_NAME)

    # Expected: res1 == res2 == res3 == res4 == res5

    for zip_res in zip(res1, res2, res3, res4, res5):
        assert zip_res[0] == zip_res[1]
        assert zip_res[1] == zip_res[2]
        assert zip_res[2] == zip_res[3]
        assert zip_res[3] == zip_res[4]


