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
# Random offsets are in [0, 100] with batches of up to 5 points.
MAX_POINT_ID = 105


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
            "limit": MAX_POINT_ID,
            "with_vector": True,
            "with_payload": True,
        },
        timeout=10
    )
    assert_http_ok(res)
    return res.json()["result"]


def get_points_by_id(peer_url, collection_name):
    return {point["id"]: point for point in get_all_points(peer_url, collection_name)["points"]}


def peers_have_consistent_data(peer_api_uris, collection_name):
    for peer_api_uri in peer_api_uris:
        cluster_info = check_collection_cluster(peer_api_uri, collection_name)
        if cluster_info["state"] != "Active":
            return False

    points_by_peer = [get_points_by_id(url, collection_name) for url in peer_api_uris]
    point_counts = {len(points) for points in points_by_peer}
    if len(point_counts) != 1:
        return False

    reference = points_by_peer[0]
    return all(points == reference for points in points_by_peer[1:])


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

    # Active state doesn't imply async replication of in-flight writes is done.
    wait_for(peers_have_consistent_data, peer_api_uris, COLLECTION_NAME)

    # Validate that all peers have the same data
    reference_points = get_points_by_id(peer_api_uris[0], COLLECTION_NAME)
    for url in peer_api_uris[1:]:
        assert get_points_by_id(url, COLLECTION_NAME) == reference_points
