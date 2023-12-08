import multiprocessing
import pathlib

from .fixtures import create_collection, insert_random_point
from .utils import *

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

N_PEERS = 3
N_SHARDS = 1
N_REPLICAS = 3
COLLECTION_NAME = "test_collection"
POINTS_COUNT = 20000


def update_points_in_loop(peer_url, collection_name):
    for i in range(POINTS_COUNT):
        insert_random_point(peer_url, i, collection_name, wait='false', ordering='weak')


def run_update_points_in_background(peer_url, collection_name):
    p = multiprocessing.Process(target=update_points_in_loop, args=(peer_url, collection_name))
    p.start()
    return p


def get_all_points(peer_url, collection_name):
    res = requests.post(
        f"{peer_url}/collections/{collection_name}/points/scroll",
        json={
            "limit": POINTS_COUNT,
            "with_vector": True,
            "with_payload": True,
        },
        timeout=10
    )
    assert_http_ok(res)
    return res.json()["result"]


def test_shard_transfer_consistency(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICAS)
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)

    # upload points to the leader
    upload_processes = [
        run_update_points_in_background(peer_api_uris[i], "test_collection")
        for i in range(len(peer_api_uris))
    ]

    print("Push points during 10 seconds")
    time.sleep(10)

    # Extract current collection cluster info
    collection_cluster_info = get_collection_cluster_info(peer_api_uris[0], "test_collection")
    shard_id = collection_cluster_info["local_shards"][0]["shard_id"]
    source_peer_id = collection_cluster_info["peer_id"]
    target_peer_id = collection_cluster_info["remote_shards"][0]["peer_id"]
    source_uri = peer_api_uris[0]

    r = requests.post(
        f"{source_uri}/collections/test_collection/cluster", json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": source_peer_id,
                "to_peer_id": target_peer_id,
                "method": "stream_records"
            }
        })

    assert_http_ok(r)

    # Validate that all peers have the same data
    results = []
    for url in peer_api_uris:
        res = get_all_points(url, COLLECTION_NAME)
        results.append(res)

    for scroll_result in results:
        prev = -1
        for row in scroll_result['points']:
            row_id = row['id']
            print(row_id)
            assert row_id == prev + 1
            prev = row_id

    # Kill all upload processes
    for p in upload_processes:
        p.kill()

    time.sleep(1)
