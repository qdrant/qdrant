import logging
import pathlib
import time

from .fixtures import create_collection, upsert_random_points, upsert_points, random_dense_vector, drop_collection
from .utils import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

N_PEERS = 2
N_SHARDS = 1
N_REPLICAS = 1
COLLECTION_NAME = "test_collection_strict_mode"


def test_strict_mode_upsert_no_local_shard(tmp_path: pathlib.Path):
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_urls[0], collection=COLLECTION_NAME, shard_number=N_SHARDS, replication_factor=N_REPLICAS, sharding_method="custom")

    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)

    collection_info = get_cluster_info(peer_urls[0])
    non_leader = 0
    for peer_id, peer_info in collection_info['peers'].items():
        peer_id = int(peer_id)
        if peer_id != int(collection_info['peer_id']):
            non_leader = peer_id
            break

    create_shard_key("non_leader", peer_urls[0], collection=COLLECTION_NAME, placement=[non_leader])

    for _ in range(32):
        point = {"id": 1, "payload": {}, "vector": random_dense_vector()}
        upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME, shard_key="non_leader").raise_for_status()

    set_strict_mode(peer_urls[0], COLLECTION_NAME, {
        "enabled": True,
        "max_collection_vector_size_bytes": 30,
    })

    for _ in range(32):
        point = {"id": 1, "payload": {}, "vector": random_dense_vector()}
        upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME, shard_key="non_leader").raise_for_status()

    for _ in range(32):
        point = {"id": 2, "payload": {}, "vector": random_dense_vector()}
        res = upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME, shard_key="non_leader")
        if not res.ok:
            assert "Max vector storage size" in res.json()['status']['error']
            assert not res.ok
            return

    assert False, "Should have blocked upsert but didn't"


def test_strict_mode_upsert_local_shard(tmp_path: pathlib.Path):
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_urls[0], collection=COLLECTION_NAME, shard_number=N_SHARDS, replication_factor=N_REPLICAS)

    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)

    for _ in range(32):
        point = {"id": 1, "payload": {}, "vector": random_dense_vector()}
        upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME).raise_for_status()

    set_strict_mode(peer_urls[0], COLLECTION_NAME, {
        "enabled": True,
        "max_collection_vector_size_bytes": 30,
    })

    for _ in range(32):
        point = {"id": 1, "payload": {}, "vector": random_dense_vector()}
        upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME).raise_for_status()

    for _ in range(32):
        point = {"id": 2, "payload": {}, "vector": random_dense_vector()}
        res = upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME)
        if not res.ok:
            assert "Max vector storage size" in res.json()['status']['error']
            assert not res.ok
            return

    assert False, "Should have blocked upsert but didn't"


def set_strict_mode(peer_id, collection_name, strict_mode_config):
    requests.patch(
        f"{peer_id}/collections/{collection_name}",
        json={
            "strict_mode_config": strict_mode_config,
        },
    ).raise_for_status()
