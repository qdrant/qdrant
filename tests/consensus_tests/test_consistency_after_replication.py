"""
Test consistency during updates and shard transfer
1. Create a 3-nodes cluster
2. Create a collection with 2 shards
3. Insert starting points (10k) with payload (timestamp)
4. Start updating points in the background (random nodes)
5. Trigger one of the shards'  replication
6. Wait for the replication to complete
7. Stop updating points
8. Check consistency
"""
import logging
import multiprocessing
import pathlib
import random
from datetime import datetime

from .fixtures import create_collection, random_dense_vector
from .utils import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

INITIAL_POINTS_COUNT = 10000
N_PEERS = 3
N_SHARDS = 2
N_REPLICAS = 1
COLLECTION_NAME = "test_collection"
LOOPS_OF_REPLICATION = 15
INCONSISTENCY_CHECK_ATTEMPTS = 3


def upsert_random_points_with_payload(
    peer_url,
    num,
    collection_name="test_collection",
    fail_on_error=True,
    offset=0,
    batch_size=None,
    wait="true",
    ordering="weak",
    shard_key=None,
    headers=None
):
    if not headers:
        headers = {}

    def get_vector():
        # Create points in first peer's collection
        vector = {
            "": random_dense_vector(),
        }

        return vector

    while num > 0:
        size = num if batch_size is None else min(num, batch_size)

        timestamp_str = datetime.now().isoformat()
        r_batch = requests.put(
            f"{peer_url}/collections/{collection_name}/points?wait={wait}&ordering={ordering}",
            json={
                "points": [
                    {
                        "id": i + offset,
                        "vector": get_vector(),
                        "payload": {"timestamp": timestamp_str},
                    }
                    for i in range(size)
                ],
                "shard_key": shard_key,
            },
            headers=headers,
        )
        if fail_on_error:
            assert_http_ok(r_batch)

        num -= size
        offset += size


def update_points_in_loop(peer_api_uris, collection_name):
    limit = 100
    while True:
        offset = random.randint(0, INITIAL_POINTS_COUNT)
        peer_url = random.choice(peer_api_uris)
        upsert_random_points_with_payload(peer_url, num=limit, collection_name=collection_name, offset=offset, wait='false')


def run_update_points_in_background(peer_api_uris, collection_name):
    p = multiprocessing.Process(target=update_points_in_loop, args=(peer_api_uris, collection_name))
    p.start()
    return p


def get_all_points(peer_url, collection_name):
    res = requests.post(
        f"{peer_url}/collections/{collection_name}/points/scroll?consistency=majority",
        json={
            "limit": INITIAL_POINTS_COUNT,
            "with_vector": True,
            "with_payload": True,
        },
        timeout=10
    )
    assert_http_ok(res)
    return res.json()["result"]


def replicate_shard(peer_api_uris, collection_name):
    random_api_uri = random.choice(peer_api_uris)
    collection_cluster_info = get_collection_cluster_info(random_api_uri, collection_name)
    while not collection_cluster_info["local_shards"]:
        print("     selected node doesn't have local shards, pick another")
        random_api_uri = random.choice(peer_api_uris)
        collection_cluster_info = get_collection_cluster_info(random_api_uri, collection_name)

    target_peer_id = collection_cluster_info["remote_shards"][0]["peer_id"]
    source_uri = random_api_uri

    shard_id = collection_cluster_info["local_shards"][0]["shard_id"]
    source_peer_id = collection_cluster_info["peer_id"]

    res = requests.post(
        f"{source_uri}/collections/{collection_name}/cluster",
        json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": source_peer_id,
                "to_peer_id": target_peer_id
            }
        },
        timeout=10
    )
    assert_http_ok(res)

    wait_for_collection_shard_transfers_count(source_uri, collection_name, 0)

    return res.json()["result"]


def check_consistency(peer_api_uris):
    print("8. Check consistency")
    attempts_counter = INCONSISTENCY_CHECK_ATTEMPTS
    results = []
    while attempts_counter > 0:
        print(f"attempts_counter {attempts_counter}")
        results = []
        for url in peer_api_uris:
            try:
                res = get_all_points(url, COLLECTION_NAME)
                results.append(res)
            except Exception as ex:
                print(f"failed with {ex}")
                raise ex

        do_break = False
        for res in results:
            for idx, row in enumerate(res['points']):
                try:
                    assert row == results[0]['points'][idx]
                except AssertionError as er:
                    print(er)
                    print(f"    There is inconsistency found, wait for 1 sec and try again")
                    time.sleep(1)
                    do_break = True
                    break
            if do_break:
                break
        attempts_counter -= 1

    for res in results:
        for idx, row in enumerate(res['points']):
            assert row == results[0]['points'][idx]


def test_consistency(tmp_path: pathlib.Path):
    assert_project_root()

    print("\n1. Create a 3-nodes cluster")
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    print("2. Create a collection with 2 shards")
    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICAS)
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)

    print("3. Insert starting points (10k) with payload (timestamp)")
    upsert_random_points_with_payload(peer_api_uris[0], INITIAL_POINTS_COUNT, "test_collection", batch_size=100, wait='false')
    # Wait for optimizations to complete
    for uri in peer_api_uris:
        wait_collection_green(uri, COLLECTION_NAME)

    print("4. Start updating points in the background (random nodes)")
    upsert_process = run_update_points_in_background(peer_api_uris, "test_collection")

    print("Wait for some time")
    time.sleep(5)

    for i in range(LOOPS_OF_REPLICATION):
        print("5-6. Trigger one of the shards' replication. Wait for the replication to complete")
        replicate_shard(peer_api_uris, "test_collection")
        print("Wait for some time")
        time.sleep(random.randint(3, 15))

        random_bool = random.choice([True, False])
        if random_bool:
            print("7. Stop updating points")
            upsert_process.kill()
            print("Wait for some time")
            time.sleep(3)

            check_consistency(peer_api_uris)

            upsert_process = run_update_points_in_background(peer_api_uris, "test_collection")

    print("7. Stop updating points")
    upsert_process.kill()

    check_consistency(peer_api_uris)

    print("OK")
