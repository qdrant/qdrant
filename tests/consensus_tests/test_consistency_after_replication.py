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
LOOPS_OF_REPLICATION = 20
INCONSISTENCY_CHECK_ATTEMPTS = 10
HEADERS = {}


def create_index(peer_url, collection=COLLECTION_NAME,
                 field_name: str = 'keyword', field_schema: str = 'keyword',
                 wait='true', ordering='weak', headers={}):
    r_batch = requests.put(
        f"{peer_url}/collections/{collection}/index?wait={wait}&ordering={ordering}",
        json={
            "field_name": field_name,
            "field_schema": field_schema
        },
        headers=headers,
    )
    assert_http_ok(r_batch)


def upsert_random_points_with_payload(
    peer_url,
    num,
    collection_name=COLLECTION_NAME,
    fail_on_error=True,
    offset=0,
    batch_size=None,
    wait="true",
    ordering="weak",
    shard_key=None,
    headers=None
):
    if not headers:
        headers = HEADERS

    def get_vector():
        # Create points in first peer's collection
        vector = {
            "": random_dense_vector(),
        }

        return vector

    while num > 0:
        size = num if batch_size is None else min(num, batch_size)

        timestamp_str = datetime.now().isoformat()
        points = [
                    {
                        "id": i + offset,
                        "vector": get_vector(),
                        "payload": {
                            "a": f"keyword_{random.randint(1, 1000)}",
                            "timestamp": timestamp_str
                        },
                    }
                    for i in range(size)
                ]
        r_batch = requests.put(
            f"{peer_url}/collections/{collection_name}/points?wait={wait}&ordering={ordering}",
            json={
                "points": points,
                "shard_key": shard_key,
            },
            headers=headers,
        )
        if fail_on_error:
            assert_http_ok(r_batch)

        num -= size
        offset += size


def update_points_in_loop(peer_api_uris, collection_name):
    limit = 10
    while True:
        # offset = random.randint(0, INITIAL_POINTS_COUNT)
        # experiment 1 on Okt 4, found some inconsistent points but only when continue updates
        offset = 0
        peer_url = random.choice(peer_api_uris)
        upsert_random_points_with_payload(peer_url, num=limit, collection_name=collection_name, offset=offset, wait='true')


def run_update_points_in_background(peer_api_uris, collection_name):
    p = multiprocessing.Process(target=update_points_in_loop, args=(peer_api_uris, collection_name))
    p.start()
    return p


def run_update_points_in_background_bfb(grpc_uris, collection_name):
    os.environ['QDRANT_API_KEY'] = HEADERS.get('api-key', '')
    bfb_cmd_args = (f"--replication-factor {N_REPLICAS} --shards {N_SHARDS} --keywords 10 --timestamp-payload "
                    f"--dim 768 -n 1000000000 --batch-size 100 --threads 1 --parallel 1 --wait-on-upsert "
                    f"--create-if-missing --quantization scalar --timing-threshold 1 --on-disk-vectors true "
                    f"--max-id {INITIAL_POINTS_COUNT} --delay 1000 --timeout 30 --retry 4 --retry-interval 1 "
                    f"--uri {grpc_uris[0]} --collection-name {collection_name}")
    popen_args = ["./bfb/target/debug/bfb", *bfb_cmd_args.split(" ")]
    p = Popen(popen_args, start_new_session=True)
    return p


def get_all_points(peer_url, collection_name):
    res = requests.post(
        f"{peer_url}/collections/{collection_name}/points/scroll",
        json={
            "limit": INITIAL_POINTS_COUNT,
            "with_vector": True,
            "with_payload": True,
        },
        timeout=30,
        headers=HEADERS
    )
    assert_http_ok(res)
    return res.json()["result"]


def replicate_shard(peer_api_uris, collection_name):
    random_api_uri = random.choice(peer_api_uris)
    collection_cluster_info = get_collection_cluster_info(random_api_uri, collection_name, headers=HEADERS)
    cluster_info = get_cluster_info(random_api_uri, headers=HEADERS)
    # get ids of all the peers in the cluster
    peer_ids = []
    for peer in cluster_info["peers"]:
        peer_ids.append(int(peer))
    # print(f"peer_ids: {peer_ids}")

    while not collection_cluster_info["local_shards"]:
        print(f"     selected node {random_api_uri} doesn't have local shards, pick another")
        random_api_uri = random.choice(peer_api_uris)
        collection_cluster_info = get_collection_cluster_info(random_api_uri, collection_name, headers=HEADERS)
        time.sleep(1)
    print(f"     selected node {random_api_uri}")

    source_uri = random_api_uri
    shard_id = collection_cluster_info["local_shards"][0]["shard_id"]
    source_peer_id = collection_cluster_info["peer_id"]

    # target_peer_id = collection_cluster_info["remote_shards"][0]["peer_id"]
    # exclude source peer
    peer_ids.remove(source_peer_id)
    target_peer_id = random.choice(peer_ids)

    res = requests.post(
        f"{source_uri}/collections/{collection_name}/cluster",
        json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": source_peer_id,
                "to_peer_id": target_peer_id
            }
        },
        headers=HEADERS,
        timeout=10
    )
    print(f"    Replicate shard {shard_id} from {source_peer_id} to {target_peer_id}")
    assert_http_ok(res)

    wait_for_collection_shard_transfers_count(source_uri, collection_name, 0, headers=HEADERS)

    return res.json()["result"]


def check_consistency(peer_api_uris):
    print("8. Check consistency")
    attempts_counter = INCONSISTENCY_CHECK_ATTEMPTS
    results = []
    diffs = {}
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

        diffs[f'attempt_{attempts_counter}'] = {}
        is_diff = False
        for node_id, res in enumerate(results[1::]):
            diffs[f'attempt_{attempts_counter}'][f'node_{node_id + 1}'] = []
            for idx, row in enumerate(res['points']):
                expected_row = results[0]['points'][idx]
                if row != expected_row:
                    # diffs.append(f"inconsistency in point {row['id']}: {row['payload']} vs {expected_row['payload']}")
                    diffs[f'attempt_{attempts_counter}'][f'node_{node_id + 1}'].append(f"inconsistency in point {row['id']}: {row['payload']} vs {expected_row['payload']}")
                    is_diff = True
        if is_diff:
            print(f"    There are inconsistencies found, wait for 1 sec and try again")
            time.sleep(1)
            attempts_counter -= 1
        else:
            break

    # final check
    diffs['final'] = {}
    is_diff = False
    for node_id, res in enumerate(results[1::]):
        diffs['final'][f'node_{node_id + 1}'] = []
        for idx, row in enumerate(res['points']):
            expected_row = results[0]['points'][idx]
            if row != expected_row:
                # diffs.append(f"inconsistency in point {row['id']}: {row['payload']} vs {expected_row['payload']}")
                diffs['final'][f'node_{node_id + 1}'].append(
                    f"inconsistency in point {row['id']}: {row['payload']} vs {expected_row['payload']}")
                is_diff = True

    with open('diff.json', 'w') as diff_log:
        diff_log.write(json.dumps(diffs))

    if is_diff:
        print("Consistency check FAILED")
        raise AssertionError("There are inconsistencies found after 10 attempts")
    print("Consistency check OK")


def test_consistency(tmp_path: pathlib.Path):
    assert_project_root()

    print("\n1. Create a 3-nodes cluster")
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)
    grpc_uris = []
    for item in processes:
        grpc_uris.append(f"http://127.0.0.1:{item.grpc_port}")

    print(f"2. Create a collection with {N_SHARDS} shards")
    create_collection(peer_api_uris[0], collection=COLLECTION_NAME, shard_number=N_SHARDS, replication_factor=N_REPLICAS, headers=HEADERS)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris, headers=HEADERS)
    # create index just 'cause chaos testing collection has 'em
    create_index(peer_api_uris[0], collection=COLLECTION_NAME, field_name='a', field_schema='keyword', headers=HEADERS)
    create_index(peer_api_uris[0], collection=COLLECTION_NAME, field_name='timestamp', field_schema='datetime', headers=HEADERS)

    print("3. Insert starting points (10k) with payload (timestamp)")
    upsert_random_points_with_payload(peer_api_uris[0], INITIAL_POINTS_COUNT, COLLECTION_NAME, batch_size=100, wait='false')
    print("     Wait for optimizations to complete")
    for uri in peer_api_uris:
        wait_collection_green(uri, COLLECTION_NAME, headers=HEADERS)

    print("4. Start updating points in the background (random nodes)")
    # upsert_process = run_update_points_in_background(peer_api_uris, COLLECTION_NAME)
    upsert_process = run_update_points_in_background_bfb(grpc_uris, COLLECTION_NAME)
    upsert_process_2 = run_update_points_in_background_bfb(grpc_uris, COLLECTION_NAME)
    print("     Wait for some time")
    time.sleep(5)

    for i in range(LOOPS_OF_REPLICATION):
        print("5-6. Trigger one of the shards' replication. Wait for the replication to complete")
        replicate_shard(peer_api_uris, COLLECTION_NAME)
        print("     Wait for some time")
        time.sleep(random.randint(3, 15))

        # random_bool = random.choice([True, False])
        random_bool = True
        if random_bool:
            print("     Stop updating points")
            upsert_process.kill()
            upsert_process_2.kill()
            print("         Wait for some time")
            time.sleep(3)

            check_consistency(peer_api_uris)

            print("     Start updating points again")
            # upsert_process = run_update_points_in_background(peer_api_uris, COLLECTION_NAME)
            upsert_process = run_update_points_in_background_bfb(grpc_uris, COLLECTION_NAME)
            upsert_process_2 = run_update_points_in_background_bfb(grpc_uris, COLLECTION_NAME)

    print("7. Stop updating points")
    upsert_process.kill()
    upsert_process_2.kill()
    print("    Wait for some time")
    time.sleep(3)

    check_consistency(peer_api_uris)

    print("OK")
