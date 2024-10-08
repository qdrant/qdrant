"""
Test consistency during updates and shard transfer
1. Create a 3-nodes cluster
2. Create a collection with 3 shards, replication factor: 3
3. Insert starting points (10k) with payload (timestamp)
4. Start updating points in the background (random nodes)
5. Kill 1 node
6. Wait for some time and start the node back again
7. Stop updating points
8. Check consistency
"""
import logging
import multiprocessing
import pathlib
import random
from datetime import datetime
from http.client import RemoteDisconnected

from urllib3.exceptions import NewConnectionError, MaxRetryError, ProtocolError

from .fixtures import create_collection, random_dense_vector
from .utils import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

INITIAL_POINTS_COUNT = 10000
N_PEERS = 3
N_SHARDS = 3
N_REPLICAS = 3
COLLECTION_NAME = "test_collection"
INCONSISTENCY_CHECK_ATTEMPTS = 10
HEADERS = {}


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
        try:
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
        except requests.exceptions.ConnectionError:
            # print(f"Got connection error from {peer_url}. Skip")
            return
        except ConnectionRefusedError:
            # print(f"Got connection error from {peer_url}. Skip")
            return
        except NewConnectionError:
            # print(f"Got connection error from {peer_url}. Skip")
            return
        except MaxRetryError:
            # print(f"Got connection error from {peer_url}. Skip")
            return
        except RemoteDisconnected:
            # print(f"Got connection error from {peer_url}. Skip")
            return
        except ProtocolError:
            # print(f"Got connection error from {peer_url}. Skip")
            return

        num -= size
        offset += size


def update_points_in_loop(peer_api_uris, collection_name):
    limit = 10
    while True:
        offset = random.randint(0, INITIAL_POINTS_COUNT)
        peer_url = random.choice(peer_api_uris)
        upsert_random_points_with_payload(
            peer_url,
            num=limit,
            collection_name=collection_name,
            fail_on_error=False, offset=offset, wait='true')


def run_update_points_in_background(peer_api_uris, collection_name):
    p = multiprocessing.Process(target=update_points_in_loop, args=(peer_api_uris, collection_name))
    p.start()
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


def wait_for_shard_transfers(peer_api_uris, collection_name):
    random_api_uri = peer_api_uris[0]
    source_uri = random_api_uri
    wait_for_collection_shard_transfers_count(source_uri, collection_name, 0, headers=HEADERS)

    return True


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

    print("2. Create a collection")
    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICAS, headers=HEADERS)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris, headers=HEADERS)

    print("3. Insert starting points (10k) with payload (timestamp)")
    upsert_random_points_with_payload(peer_api_uris[0], INITIAL_POINTS_COUNT, COLLECTION_NAME, batch_size=100, wait='false')
    print("     Wait for optimizations to complete")
    for uri in peer_api_uris:
        wait_collection_green(uri, COLLECTION_NAME)

    print("4. Start updating points in the background (random nodes)")
    upsert_process = run_update_points_in_background(peer_api_uris, COLLECTION_NAME)
    print("     Wait for some time")
    time.sleep(5)

    print("5-6. Kill 1 node. Wait for some time and start the node back again")
    p = processes.pop()
    p_http_port = str(p.http_port)
    p_p2p_port = str(p.p2p_port)
    print(f"    Kill node http://localhost:{p_http_port}, p2p port: {p_p2p_port}")
    p.kill()
    wait_for_some_replicas_not_active(peer_api_uris[0], COLLECTION_NAME)
    time.sleep(3)
    new_url = start_peer(peer_dirs[-2], "peer_0_restarted.log", bootstrap_uri)
    print(f"    Restarted a node: {new_url}")
    upsert_process_2 = run_update_points_in_background([new_url], COLLECTION_NAME)
    wait_for_all_replicas_active(peer_api_uris[0], COLLECTION_NAME)

    print("7. Wait for some time and stop updating points")
    time.sleep(3)
    upsert_process.kill()
    upsert_process_2.kill()

    check_consistency(peer_api_uris)

    print("OK")
