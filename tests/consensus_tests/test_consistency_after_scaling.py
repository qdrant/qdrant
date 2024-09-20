"""
Test consistency during updates and shard transfer
1. Create a 3-nodes cluster
2. Create a collection with 3 shards, replication factor: 3
3. Insert starting points (10k) with payload (timestamp)
4. Start updating points in the background (random nodes)
5. Scale up
6. Replicate shard on a new node and wait for transfers
7. Wait for some time
8. Scale down
9. Wait for some time
10. Stop updating points
11. Check consistency
"""
import logging
import multiprocessing
import pathlib
import random
import time
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
LOOPS_OF_SCALING = 2
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
        print(f"Update in batches of size {size}")

        timestamp_str = datetime.now().isoformat()
        try:
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
    limit = 50
    while True:
        offset = random.randint(0, INITIAL_POINTS_COUNT)
        # print(f"peer_api_uris length: {len(peer_api_uris)}")
        peer_url = random.choice(peer_api_uris)
        # print(f"Upload points to {peer_url}")
        upsert_random_points_with_payload(
            peer_url,
            num=limit,
            collection_name=collection_name,
            fail_on_error=False, offset=offset, wait='false')


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


def wait_for_shard_transfers(peer_api_uris, collection_name):
    random_api_uri = peer_api_uris[0]
    collection_cluster_info = get_collection_cluster_info(random_api_uri, collection_name)
    source_uri = random_api_uri
    wait_for_collection_shard_transfers_count(source_uri, collection_name, 0)

    return True


def replicate_shard(target_peer_id, peer_api_uris, collection_name):
    random_api_uri = random.choice(peer_api_uris)
    collection_cluster_info = get_collection_cluster_info(random_api_uri, collection_name)
    while not collection_cluster_info["local_shards"]:
        print("selected node doesn't have local shards, pick another")
        random_api_uri = random.choice(peer_api_uris)
        collection_cluster_info = get_collection_cluster_info(random_api_uri, collection_name)

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


def cluster_scale_up(tmp_path, bootstrap_uri, port_seed, peer_number, peer_api_uris, peer_dirs, collection_name = "test_collection"):
    new_peer_dir = make_peer_folder(tmp_path, peer_number)
    peer_dirs.append(new_peer_dir)

    port = None
    # Start 1 peer
    if port_seed is not None:
        port = port_seed + 1 * 100
    new_peer_uri = start_peer(new_peer_dir, f"peer_0_{peer_number}.log", bootstrap_uri, port=port, extra_env=None)
    peer_api_uris.append(new_peer_uri)

    time.sleep(3)

    return new_peer_uri


def cluster_scale_down(peer_number, peer_api_uris, collection_name):
    def delete_peer_folder(base_path: Path) -> Path:
        peer_dir = base_path / f"peer{peer_number}"
        peer_dir.rmdir()
        return peer_dir

    peer_url = random.choice(peer_api_uris[1::])
    # peer_url = peer_api_uris[-1]
    print(f"    Remove peer {peer_url} from cluster")
    collection_cluster_info = get_collection_cluster_info(peer_url, collection_name)
    from_peer = collection_cluster_info['peer_id']
    to_peer = collection_cluster_info["remote_shards"][0]["peer_id"]

    if not collection_cluster_info["local_shards"]:
        print(f"    Initially no local shards found on peer, safe to remove")

    for local_shards in collection_cluster_info["local_shards"]:
        print(f"    Moving shards from peer")
        shard_id = local_shards["shard_id"]
        r = requests.post(f"{peer_url}/collections/{collection_name}/cluster?timeout=60",
                          json={
                              "move_shard": {
                                  "from_peer_id": from_peer,
                                  "shard_id": shard_id,
                                  "to_peer_id": to_peer
                              }
                          })
        assert_http_ok(r)

    max_wait = 90
    # Check that the shard is moved
    while max_wait > 0:
        r = requests.get(f"{peer_url}/collections/{collection_name}/cluster")
        assert_http_ok(r)
        collection_cluster_status = r.json()

        if len(collection_cluster_status["result"]["local_shards"]) == 0:
            print(f"    No more local shards on peer, safe to remove")
            break

        max_wait -= 1
        time.sleep(1)

    # Disconnect peer from the cluster
    r = requests.delete(f"{peer_url}/cluster/peer/{from_peer}?timeout=60")
    try:
        assert_http_ok(r)
    except Exception as ex:
        print(get_collection_cluster_info(peer_url, collection_name))
        raise ex

    peer_api_uris.remove(peer_url)

    return peer_api_uris


def test_consistency(tmp_path: pathlib.Path):
    assert_project_root()

    print("\n1. Create a 3-nodes cluster")
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    print("2. Create a collection")
    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICAS)
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)

    print("3. Insert starting points (10k) with payload (timestamp)")
    upsert_random_points_with_payload(peer_api_uris[0], INITIAL_POINTS_COUNT, "test_collection", batch_size=100, wait='false')
    for uri in peer_api_uris:
        wait_collection_green(uri, COLLECTION_NAME)

    print("4. Start updating points in the background (random nodes)")
    upsert_process = run_update_points_in_background(peer_api_uris, "test_collection")
    print(" Wait for some time")
    time.sleep(5)

    new_upsert_processes = []
    for i in range(4, 4 + LOOPS_OF_SCALING):
        print("5. Scale up")
        new_peer_uri = cluster_scale_up(tmp_path, bootstrap_uri, processes[-1].http_port, i, peer_api_uris, peer_dirs)
        print("6. Replicate shard on a new node and wait for transfers")
        wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)
        new_upsert_processes.append(run_update_points_in_background([new_peer_uri], "test_collection"))
        collection_cluster_info = get_collection_cluster_info(new_peer_uri, "test_collection")
        peer_id = collection_cluster_info['peer_id']
        replicate_shard(peer_id, peer_api_uris, "test_collection")
        wait_for_all_replicas_active(peer_api_uris[0], "test_collection")
        print("7. Wait for some time after scaling up")
        time.sleep(random.randint(3, 15))
        print("8-9. Scale down. Wait for some time")
        cluster_scale_down(i, peer_api_uris, "test_collection")
        wait_for_all_replicas_active(peer_api_uris[0], "test_collection")
        print("7. Wait for some time after scaling down")
        time.sleep(random.randint(3, 15))

    print("10. Wait for some time and stop updating points")
    time.sleep(5)
    upsert_process.kill()
    for item in new_upsert_processes:
        item.kill()

    print("11. Check consistency")
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
                    print(f"    There is one inconsistency found, wait for 1 sec and try again")
                    time.sleep(1)
                    # print(f"    Stop updating points")
                    # upsert_process.kill()
                    # for item in new_upsert_processes:
                    #     item.kill()
                    do_break = True
                    break
            if do_break:
                break
        attempts_counter -= 1

    for res in results:
        for idx, row in enumerate(res['points']):
            assert row == results[0]['points'][idx]

    print("OK")
