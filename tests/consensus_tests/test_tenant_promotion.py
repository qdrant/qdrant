import multiprocessing
import pathlib

from .custom_sharding import create_collection_with_custom_sharding, create_shard
from .fixtures import *
from .utils import *

COLLECTION_NAME = "tenant_collection"

N_PEERS = 3
N_SHARDS = 1
N_REPLICAS = 1


def match_results(results1: List[dict], results2: List[dict]):
    # Compare ids and payloads of search results
    if len(results1) != len(results2):
        return False, f"Results length differ: {len(results1)} != {len(results2)}"

    for idx, (r1, r2) in enumerate(zip(results1, results2)):
        if r1['id'] != r2['id']:
            return False, f"Result IDs differ: {r1['id']} != {r2['id']} at index {idx}"
        if r1.get('payload') != r2.get('payload'):
            return False, f"Result payloads differ for ID {r1['id']}: {r1.get('payload')} != {r2.get('payload')}, at index {idx}"

    return True, ""


def search_points(peer_url):
    query_vector = [0.1, 0.2, 0.3, 0.4]
    resp = requests.post(
        f"{peer_url}/collections/{COLLECTION_NAME}/points/search",
        json={
            "vector": query_vector,
            "top": 5,
            "filter": {
                "must": [
                    {
                        "key": "tenant",
                        "match": {
                            "value": "tenant1"
                        }
                    }
                ]
            },
            "shard_key": {
                "fallback": "default",
                "target": "tenant1"
            }
        }
    )
    assert_http_ok(resp)

    results = resp.json()["result"]
    return results


def test_tenant_promotion_simple(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection_with_custom_sharding(peer_api_uris[0], collection=COLLECTION_NAME, shard_number=N_SHARDS,
                                           replication_factor=N_REPLICAS)

    create_shard(
        peer_url=peer_api_uris[0],
        collection=COLLECTION_NAME,
        shard_key="default",
        shard_number=N_SHARDS,
        replication_factor=N_REPLICAS,
        # initial_state="Partial",
    )

    n_points = 1_000
    upsert_random_points(
        peer_api_uris[0],
        n_points,
        collection_name=COLLECTION_NAME,
        shard_key={
            "fallback": "default",
            "target": "tenant1"
        },
        extra_payload={
            "tenant": "tenant1"
        }
    )

    n_points = 100
    upsert_random_points(
        peer_api_uris[0],
        n_points,
        collection_name=COLLECTION_NAME,
        shard_key={
            "fallback": "default",
            "target": "tenant2"
        },
        extra_payload={
            "tenant": "tenant2"
        }
    )

    # Search point with shard key and filter
    results = search_points(peer_api_uris[0])
    assert len(results) > 0, "No results found for tenant1"

    create_shard(
        peer_url=peer_api_uris[0],
        collection=COLLECTION_NAME,
        shard_key="tenant1",
        shard_number=N_SHARDS,
        replication_factor=N_REPLICAS,
        initial_state="Partial",
    )

    # With new shard we should get the same results
    result2 = search_points(peer_api_uris[0])
    match, msg = match_results(results, result2)
    assert match, f"Results differ after tenant1 shard creation: {msg}"

    n_points = 100
    upsert_random_points(
        peer_api_uris[0],
        n_points,
        collection_name=COLLECTION_NAME,
        shard_key={
            "fallback": "default",
            "target": "tenant1"
        },
        extra_payload={
            "tenant": "tenant1"
        }
    )

    results = search_points(peer_api_uris[0])

    # Promote to dedicated shard
    resp = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster",
        json={
            "replicate_points": {
                "filter": {
                    "must": {
                        "key": "tenant",
                        "match": {
                            "value": "tenant1"
                        }
                    }
                },
                "from_shard_key": "default",
                "to_shard_key": "tenant1"
            }
        }
    )
    assert_http_ok(resp)

    results2 = search_points(peer_api_uris[0])
    match, msg = match_results(results, results2)
    assert match, f"Results differ after tenant1 promotion start: {msg}"

    # Wait all transfers complete on all peers
    for peer_url in peer_api_uris:
        wait_for_collection_shard_transfers_count(peer_url, COLLECTION_NAME, 0)
        # Check that new shard have active state
        info = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
        for shard in info["local_shards"]:
            assert shard['state'] == 'Active'
        for shard in info["remote_shards"]:
            assert shard['state'] == 'Active'

    results3 = search_points(peer_api_uris[0])
    match, msg = match_results(results, results3)
    assert match, f"Results differ after tenant1 promotion complete: {msg}"

    # Delete records from the old shard
    resp = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/delete?wait=true",
        json={
            "filter": {
                "must": {
                    "key": "tenant",
                    "match": {
                        "value": "tenant1"
                    }
                }
            },
            "shard_key": "default"
        }
    )
    assert_http_ok(resp)

    results4 = search_points(peer_api_uris[0])
    match, msg = match_results(results, results4)
    assert match, f"Results differ after tenant1 deletion from old shard: {msg}"


def update_points_in_loop(peer_urls: List[str], collection_name, offset=0, duration=None):
    start = time.time()
    limit = 3

    while True:
        peer_url = random.choice(peer_urls)
        upsert_random_points(
            peer_url,
            limit,
            collection_name,
            offset=offset,
            shard_key={
                "fallback": "default",
                "target": "tenant1"
            },
            extra_payload={
                "tenant": "tenant1"
            }
        )
        offset += limit

        if duration is not None and (time.time() - start) > duration:
            break


def run_update_points_in_background(peer_urls, collection_name, init_offset=0, duration=None):
    p = multiprocessing.Process(target=update_points_in_loop, args=(peer_urls, collection_name, init_offset, duration))
    p.start()
    return p


def set_payload_in_loop(peer_urls: List[str], collection_name, from_id, to_id, duration=None):
    start = time.time()

    while True:
        peer_url = random.choice(peer_urls)
        point_id = random.randint(from_id, to_id - 1)

        update_points_payload(
            peer_url,
            points=[point_id],
            collection_name=collection_name,
            shard_key={
                "fallback": "default",
                "target": "tenant1"
            },
        )

        if duration is not None and (time.time() - start) > duration:
            break

def run_set_payload_in_background(peer_urls, collection_name, from_id, to_id, duration=None):
    p = multiprocessing.Process(target=set_payload_in_loop, args=(peer_urls, collection_name, from_id, to_id, duration))
    p.start()
    return p

def test_tenant_promotion_background_updates(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection_with_custom_sharding(peer_api_uris[0], collection=COLLECTION_NAME, shard_number=N_SHARDS,
                                           replication_factor=N_REPLICAS)

    create_shard(
        peer_url=peer_api_uris[0],
        collection=COLLECTION_NAME,
        shard_key="default",
        shard_number=N_SHARDS,
        replication_factor=N_REPLICAS,
    )

    n_points = 1_000
    upsert_random_points(
        peer_api_uris[0],
        n_points,
        collection_name=COLLECTION_NAME,
        shard_key={
            "fallback": "default",
            "target": "tenant1"
        },
        extra_payload={
            "tenant": "tenant1"
        }
    )

    # Create tenant1 shard in Partial state
    create_shard(
        peer_url=peer_api_uris[0],
        collection=COLLECTION_NAME,
        shard_key="tenant1",
        shard_number=N_SHARDS,
        replication_factor=N_REPLICAS,
        initial_state="Partial",
    )

    # Start background process with updates
    upload_process_1 = run_update_points_in_background(peer_api_uris, COLLECTION_NAME, init_offset=10000, duration=5)
    set_payload_process_1 = run_set_payload_in_background(peer_api_uris, COLLECTION_NAME, from_id=0, to_id=1000, duration=5)

    time.sleep(1)  # Let some updates happen

    # Promote to dedicated shard
    resp = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster",
        json={
            "replicate_points": {
                "filter": {
                    "must": {
                        "key": "tenant",
                        "match": {
                            "value": "tenant1"
                        }
                    }
                },
                "from_shard_key": "default",
                "to_shard_key": "tenant1"
            }
        }
    )
    assert_http_ok(resp)

    # Wait all transfers complete on all peers
    for peer_url in peer_api_uris:
        wait_for_collection_shard_transfers_count(peer_url, COLLECTION_NAME, 0)
        # Check that new shard have active state
        info = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
        for shard in info["local_shards"]:
            assert shard['state'] == 'Active'
        for shard in info["remote_shards"]:
            assert shard['state'] == 'Active'

    # Stop background updates and check that `tenant1` contains all points (full sequence)

    upload_process_1.kill()
    set_payload_process_1.kill()

    # Read all points from offset 10000 and check that there is no gaps
    # Use scroll API to get all points

    response = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/scroll",
        json={
            "limit": 1000000,
            "with_payload": False,
            "with_vector": False,
            "shard_key": "tenant1",
            "offset": 10000,
            "filter": {
                "must": {
                    "key": "tenant",
                    "match": {
                        "value": "tenant1"
                    }
                }
            }
        }
    )

    assert_http_ok(response)
    points = response.json()["result"]["points"]

    received_ids = set()
    for point in points:
        received_ids.add(point['id'])

    missing_points = []
    # Check that there is no gaps in IDs from 10000 to max received ID
    if points:
        max_id = max(received_ids)
        print("Max ID in tenant1 shard:", max_id)
        for expected_id in range(10000, max_id + 1):
            if expected_id not in received_ids:
                missing_points.append(expected_id)

    assert len(missing_points) == 0, f"Missing point ID {missing_points} in tenant1 shard"
