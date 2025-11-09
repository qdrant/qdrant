import concurrent.futures
import pathlib
import threading
import time

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

    # Wait all transfers complete
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    results3 = search_points(peer_api_uris[0])
    match, msg = match_results(results, results3)
    assert match, f"Results differ after tenant1 promotion complete: {msg}"
