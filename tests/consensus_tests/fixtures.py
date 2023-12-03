import random
from typing import List

import requests

from consensus_tests.assertions import assert_http_ok

CITIES = ["London", "New York", "Paris", "Tokyo", "Berlin", "Rome", "Madrid", "Moscow"]

# dense vector sizing
DENSE_VECTOR_SIZE = 4

# sparse vector sizing
SPARSE_VECTOR_SIZE = 1000
SPARSE_VECTOR_DENSITY = 0.1


# Generate a random dense vector
def random_dense_vector():
    return [random.random() for _ in range(DENSE_VECTOR_SIZE)]


# Generate a random sparse vector
def random_sparse_vector():
    num_non_zero = int(SPARSE_VECTOR_SIZE * SPARSE_VECTOR_DENSITY)
    indices: List[int] = random.sample(range(SPARSE_VECTOR_SIZE), num_non_zero)
    values: List[float] = [round(random.random(), 6) for _ in range(num_non_zero)]
    return {"indices": indices, "values": values}


def upsert_random_points(peer_url, num, collection_name="test_collection", fail_on_error=True, offset=0, wait='true', ordering ='weak'):
    # Create points in first peer's collection
    r_batch = requests.put(
        f"{peer_url}/collections/{collection_name}/points?wait={wait}&ordering={ordering}", json={
            "points": [
                {
                    "id": i + offset,
                    "vector": {
                        "": random_dense_vector(),
                        "sparse-text": random_sparse_vector(),
                    },
                    "payload": {"city": random.choice(CITIES)}
                } for i in range(num)
            ]
        })
    if fail_on_error:
        assert_http_ok(r_batch)


def create_collection(peer_url, collection="test_collection", shard_number=1, replication_factor=1, write_consistency_factor=1, timeout=10):
    # Create collection in peer_url
    r_batch = requests.put(
        f"{peer_url}/collections/{collection}?timeout={timeout}", json={
            "vectors": {
                "size": DENSE_VECTOR_SIZE,
                "distance": "Dot"
            },
            "sparse_vectors": {
                "sparse-text": {}
            },
            "shard_number": shard_number,
            "replication_factor": replication_factor,
            "write_consistency_factor": write_consistency_factor,
        })
    assert_http_ok(r_batch)


def drop_collection(peer_url, collection="test_collection", timeout=10):
    # Delete collection in peer_url
    r_delete = requests.delete(
        f"{peer_url}/collections/{collection}?timeout={timeout}")
    assert_http_ok(r_delete)


def search(peer_url, vector, city, collection="test_collection"):
    q = {
        "vector": vector,
        "top": 10,
        "with_vector": False,
        "with_payload": True,
        "filter": {
            "must": [
                {
                    "key": "city",
                    "match": {"value": city}
                }
            ]
        }
    }
    r_search = requests.post(f"{peer_url}/collections/{collection}/points/search", json=q)
    assert_http_ok(r_search)
    return r_search.json()["result"]


def count_counts(peer_url, collection="test_collection"):
    r_search = requests.post(
        f"{peer_url}/collections/{collection}/points/count",
        json={
            "exact": True,
        }
    )
    assert_http_ok(r_search)
    return r_search.json()["result"]["count"]
