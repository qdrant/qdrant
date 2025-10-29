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


def upsert_points(
        peer_url,
        points,
        collection_name="test_collection",
        wait="true",
        ordering="weak",
        shard_key=None,
        headers={},
) -> requests.Response:
    return requests.put(
        f"{peer_url}/collections/{collection_name}/points?wait={wait}&ordering={ordering}",
        json={
            "points": points,
            "shard_key": shard_key,
        },
        headers=headers,
    )


def update_points_vector(
        peer_url,
        points,
        collection_name="test_collection",
        wait="true",
):
    r_batch = requests.put(
        f"{peer_url}/collections/{collection_name}/points/vectors?wait={wait}",
        json={
            "points": [{"id": x, "vector": {"": random_dense_vector()}} for x in points],
        },
    )
    assert_http_ok(r_batch)
    return r_batch.json()


def update_points_payload(
        peer_url,
        points,
        collection_name="test_collection",
        wait="true",
):
    r_batch = requests.post(
        f"{peer_url}/collections/{collection_name}/points/payload?wait={wait}",
        json={
            "points": points,
            "payload": {"city": random.choice(CITIES)},
        },
    )
    assert_http_ok(r_batch)
    return r_batch.json()


def upsert_random_points(
    peer_url,
    num,
    collection_name="test_collection",
    fail_on_error=True,
    offset=0,
    batch_size=None,
    wait="true",
    ordering="weak",
    with_sparse_vector=True,
    shard_key=None,
    num_cities=None,
    headers={},
):

    def get_vector():
        # Create points in first peer's collection
        vector = {
            "": random_dense_vector(),
        }
        if with_sparse_vector:
            vector["sparse-text"] = random_sparse_vector()

        return vector

    while num > 0:
        size = num if batch_size is None else min(num, batch_size)

        r_batch = requests.put(
            f"{peer_url}/collections/{collection_name}/points?wait={wait}&ordering={ordering}",
            json={
                "points": [
                    {
                        "id": i + offset,
                        "vector": get_vector(),
                        "payload": {"city": random.choice(CITIES[:num_cities]) if num_cities is not None else random.choice(CITIES)},
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


def create_collection(
    peer_url,
    collection="test_collection",
    shard_number=1,
    replication_factor=1,
    write_consistency_factor=1,
    timeout=10,
    sharding_method=None,
    indexing_threshold=20000,
    headers={},
    strict_mode=None,
    sparse_vectors=True,
    default_segment_number=None,
    on_disk_payload=None,
):
    payload = {
        "vectors": {"size": DENSE_VECTOR_SIZE, "distance": "Dot"},
        "shard_number": shard_number,
        "replication_factor": replication_factor,
        "write_consistency_factor": write_consistency_factor,
        "sharding_method": sharding_method,
        "optimizers_config": {
            "indexing_threshold": indexing_threshold,
            "default_segment_number": default_segment_number,
        },
        "strict_mode_config": strict_mode,
        "on_disk_payload": on_disk_payload,
    }

    if sparse_vectors:
       payload["sparse_vectors"] = {"sparse-text": {}}

    # Create collection in peer_url
    r_batch = requests.put(
        f"{peer_url}/collections/{collection}?timeout={timeout}",
        json=payload,
        headers=headers,
    )
    assert_http_ok(r_batch)


def drop_collection(peer_url, collection="test_collection", timeout=10, headers={}):
    # Delete collection in peer_url
    r_delete = requests.delete(
        f"{peer_url}/collections/{collection}?timeout={timeout}", headers=headers
    )
    assert_http_ok(r_delete)


def create_field_index(
    peer_url,
    collection="test_collection",
    field_name="city",
    field_schema="keyword",
    headers={},
):
    # Create field index in peer_url
    r_batch = requests.put(
        f"{peer_url}/collections/{collection}/index",
        json={
            "field_name": field_name,
            "field_schema": field_schema,
        },
        headers=headers,
        params={"wait": "true"}
    )
    assert_http_ok(r_batch)


def search(peer_url, vector, city, collection="test_collection"):
    q = {
        "vector": vector,
        "top": 10,
        "with_vector": False,
        "with_payload": True,
        "filter": {"must": [{"key": "city", "match": {"value": city}}]},
    }
    r_search = requests.post(f"{peer_url}/collections/{collection}/points/search", json=q)
    assert_http_ok(r_search)
    return r_search.json()["result"]

def scroll(peer_url, city, collection="test_collection"):
    q = {
        "with_vector": False,
        "with_payload": True,
        "filter": {"must": [{"key": "city", "match": {"value": city}}]},
    }
    r_search = requests.post(f"{peer_url}/collections/{collection}/points/scroll", json=q)
    assert_http_ok(r_search)
    return r_search.json()["result"]["points"]

def count_counts(peer_url, collection="test_collection"):
    r_search = requests.post(
        f"{peer_url}/collections/{collection}/points/count",
        json={
            "exact": True,
        },
    )
    assert_http_ok(r_search)
    return r_search.json()["result"]["count"]


def set_strict_mode(peer_id, collection_name, strict_mode_config):
    requests.patch(
        f"{peer_id}/collections/{collection_name}",
        json={
            "strict_mode_config": strict_mode_config,
        },
    ).raise_for_status()


def get_telemetry_hw_info(peer_url, collection):
    r_search = requests.get(
        f"{peer_url}/telemetry", params="details_level=3"
    )
    assert_http_ok(r_search)
    hw = r_search.json()["result"]["hardware"]["collection_data"]
    if collection in hw:
        return hw[collection]
    else:
        return None

def get_telemetry_collections(peer_url):
    r_search = requests.get(
        f"{peer_url}/telemetry", params="details_level=3"
    )
    assert_http_ok(r_search)
    return r_search.json()["result"]['collections']['collections']
