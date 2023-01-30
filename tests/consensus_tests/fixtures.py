import random

import requests

from consensus_tests.assertions import assert_http_ok

CITIES = ["London", "New York", "Paris", "Tokyo", "Berlin", "Rome", "Madrid", "Moscow"]


def random_vector():
    return [random.random() for _ in range(4)]


def upsert_random_points(peer_url, num, collection_name="test_collection", fail_on_error=True, offset=0, wait='true', ordering ='weak'):
    # Create points in first peer's collection
    r_batch = requests.put(
        f"{peer_url}/collections/{collection_name}/points?wait={wait}&ordering={ordering}", json={
            "points": [
                {
                    "id": i + offset,
                    "vector": random_vector(),
                    "payload": {"city": random.choice(CITIES)}
                } for i in range(num)
            ]
        })
    if fail_on_error:
        assert_http_ok(r_batch)


def create_collection(peer_url, collection="test_collection", shard_number=1, replication_factor=1, timeout=10):
    # Create collection in first peer
    r_batch = requests.put(
        f"{peer_url}/collections/{collection}?timeout={timeout}", json={
            "vectors": {
                "size": 4,
                "distance": "Dot"
            },
            "shard_number": shard_number,
            "replication_factor": replication_factor,
        })
    assert_http_ok(r_batch)


def drop_collection(peer_url, collection="test_collection", timeout=10):
    # Create collection in first peer
    r_batch = requests.delete(
        f"{peer_url}/collections/{collection}?timeout={timeout}")
    assert_http_ok(r_batch)


def search(peer_url, vector, city):
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
    r_search = requests.post(f"{peer_url}/collections/test_collection/points/search", json=q)
    assert_http_ok(r_search)
    return r_search.json()["result"]
