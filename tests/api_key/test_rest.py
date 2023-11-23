import requests
import pytest
import os


def test_delete_collection():
    assert_read_write("DELETE", "/collections/test_collection")


def test_create_collection():
    assert_read_write(
        "PUT",
        "/collections/test_collection",
        {
            "vectors": {"size": 4, "distance": "Dot"},
            "optimizers_config": {"default_segment_number": 2},
            "replication_factor": 2,
        },
    )


def test_create_index():
    assert_read_write(
        "PUT",
        "/collections/test_collection/index",
        {"field_name": "city", "field_schema": "keyword"},
    )


def test_read_collection_info():
    assert_read_only(
        "GET",
        "/collections/test_collection",
    )


def test_insert_points():
    assert_read_write(
        "PUT",
        "/collections/test_collection/points?wait=true",
        {
            "points": [
                {
                    "id": 1,
                    "vector": [0.05, 0.61, 0.76, 0.74],
                    "payload": {
                        "city": "Berlin",
                        "country": "Germany",
                        "count": 1000000,
                        "square": 12.5,
                        "coords": {"lat": 1.0, "lon": 2.0},
                    },
                },
                {
                    "id": 2,
                    "vector": [0.19, 0.81, 0.75, 0.11],
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 3,
                    "vector": [0.36, 0.55, 0.47, 0.94],
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 4,
                    "vector": [0.18, 0.01, 0.85, 0.80],
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": "98a9a4b1-4ef2-46fb-8315-a97d874fe1d7",
                    "vector": [0.24, 0.18, 0.22, 0.44],
                    "payload": {"count": [0]},
                },
                {
                    "id": "f0e09527-b096-42a8-94e9-ea94d342b925",
                    "vector": [0.35, 0.08, 0.11, 0.44],
                },
            ]
        },
    )


def test_retrieve_points():
    assert_read_only(
        "GET",
        "/collections/test_collection/points/2",
    )
    assert_read_only("POST", "/collections/test_collection/points", {"ids": [1, 2]})


def test_search_points():
    assert_read_only(
        "POST",
        "/collections/test_collection/points/search",
        {"vector": [0.2, 0.1, 0.9, 0.7], "top": 3},
    )
    assert_read_only(
        "POST",
        "/collections/test_collection/points/search/batch",
        {
            "searches": [
                {"vector": [0.2, 0.1, 0.9, 0.7], "top": 3},
                {"vector": [0.2, 0.1, 0.9, 0.7], "top": 3},
            ]
        },
    )


QDRANT_HOST = os.environ.get("QDRANT_HOST", "localhost")
QDRANT_PORT = 6333


def assert_read_only(method, path, data=None):
    assert_rw_token_success(method, f"http://{QDRANT_HOST}:{QDRANT_PORT}{path}", data)
    assert_ro_token_success(method, f"http://{QDRANT_HOST}:{QDRANT_PORT}{path}", data)


def assert_read_write(method, path, data=None):
    assert_rw_token_success(method, f"http://{QDRANT_HOST}:{QDRANT_PORT}{path}", data)
    assert_ro_token_failure(method, f"http://{QDRANT_HOST}:{QDRANT_PORT}{path}", data)


def assert_rw_token_success(method, url, data=None):
    """Perform the request with a read-write token and assert success"""
    requests.request(
        method=method, url=url, headers={"api-key": "my-secret"}, json=data
    ).raise_for_status()


def assert_ro_token_success(method, url, data=None):
    """Perform the request with a read-only token and assert success"""
    requests.request(
        method=method, url=url, headers={"api-key": "my-ro-secret"}, json=data
    ).raise_for_status()


def assert_ro_token_failure(method, url, data=None):
    """Perform the request with a read-only token and assert failure"""
    with pytest.raises(requests.HTTPError):
        requests.request(
            method=method, url=url, headers={"api-key": "my-ro-secret"}, json=data
        ).raise_for_status()
