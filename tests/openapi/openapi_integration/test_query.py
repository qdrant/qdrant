import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = "test_query"


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    
    response = request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={
            "field_name": "price",
            "field_schema": "float"},
    )
    assert response.ok
    yield
    drop_collection(collection_name=collection_name)


def test_basic_search():
    response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [0.1, 0.2, 0.3, 0.4],
            "limit": 10,
        },
    )
    assert response.ok
    search_result = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": [0.1, 0.2, 0.3, 0.4],
        },
    )
    assert response.ok
    default_query_result = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {"nearest": [0.1, 0.2, 0.3, 0.4]},
        },
    )
    assert response.ok
    nearest_query_result = response.json()["result"]

    assert search_result == default_query_result
    assert search_result == nearest_query_result


def test_basic_scroll():
    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={},
    )
    assert response.ok
    scroll_result = response.json()["result"]["points"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "with_payload": True,
        },
    )
    assert response.ok
    query_result = response.json()["result"]

    for record, scored_point in zip(scroll_result, query_result):
        assert record.get("id") == scored_point.get("id")
        assert record.get("payload") == scored_point.get("payload")


def test_basic_recommend_avg():
    response = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [1, 2, 3, 4],  # ids
            "negative": [3],  # ids
            "limit": 10,
        },
    )
    assert response.ok
    recommend_result = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "recommend": {"positive": [1, 2, 3, 4], "negative": [3]},  # ids
            }
        },
    )
    assert response.ok
    query_result = response.json()["result"]

    assert recommend_result == query_result


def test_basic_recommend_best_score():
    response = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [1, 2, 3, 4],  # ids
            "negative": [3],  # ids
            "limit": 10,
            "strategy": "best_score",
        },
    )
    assert response.ok
    recommend_result = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "recommend": {
                    "positive": [1, 2, 3, 4],  # ids
                    "negative": [3],  # ids
                    "strategy": "best_score",
                },
            }
        },
    )
    assert response.ok
    query_result = response.json()["result"]

    assert recommend_result == query_result


def test_basic_discover():
    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "target": 2,  # ids
            "context": [{"positive": 3, "negative": 4}],  # ids
            "limit": 10,
        },
    )
    assert response.ok
    discover_result = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "discover": {
                    "target": 2,
                    "context": [{"positive": 3, "negative": 4}],
                },
            }
        },
    )
    assert response.ok
    query_result = response.json()["result"]

    assert discover_result == query_result


def test_basic_context():
    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "context": [{"positive": 2, "negative": 4}],
            "limit": 100,
        },
    )
    assert response.ok
    context_result = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "context": [{"positive": 2, "negative": 4}],
            },
            "limit": 100,
        },
    )
    assert response.ok
    query_result = response.json()["result"]
    
    assert set([p["id"] for p in context_result]) == set([p["id"] for p in query_result])


def test_basic_order_by():
    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "order_by": "price",
        },
    )
    assert response.ok
    scroll_result = response.json()["result"]["points"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {"order_by": "price"},
            "with_payload": True,
        },
    )
    assert response.ok
    query_result = response.json()["result"]

    for record, scored_point in zip(scroll_result, query_result):
        assert record.get("id") == scored_point.get("id")
        assert record.get("payload") == scored_point.get("payload")