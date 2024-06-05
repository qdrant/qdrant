import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = "test_query"


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
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
                "recommend": {"positives": [1, 2, 3, 4], "negatives": [3]},  # ids
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
                    "positives": [1, 2, 3, 4], # ids
                    "negatives": [3], # ids
                    "strategy": "best_score",
                },
            }
        },
    )
    assert response.ok
    query_result = response.json()["result"]

    assert recommend_result == query_result
