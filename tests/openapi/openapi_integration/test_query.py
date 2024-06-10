from math import isclose
import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import reciprocal_rank_fusion, request_with_validation

collection_name = "test_query"


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)

    response = request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={"field_name": "price", "field_schema": "float"},
    )
    assert response.ok
    yield
    drop_collection(collection_name=collection_name)


def root_and_rescored_query(query, limit=None, with_payload=None):
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": query,
            "limit": limit,
            "with_payload": with_payload,
        },
    )
    assert response.ok
    root_query_result = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": {
                "limit": 1000,
            },
            "query": query,
            "with_payload": with_payload,
        },
    )
    assert response.ok
    nested_query_result = response.json()["result"]

    assert root_query_result == nested_query_result
    return root_query_result


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

    default_query_result = root_and_rescored_query([0.1, 0.2, 0.3, 0.4])

    nearest_query_result = root_and_rescored_query({"nearest": [0.1, 0.2, 0.3, 0.4]})

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

    query_result = root_and_rescored_query(
        {
            "recommend": {"positive": [1, 2, 3, 4], "negative": [3]},  # ids
        }
    )

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

    query_result = root_and_rescored_query(
        {
            "discover": {
                "target": 2,
                "context": [{"positive": 3, "negative": 4}],
            }
        },
    )

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

    query_result = root_and_rescored_query({"order_by": "price"}, with_payload=True)

    for record, scored_point in zip(scroll_result, query_result):
        assert record.get("id") == scored_point.get("id")
        assert record.get("payload") == scored_point.get("payload")


def test_basic_rrf():
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
    search_result_1 = response.json()["result"]
    
    response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [0.5, 0.6, 0.7, 0.8],
            "limit": 10,
        },
    )
    assert response.ok
    search_result_2 = response.json()["result"]
    
    rrf_expected = reciprocal_rank_fusion([search_result_1, search_result_2], limit=10)
    
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": [
                { "query": [0.1, 0.2, 0.3, 0.4] },
                { "query": [0.5, 0.6, 0.7, 0.8] },
            ],
            "query": {"fusion": "rrf"},
        },
    )
    assert response.ok, response.json()
    rrf_result = response.json()["result"]
    
    def get_id(x):
        return x["id"]
    
    # rrf order is not deterministic with same scores, so we need to sort by id
    for expected, result in zip(sorted(rrf_expected, key=get_id), sorted(rrf_result, key=get_id)):
        assert expected["id"] == result["id"]
        assert expected["payload"] == result["payload"]
        assert isclose(expected["score"], result["score"], rel_tol=1e-5)
