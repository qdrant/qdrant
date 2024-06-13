from math import isclose

import pytest

from .helpers.collection_setup import drop_collection, full_collection_setup
from .helpers.helpers import reciprocal_rank_fusion, request_with_validation

collection_name = "test_query"


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    full_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)

    # keyword index on `city`
    response = request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        query_params={'wait': 'true'},
        path_params={"collection_name": collection_name},
        body={"field_name": "city", "field_schema": "keyword"},
    )
    assert response.ok

    # integer index on count
    response = request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        query_params={'wait': 'true'},
        path_params={"collection_name": collection_name},
        body={"field_name": "count", "field_schema": "integer"},
    )
    assert response.ok

    yield
    drop_collection(collection_name=collection_name)


def root_and_rescored_query(query, using, filter=None, limit=None, with_payload=None):
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": query,
            "limit": limit,
            "filter": filter,
            "with_payload": with_payload,
            "using": using,
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
            "filter": filter,
            "with_payload": with_payload,
            "using": using,
        },
    )
    assert response.ok
    nested_query_result = response.json()["result"]

    assert root_query_result == nested_query_result
    return root_query_result


def test_search():
    response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": {
                "name": "dense-image",
                "vector": [0.1, 0.2, 0.3, 0.4],
            },
            "limit": 10,
        },
    )
    assert response.ok
    search_result = response.json()["result"]

    default_query_result = root_and_rescored_query([0.1, 0.2, 0.3, 0.4], "dense-image")

    nearest_query_result = root_and_rescored_query({"nearest": [0.1, 0.2, 0.3, 0.4]}, "dense-image")

    assert search_result == default_query_result
    assert search_result == nearest_query_result

def test_filtered_search():
    filter = {
        "must": [
            {
                "key": "city",
                "match": {
                    "value": "Berlin"
                }
            }
        ]
    }
    response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": {
                "name": "dense-image",
                "vector": [0.1, 0.2, 0.3, 0.4],
            },
            "filter": filter,
            "limit": 10,
        },
    )
    assert response.ok
    search_result = response.json()["result"]

    default_query_result = root_and_rescored_query([0.1, 0.2, 0.3, 0.4], "dense-image", filter)

    nearest_query_result = root_and_rescored_query({"nearest": [0.1, 0.2, 0.3, 0.4]}, "dense-image", filter)

    assert search_result == default_query_result
    assert search_result == nearest_query_result


def test_scroll():
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

def test_filtered_scroll():
    filter = {
        "must": [
            {
                "key": "city",
                "match": {
                    "value": "Berlin"
                }
            }
        ]
    }
    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "filter": filter
        },
    )
    assert response.ok
    scroll_result = response.json()["result"]["points"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "with_payload": True,
            "filter": filter
        },
    )
    assert response.ok
    query_result = response.json()["result"]

    for record, scored_point in zip(scroll_result, query_result):
        assert record.get("id") == scored_point.get("id")
        assert record.get("payload") == scored_point.get("payload")


def test_recommend_avg():
    response = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [1, 2, 3, 4],  # ids
            "negative": [3],  # ids
            "limit": 10,
            "using": "dense-image",
        },
    )
    assert response.ok
    recommend_result = response.json()["result"]

    query_result = root_and_rescored_query(
        {
            "recommend": {"positive": [1, 2, 3, 4], "negative": [3]},  # ids
        },
        "dense-image",
    )

    assert recommend_result == query_result


def test_recommend_best_score():
    response = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [1, 2, 3, 4],  # ids
            "negative": [3],  # ids
            "limit": 10,
            "strategy": "best_score",
            "using": "dense-image",
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
            },
            "using": "dense-image",
        },
    )
    assert response.ok
    query_result = response.json()["result"]

    assert recommend_result == query_result


def test_discover():
    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "target": 2,  # ids
            "context": [{"positive": 3, "negative": 4}],  # ids
            "limit": 10,
            "using": "dense-image",
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
        "dense-image"
    )

    assert discover_result == query_result


def test_context():
    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "context": [{"positive": 2, "negative": 4}],
            "limit": 100,
            "using": "dense-image",
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
            "using": "dense-image",
        },
    )
    assert response.ok
    query_result = response.json()["result"]

    assert set([p["id"] for p in context_result]) == set([p["id"] for p in query_result])


def test_order_by():
    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "order_by": "count",
        },
    )
    assert response.ok
    scroll_result = response.json()["result"]["points"]

    query_result = root_and_rescored_query({"order_by": "count"}, "dense-image", with_payload=True)

    for record, scored_point in zip(scroll_result, query_result):
        assert record.get("id") == scored_point.get("id")
        assert record.get("payload") == scored_point.get("payload")


def test_rrf():
    filter = {
        "must": [
            {
                "key": "city",
                "match": {
                    "value": "Berlin"
                }
            }
        ]
    }
    response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": {
                "name": "dense-image",
                "vector": [0.1, 0.2, 0.3, 0.4]
            },
            "filter": filter,
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
            "vector": {
                "name": "dense-text",
                "vector": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
            },
            "filter": filter,
            "limit": 10,
        },
    )
    assert response.ok
    search_result_2 = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": {
                "name": "sparse-text",
                "vector": {
                    "indices": [63, 65, 66],
                    "values": [1, 2.2, 3.3],
                }
            },
            "filter": filter,
            "limit": 10,
        },
    )
    assert response.ok
    search_result_3 = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": {
                "name": "dense-multi",
                "vector": [3.05, 3.61, 3.76, 3.74], # legacy API expands single vector to multiple vectors
            },
            "filter": filter,
            "limit": 10,
        },
    )
    assert response.ok
    search_result_4 = response.json()["result"]

    rrf_expected = reciprocal_rank_fusion([search_result_1, search_result_2, search_result_3, search_result_4], limit=10)

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": [
                {
                    "query": [0.1, 0.2, 0.3, 0.4],
                    "using": "dense-image"
                },
                {
                    "query": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
                    "using": "dense-text"
                },
                {
                    "query": {
                        "indices": [63, 65, 66],
                        "values": [1, 2.2, 3.3],
                    },
                    "using": "sparse-text",
                },
                {
                    "query": [[3.05, 3.61, 3.76, 3.74]],
                    "using": "dense-multi"
                },
            ],
            "filter": filter,
            "limit": 10,
            "query": {"fusion": "rrf"}
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

    # test with inner filters instead of root filter
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": [
                {
                    "query": [0.1, 0.2, 0.3, 0.4],
                    "using": "dense-image",
                    "filter": filter,
                },
                {
                    "query": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
                    "using": "dense-text",
                    "filter": filter,
                },
                {
                    "query": {
                        "indices": [63, 65, 66],
                        "values": [1, 2.2, 3.3],
                    },
                    "using": "sparse-text",
                    "filter": filter,
                },
                {
                    "query": [[3.05, 3.61, 3.76, 3.74]],
                    "using": "dense-multi",
                    "filter": filter,
                },
            ],
            "limit": 10,
            "query": {"fusion": "rrf"}
        },
    )
    assert response.ok, response.json()
    rrf_inner_filter_result = response.json()["result"]

    for expected, result in zip(sorted(rrf_expected, key=get_id), sorted(rrf_inner_filter_result, key=get_id)):
        assert expected["id"] == result["id"]
        assert expected["payload"] == result["payload"]
        assert isclose(expected["score"], result["score"], rel_tol=1e-5)
