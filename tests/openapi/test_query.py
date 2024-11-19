from math import isclose
import pytest
import requests
import os

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import distribution_based_score_fusion, reciprocal_rank_fusion, request_with_validation, \
    qdrant_host_headers
from .helpers.settings import QDRANT_HOST


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, collection_name):
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


def test_query_validation(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": [
                {
                    "query": {
                        "recommend": {
                            "positive": [1]
                        },
                    }
                }
            ]
        },
    )
    assert not response.ok, response.text
    assert response.json()["status"]["error"] == ("Bad request: A query is needed to merge the prefetches. Can't have prefetches without defining a query.")

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "score_threshold": 10,
        },
    )
    assert not response.ok, response.text
    assert response.json()["status"]["error"] == ("Bad request: A query is needed to use the score_threshold. Can't have score_threshold without defining a query.")

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "score_threshold": 10,
            "query": {
                "order_by": {
                    "key": "price",
                }
            }
        },
    )
    assert not response.ok, response.text
    assert response.json()["status"]["error"] == ("Bad request: Can't use score_threshold with an order_by query.")


    # raw query to bypass local validation
    response = requests.post(f"{QDRANT_HOST}/collections/{collection_name}/points/query",
        headers=qdrant_host_headers(),
        json={
            "query": {
                "recommend": {
                    "positive": [1]
                },
            },
            "limit": 0,
        },
    )
    assert not response.ok, response.text
    assert response.json()["status"]["error"] == ("Validation error in JSON body: [internal.limit: value 0 invalid, must be 1 or larger]")


def root_and_rescored_query(collection_name, query, limit=None, with_payload=None):
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
    root_query_result = response.json()["result"]["points"]

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
    nested_query_result = response.json()["result"]["points"]

    assert root_query_result == nested_query_result
    return root_query_result


def test_basic_search(collection_name):
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

    default_query_result = root_and_rescored_query(collection_name, [0.1, 0.2, 0.3, 0.4])

    nearest_query_result = root_and_rescored_query(collection_name, {"nearest": [0.1, 0.2, 0.3, 0.4]})

    assert search_result == default_query_result
    assert search_result == nearest_query_result


# Test basic search with huge limit, it must not panic with allocation failure
# See: <https://github.com/qdrant/qdrant/issues/5483>
def test_basic_search_high_limit(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [0.1, 0.2, 0.3, 0.4],
            "limit": 18446744073709551615, # u64::MAX
        },
    )
    assert response.ok
    search_result = response.json()["result"]

    default_query_result = root_and_rescored_query(
        collection_name,
        [0.1, 0.2, 0.3, 0.4],
        limit=18446744073709551615, # u64::MAX
    )

    nearest_query_result = root_and_rescored_query(
        collection_name,
        {"nearest": [0.1, 0.2, 0.3, 0.4]},
        limit=18446744073709551615, # u64::MAX
    )

    assert search_result == default_query_result
    assert search_result == nearest_query_result


def test_basic_scroll(collection_name):
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
    query_result = response.json()["result"]["points"]

    for record, scored_point in zip(scroll_result, query_result):
        assert record.get("id") == scored_point.get("id")
        assert record.get("payload") == scored_point.get("payload")

def test_basic_scroll_offset(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "offset": 2, # skip first record, start at id 2
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
            "offset": 1, # skip one record
        },
    )
    assert response.ok
    query_result = response.json()["result"]["points"]

    for record, scored_point in zip(scroll_result, query_result):
        assert record.get("id") == scored_point.get("id")
        assert record.get("payload") == scored_point.get("payload")

def test_basic_recommend_avg(collection_name):
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

    query_result = root_and_rescored_query(collection_name,
        {
            "recommend": {"positive": [1, 2, 3, 4], "negative": [3]},  # ids
        }
    )

    assert recommend_result == query_result


def test_basic_recommend_best_score(collection_name):
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
    query_result = response.json()["result"]["points"]

    assert recommend_result == query_result


def test_basic_discover(collection_name):
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

    query_result = root_and_rescored_query(collection_name,
        {
            "discover": {
                "target": 2,
                "context": [{"positive": 3, "negative": 4}],
            }
        },
    )

    assert discover_result == query_result


def test_basic_context(collection_name):
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
    query_result = response.json()["result"]["points"]

    assert set([p["id"] for p in context_result]) == set([p["id"] for p in query_result])


def test_basic_order_by(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "order_by": "price",
        },
    )
    assert response.ok, response.text
    scroll_result = response.json()["result"]["points"]

    query_result = root_and_rescored_query(collection_name, {"order_by": "price"}, with_payload=True)

    for record, scored_point in zip(scroll_result, query_result):
        assert record.get("id") == scored_point.get("id")
        assert record.get("payload") == scored_point.get("payload")


def test_basic_random_query(collection_name):
    ids_lists = set()
    for _ in range(4):
        response = request_with_validation(
            api="/collections/{collection_name}/points/query",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "query": { "sample": "random" }
            },
        )
        assert response.ok, response.text

        points = response.json()["result"]["points"]
        assert len(points) == 10
        assert set(point["id"] for point in points) == set(range(1, 11))

        ids = str([point["id"] for point in points])
        ids_lists.add(ids)

    # check the order of ids are different at least once
    assert len(ids_lists) > 1


def test_basic_rrf(collection_name):
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
    rrf_result = response.json()["result"]["points"]
    
    def get_id(x):
        return x["id"]
    
    # rrf order is not deterministic with same scores, so we need to sort by id
    for expected, result in zip(sorted(rrf_expected, key=get_id), sorted(rrf_result, key=get_id)):
        assert expected["id"] == result["id"]
        assert expected.get("payload") == result.get("payload")
        assert isclose(expected["score"], result["score"], rel_tol=1e-5)
        

def test_basic_dbsf(collection_name):
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
    
    dbsf_expected = distribution_based_score_fusion([search_result_1, search_result_2], limit=10)
    
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": [
                { "query": [0.1, 0.2, 0.3, 0.4] },
                { "query": [0.5, 0.6, 0.7, 0.8] },
            ],
            "query": {"fusion": "dbsf"},
        },
    )
    assert response.ok, response.json()
    dbsf_result = response.json()["result"]["points"]
    
    for point, expected in zip(dbsf_result, dbsf_expected):
        assert point["id"] == expected["id"]
        assert point.get("payload") == expected.get("payload")
        assert isclose(point["score"], expected["score"], rel_tol=1e-5)

    
@pytest.mark.parametrize("body", [
    {
        "prefetch": [
            { "query": [0.1, 0.2, 0.3, 0.4] },
            { "query": [0.5, 0.6, 0.7, 0.8] },
        ],
        "query": {"fusion": "rrf"},
    },
    { "query": [0.1, 0.2, 0.3, 0.4] }
])

def test_score_threshold(body, collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            **body
        },
    )
    assert response.ok, response.json()
    points = response.json()["result"]["points"]
    
    assert len(points) == 8
    score_threshold = points[3]["score"]
    
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "score_threshold": score_threshold,
            **body
        },
    )
    assert response.ok, response.json()
    points = response.json()["result"]["points"]
    
    assert len(points) < 8
    for point in points:
        assert point["score"] >= score_threshold
