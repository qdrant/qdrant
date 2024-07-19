from math import isclose

import pytest

from .helpers.collection_setup import drop_collection, full_collection_setup
from .helpers.helpers import reciprocal_rank_fusion, request_with_validation

collection_name = "test_query"
lookup_collection_name = "test_collection_lookup"


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
    assert response.ok, response.text

    # integer index on count
    response = request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        query_params={'wait': 'true'},
        path_params={"collection_name": collection_name},
        body={"field_name": "count", "field_schema": "integer"},
    )
    assert response.ok, response.text

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
    assert response.ok, response.text
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
            "filter": filter,
            "with_payload": with_payload,
            "using": using,
        },
    )
    assert response.ok, response.text
    nested_query_result = response.json()["result"]["points"]

    assert root_query_result == nested_query_result
    return root_query_result


def test_query_validation():
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": {
                "name": "dense-image",
                "vector": [0.1, 0.2, 0.3, 0.4],
            },
            "using": "dense-image",
            "query": {"fusion": "rrf"},
            "limit": 10,
        },
    )
    assert not response.ok, response.text
    assert response.json()["status"]["error"] == "Bad request: Fusion queries cannot be combined with the 'using' field."


def test_query_by_vector():
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
    assert response.ok, response.text
    search_result = response.json()["result"]

    default_query_result = root_and_rescored_query([0.1, 0.2, 0.3, 0.4], "dense-image")

    nearest_query_result = root_and_rescored_query({"nearest": [0.1, 0.2, 0.3, 0.4]}, "dense-image")

    assert search_result == default_query_result
    assert search_result == nearest_query_result


def test_query_by_id():
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": 2,
            "using": "dense-image",
        },
    )
    assert response.ok, response.text
    by_id_query_result = response.json()["result"]["points"]

    top = by_id_query_result[0]
    assert top["id"] != 2  # id 2 is excluded from the results


def test_filtered_query():
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
    assert response.ok, response.text
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
    assert response.ok, response.text
    scroll_result = response.json()["result"]["points"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "with_payload": True,
        },
    )
    assert response.ok, response.text
    query_result = response.json()["result"]["points"]

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
    assert response.ok, response.text
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
    assert response.ok, response.text
    query_result = response.json()["result"]["points"]

    for record, scored_point in zip(scroll_result, query_result):
        assert record.get("id") == scored_point.get("id")
        assert record.get("payload") == scored_point.get("payload")


def test_recommend_avg():
    response = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [1, 2, 3, 4],
            "negative": [3],
            "limit": 10,
            "using": "dense-image",
        },
    )
    assert response.ok, response.text
    recommend_result = response.json()["result"]

    query_result = root_and_rescored_query(
        {
            "recommend": {"positive": [1, 2, 3, 4], "negative": [3]},
        },
        "dense-image",
    )

    assert recommend_result == query_result


def test_recommend_lookup_validations():
    # delete lookup collection if exists
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': lookup_collection_name},
    )
    assert response.ok, response.text

    # re-create lookup collection
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': lookup_collection_name},
        body={
            "vectors": {
                "other": {
                    "size": 4,
                    "distance": "Dot",
                }
            }
        }
    )
    assert response.ok, response.text

    # insert vectors to lookup collection
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': lookup_collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {"other": [1.0, 0.0, 0.0, 0.0]},
                },
                {
                    "id": 2,
                    "vector": {"other": [0.0, 0.0, 0.0, 2.0]},
                },
            ]
        }
    )
    assert response.ok, response.text

    # check query + lookup_from non-existing id
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "recommend": {
                    "positive": [1],
                    "negative": [2, 3],
                },
            },
            "limit": 10,
            "using": "dense-image",
            "lookup_from": {
                "collection": lookup_collection_name,
                "vector": "other"
            }
        },
    )
    assert not response.ok, response.text
    assert response.json()["status"]["error"] == "Not found: No point with id 3 found"

    # check query + lookup_from non-existing collection
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "recommend": {
                    "positive": [1],
                    "negative": [2],
                },
            },
            "limit": 10,
            "using": "dense-image",
            "lookup_from": {
                "collection": "non-existing-collection",
                "vector": "other"
            }
        },
    )
    assert not response.ok, response.text
    assert response.json()["status"]["error"] == "Not found: Collection non-existing-collection not found"

    # check query + lookup_from non-existing vector
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "recommend": {
                    "positive": [1],
                    "negative": [2],
                },
            },
            "limit": 10,
            "using": "dense-image",
            "lookup_from": {
                "collection": lookup_collection_name,
                "vector": "non-existing-vector"
            }
        },
    )
    assert not response.ok, response.text
    assert response.json()["status"]["error"] == "Wrong input: Not existing vector name error: non-existing-vector"

    # check nested query + lookup_from non-existing id
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": [
                {
                    "query": {
                        "recommend": {
                            "positive": [1],
                            "negative": [2, 3],
                        },
                    },
                    "using": "dense-image",
                    "lookup_from": {
                        "collection": lookup_collection_name,
                        "vector": "other"
                    }
                }
            ],
            "limit": 10,
            "using": "dense-image",
            "query": {"fusion": "rrf"}
        },
    )
    assert not response.ok, response.text
    assert response.json()["status"]["error"] == "Not found: No point with id 3 found"

    # check nested query + lookup_from non-existing collection
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": [
                {
                    "query": {
                        "recommend": {
                            "positive": [1],
                            "negative": [2],
                        },
                    },
                    "using": "dense-image",
                    "lookup_from": {
                        "collection": "non-existing-collection",
                        "vector": "other"
                    }
                }
            ],
            "limit": 10,
            "using": "dense-image",
            "query": {"fusion": "rrf"}
        },
    )
    assert not response.ok, response.text
    assert response.json()["status"]["error"] == "Not found: Collection non-existing-collection not found"

    # check nested query + lookup_from non-existing vector
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": [
                {
                    "query": {
                        "recommend": {
                            "positive": [1],
                            "negative": [2],
                        },
                    },
                    "using": "dense-image",
                    "lookup_from": {
                        "collection": lookup_collection_name,
                        "vector": "non-existing-vector"
                    }
                }
            ],
            "limit": 10,
            "using": "dense-image",
            "query": {"fusion": "rrf"}
        },
    )
    assert not response.ok, response.text
    assert response.json()["status"]["error"] == "Wrong input: Not existing vector name error: non-existing-vector"


def test_recommend_lookup():
    # delete lookup collection if exists
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': lookup_collection_name},
    )
    assert response.ok, response.text

    # re-create lookup collection
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': lookup_collection_name},
        body={
            "vectors": {
                "other": {
                    "size": 4,
                    "distance": "Dot",
                }
            }
        }
    )
    assert response.ok, response.text

    # insert vectors to lookup collection
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': lookup_collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {"other": [1.0, 0.0, 0.0, 0.0]},
                },
                {
                    "id": 2,
                    "vector": {"other": [0.0, 0.0, 0.0, 2.0]},
                },
            ]
        }
    )
    assert response.ok, response.text

    # check recommend + lookup_from
    response = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [1],
            "negative": [2],
            "limit": 10,
            "using": "dense-image",
            "lookup_from": {
                "collection": lookup_collection_name,
                "vector": "other"
            }
        },
    )
    assert response.ok, response.text
    recommend_result = response.json()["result"]

    # check query + lookup_from
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "recommend": {
                    "positive": [1],
                    "negative": [2],
                },
            },
            "limit": 10,
            "using": "dense-image",
            "lookup_from": {
                "collection": lookup_collection_name,
                "vector": "other"
            }
        },
    )
    assert response.ok, response.text
    query_result = response.json()["result"]["points"]

    # check equivalence recommend vs query
    assert recommend_result == query_result, f"{recommend_result} != {query_result}"

    # check nested query id + lookup_from
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": [
                {
                    "query": {
                        "recommend": {
                            "positive": [1],
                            "negative": [2],
                        },
                    },
                    "using": "dense-image",
                    "lookup_from": {
                        "collection": lookup_collection_name,
                        "vector": "other"
                    }
                }
            ],
            "query": {"fusion": "rrf"}
        },
    )
    assert response.ok, response.text
    nested_query_result_id = response.json()["result"]["points"]

    # check nested query vector + lookup_from
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": [
                {
                    "query": {
                        "recommend": {
                            "positive": [[1.0, 0.0, 0.0, 0.0]],
                            "negative": [[0.0, 0.0, 0.0, 2.0]],
                        },
                    },
                    "using": "dense-image",
                }
            ],
            "query": {"fusion": "rrf"}
        },
    )
    assert response.ok, response.text
    nested_query_result_vector = response.json()["result"]["points"]

    # check equivalence nested query id vs nested query vector
    assert nested_query_result_id == nested_query_result_vector, f"{nested_query_result_id} != {nested_query_result_vector}"


def test_recommend_best_score():
    response = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [1, 2, 3, 4],
            "negative": [3],
            "limit": 10,
            "strategy": "best_score",
            "using": "dense-image",
        },
    )
    assert response.ok, response.text
    recommend_result = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "recommend": {
                    "positive": [1, 2, 3, 4],
                    "negative": [3],
                    "strategy": "best_score",
                },
            },
            "using": "dense-image",
        },
    )
    assert response.ok, response.text
    query_result = response.json()["result"]["points"]

    assert recommend_result == query_result


def test_discover():
    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "target": 2,
            "context": [{"positive": 3, "negative": 4}],
            "limit": 10,
            "using": "dense-image",
        },
    )
    assert response.ok, response.text
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
    assert response.ok, response.text
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
    assert response.ok, response.text
    query_result = response.json()["result"]["points"]

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
    assert response.ok, response.text
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
    assert response.ok, response.text
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
    assert response.ok, response.text
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
    assert response.ok, response.text
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
    assert response.ok, response.text
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
    rrf_result = response.json()["result"]["points"]

    def get_id(x):
        return x["id"]

    # rrf order is not deterministic with same scores, so we need to sort by id
    for expected, result in zip(sorted(rrf_expected, key=get_id), sorted(rrf_result, key=get_id)):
        assert expected["id"] == result["id"]
        assert expected.get("payload") == result.get("payload")
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
    rrf_inner_filter_result = response.json()["result"]["points"]

    for expected, result in zip(sorted(rrf_expected, key=get_id), sorted(rrf_inner_filter_result, key=get_id)):
        assert expected["id"] == result["id"]
        assert expected.get("payload") == result.get("payload")
        assert isclose(expected["score"], result["score"], rel_tol=1e-5)


def test_sparse_dense_rerank_colbert():
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
                    "query": {
                        "indices": [63, 65, 66],
                        "values": [1, 2.2, 3.3],
                    },
                    "using": "sparse-text",
                }
            ],
            "limit": 3,
            "query": [[3.05, 3.61, 3.76, 3.74]],
            "using": "dense-multi"
        },
    )
    assert response.ok, response.json()
    rerank_result = response.json()["result"]["points"]
    assert len(rerank_result) == 3
    # record current result to detect change
    assert rerank_result[0]["id"] == 5
    assert rerank_result[1]["id"] == 2
    assert rerank_result[2]["id"] == 1


def test_nearest_query_group():
    response = request_with_validation(
        api="/collections/{collection_name}/points/query/groups",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": [],
            "limit": 3,
            "query": [-1.9, 1.1, -1.1, 1.1],
            "using": "dense-image",
            "with_payload": True,
            "group_by": "city",
            "group_size": 2,
        },
    )
    groups = response.json()["result"]["groups"]
    # found 3 groups has requested with `limit`
    assert len(groups) == 3

    # group 1
    assert groups[0]["id"] == "Berlin"
    assert len(groups[0]["hits"]) == 2  # group_size
    assert groups[0]["hits"][0]["id"] == 1
    assert groups[0]["hits"][0]["payload"]["city"] == "Berlin"
    assert groups[0]["hits"][1]["id"] == 3
    assert groups[0]["hits"][1]["payload"]["city"] == ["Berlin", "Moscow"]

    # group 2
    assert groups[1]["id"] == "Moscow"
    assert len(groups[1]["hits"]) == 2  # group_size
    assert groups[1]["hits"][0]["id"] == 3
    assert groups[1]["hits"][0]["payload"]["city"] == ["Berlin", "Moscow"]
    assert groups[1]["hits"][1]["id"] == 4
    assert groups[1]["hits"][1]["payload"]["city"] == ["London", "Moscow"]

    # group 3
    assert groups[2]["id"] == "London"
    assert len(groups[2]["hits"]) == 2  # group_size
    assert groups[2]["hits"][0]["id"] == 2
    assert groups[2]["hits"][0]["payload"]["city"] == ["Berlin", "London"]
    assert groups[2]["hits"][1]["id"] == 4
    assert groups[2]["hits"][1]["payload"]["city"] == ["London", "Moscow"]


@pytest.mark.parametrize("strategy", [
    "best_score",
    "average_vector",
])
def test_recommend_group(strategy):
    response = request_with_validation(
        api="/collections/{collection_name}/points/recommend/groups",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [1, 2, 3, 4],
            "negative": [3],
            "limit": 10,
            "using": "dense-image",
            "with_payload": True,
            "strategy": strategy,
            "group_by": "city",
            "group_size": 2,
        },
    )
    assert response.ok, response.text
    recommend_result = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query/groups",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "recommend": {
                    "positive": [1, 2, 3, 4],
                    "negative": [3],
                },
            },
            "limit": 10,
            "using": "dense-image",
            "with_payload": True,
            "strategy": strategy,
            "group_by": "city",
            "group_size": 2,
        },
    )
    assert response.ok, response.text
    query_result = response.json()["result"]

    assert recommend_result == query_result, f"{recommend_result} != {query_result}"


@pytest.mark.parametrize("direction", [
    "asc",
    "desc",
])
def test_order_by_group(direction):
    # will check equivalence of scroll and query result with order_by group
    # where query uses a single result per group

    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "order_by": {
                "key": "count",
                "direction": direction,
            },
            "limit": 50,
        },
    )
    assert response.ok, response.text
    scroll_result = response.json()["result"]["points"]

    # keep only first result per payload value
    seen_payloads = set()
    filtered_scroll_result = []
    for record in scroll_result:
        if record["payload"]["count"] in seen_payloads:
            continue
        else:
            seen_payloads.add(record["payload"]["count"])
            filtered_scroll_result.append(record)

    response = request_with_validation(
        api="/collections/{collection_name}/points/query/groups",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "order_by": {
                    "key": "count",
                    "direction": direction,
                }
            },
            "limit": 10,
            "using": "dense-image",
            "with_payload": True,
            "group_by": "count",
            "group_size": 1,
        },
    )
    assert response.ok, response.text
    query_result = response.json()["result"]

    # flatten group result to match scroll result
    flatten_query_result = []
    for group in query_result["groups"]:
        flatten_query_result.extend(group["hits"])

    for record, scored_point in zip(filtered_scroll_result, flatten_query_result):
        assert record.get("id") == scored_point.get("id")
        assert record.get("payload") == scored_point.get("payload")


def test_discover_group():
    response = request_with_validation(
        api="/collections/{collection_name}/points/query/groups",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": [],
            "limit": 2,
            "query": {
                "discover": {
                    "target": 5,
                    "context": [{"positive": 3, "negative": 4}],
                }
            },
            "using": "dense-image",
            "with_payload": True,
            "group_by": "city",
            "group_size": 2,
        },
    )
    assert response.ok, response.text

    groups = response.json()["result"]["groups"]
    # found 2 groups has requested with `limit`
    assert len(groups) == 2

    # group 1
    assert groups[0]["id"] == "Berlin"
    assert len(groups[0]["hits"]) == 2  # group_size
    assert groups[0]["hits"][0]["id"] == 1
    assert groups[0]["hits"][0]["payload"]["city"] == "Berlin"
    assert groups[0]["hits"][1]["id"] == 2
    assert groups[0]["hits"][1]["payload"]["city"] == ["Berlin", "London"]

    # group 2
    assert groups[1]["id"] == "London"
    assert len(groups[1]["hits"]) == 1
    assert groups[1]["hits"][0]["id"] == 2
    assert groups[1]["hits"][0]["payload"]["city"] == ["Berlin", "London"]


def test_recommend_lookup_group():
    # delete lookup collection if exists
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': lookup_collection_name},
    )
    assert response.ok, response.text

    # re-create lookup collection
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': lookup_collection_name},
        body={
            "vectors": {
                "other": {
                    "size": 4,
                    "distance": "Dot",
                }
            }
        }
    )
    assert response.ok, response.text

    # insert vectors to lookup collection
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': lookup_collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {"other": [10.0, 10.0, 10.0, 10.0]},
                },
                {
                    "id": 2,
                    "vector": {"other": [20.0, 0.0, 0.0, 0.0]},
                },
            ]
        }
    )
    assert response.ok, response.text

    # check recommend group + lookup_from
    response = request_with_validation(
        api="/collections/{collection_name}/points/recommend/groups",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [1],
            "negative": [2],
            "limit": 10,
            "using": "dense-image",
            "lookup_from": {
                "collection": lookup_collection_name,
                "vector": "other"
            },
            "group_by": "city",
            "group_size": 2,
        },
    )
    assert response.ok, response.text
    recommend_result = response.json()["result"]

    # check query + lookup_from
    response = request_with_validation(
        api="/collections/{collection_name}/points/query/groups",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "recommend": {
                    "positive": [1],
                    "negative": [2],
                },
            },
            "limit": 10,
            "using": "dense-image",
            "lookup_from": {
                "collection": lookup_collection_name,
                "vector": "other"
            },
            "group_by": "city",
            "group_size": 2,
        },
    )
    assert response.ok, response.text
    query_result = response.json()["result"]

    # check equivalence recommend vs query
    assert recommend_result == query_result, f"{recommend_result} != {query_result}"

    # check nested query id + lookup_from
    response = request_with_validation(
        api="/collections/{collection_name}/points/query/groups",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": [
                {
                    "query": {
                        "recommend": {
                            "positive": [1],
                            "negative": [2],
                        },
                    },
                    "using": "dense-image",
                    "lookup_from": {
                        "collection": lookup_collection_name,
                        "vector": "other"
                    },
                }
            ],
            "group_by": "city",
            "group_size": 2,
            "query": {"fusion": "rrf"},
        },
    )
    assert response.ok, response.text
    nested_query_result_id = response.json()["result"]

    # check nested query vector + lookup_from
    response = request_with_validation(
        api="/collections/{collection_name}/points/query/groups",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": [
                {
                    "query": {
                        "recommend": {
                            "positive": [[10.0, 10.0, 10.0, 10.0]],
                            "negative": [[20.0, 0.0, 0.0, 0.0]],
                        },
                    },
                    "using": "dense-image",
                }
            ],
            "group_by": "city",
            "group_size": 2,
            "query": {"fusion": "rrf"},
        },
    )
    assert response.ok, response.text
    nested_query_result_vector = response.json()["result"]

    # check equivalence nested query id vs nested query vector
    assert nested_query_result_id == nested_query_result_vector, f"{nested_query_result_id} != {nested_query_result_vector}"
