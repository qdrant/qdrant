import uuid
from math import isclose

import pytest

from .helpers.collection_setup import drop_collection, full_collection_setup
from .helpers.helpers import reciprocal_rank_fusion, request_with_validation


uuid_1 = str(uuid.uuid4())
uuid_2 = str(uuid.uuid4())
uuid_3 = str(uuid.uuid4())


@pytest.fixture(scope='module', autouse=True)
def lookup_collection_name(collection_name):
    return f"{collection_name}_lookup"


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, collection_name):
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

    # UUID index
    def set_payload(payload, points):
        response = request_with_validation(
            api='/collections/{collection_name}/points/payload',
            method="POST",
            path_params={'collection_name': collection_name},
            query_params={'wait': 'true'},
            body={
                "payload": payload,
                "points": points
            }
        )
        assert response.ok

    # create payload
    set_payload({"uuid": uuid_1}, [1])
    set_payload({"uuid": uuid_2}, [2])
    set_payload({"uuid": uuid_3}, [3])

    # Create index
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": "uuid",
            "field_schema": "uuid"
        }
    )
    assert response.ok

    yield
    drop_collection(collection_name=collection_name)


def root_and_rescored_query(collection_name, query, using, filter=None, limit=None, with_payload=None):
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


def test_query_validation(collection_name):
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


def test_query_by_vector(collection_name):
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

    default_query_result = root_and_rescored_query(collection_name, [0.1, 0.2, 0.3, 0.4], "dense-image")

    nearest_query_result = root_and_rescored_query(collection_name, {"nearest": [0.1, 0.2, 0.3, 0.4]}, "dense-image")

    assert search_result == default_query_result
    assert search_result == nearest_query_result


def test_query_by_id(collection_name):
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


def test_filtered_query(collection_name):
    filters = [
        {
            "must": [
                {
                    "key": "city",
                    "match": {
                        "value": "Berlin"
                    }
                }
            ]
        },
        {
            "must_not": [
                {
                    "key": "city",
                    "match": {
                        "value": "Berlin"
                    }
                }
            ]
        },
        {
            "should": [
                {
                    "key": "city",
                    "match": {
                        "value": "Berlin"
                    }
                }
            ]
        },
        {
            "min_should": {
                "conditions": [
                    {
                        "key": "city",
                        "match": {
                            "any": ["Berlin", "Moscow"]
                        }
                    },
                    {
                        "key": "count",
                        "match": {
                            "value": 0
                        }
                    }
                ],
                "min_count": 2
            }
        }
    ]
    for filter in filters:
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

        default_query_result = root_and_rescored_query(collection_name, [0.1, 0.2, 0.3, 0.4], "dense-image", filter)

        nearest_query_result = root_and_rescored_query(collection_name, {"nearest": [0.1, 0.2, 0.3, 0.4]}, "dense-image", filter)

        assert search_result == default_query_result
        assert search_result == nearest_query_result


def test_uuid_index_filtered_query(collection_name):
    filters_arr = get_uuid_index_filters()

    for item in filters_arr:
        response = request_with_validation(
            api="/collections/{collection_name}/points/search",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "vector": {"vector": [0.1, 0.2, 0.3, 0.4], "name": "dense-image"},
                "filter": item,
                "limit": 10,
            },
        )
        assert response.ok, f"{response.text}\n{item}"
        search_result = response.json()["result"]

        default_query_result = root_and_rescored_query(collection_name, [0.1, 0.2, 0.3, 0.4], "dense-image", filter=item)

        nearest_query_result = root_and_rescored_query(collection_name, {"nearest": [0.1, 0.2, 0.3, 0.4]}, "dense-image", filter=item)

        assert search_result == default_query_result
        assert search_result == nearest_query_result


def test_scroll(collection_name):
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


def test_filtered_scroll(collection_name):
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


def get_uuid_index_filters():
    # Check different filters
    filters_arr = []
    match_conditions = [
        {"value": uuid_1},
        {"text": uuid_2},
        {"any": [uuid_1, uuid_2]},
        {"except": [uuid_1, uuid_2]}
    ]

    for item in ["must", "must_not", "should"]:
        for condition in match_conditions:
            filters_arr.append(
                {
                    item: [
                        {
                            "key": "uuid",
                            "match": condition
                        }
                    ]
                }
            )

    # min_should
    for condition in match_conditions:
        filters_arr.append(
            {
                "min_should": {
                    "conditions": [
                        {
                            "key": "uuid",
                            "match": condition
                        }
                    ],
                "min_count": 2
            }
            }
        )
    return filters_arr


@pytest.mark.parametrize("query_filter", [None, *get_uuid_index_filters()])
def test_recommend_avg(query_filter, collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [1, 2, 3, 4],
            "negative": [3],
            "limit": 10,
            "using": "dense-image",
            "filter": query_filter
        },
    )
    assert response.ok, response.text
    recommend_result = response.json()["result"]

    query_result = root_and_rescored_query(collection_name,
        {
            "recommend": {"positive": [1, 2, 3, 4], "negative": [3]},
        },
        "dense-image",
        filter=query_filter
    )

    assert recommend_result == query_result


def test_recommend_lookup_validations(collection_name, lookup_collection_name):
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


def test_recommend_lookup(collection_name, lookup_collection_name):
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


def test_recommend_best_score(collection_name):
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


@pytest.mark.parametrize("query_filter", [None, *get_uuid_index_filters()])
def test_discover(query_filter, collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "target": 2,
            "context": [{"positive": 3, "negative": 4}],
            "limit": 10,
            "using": "dense-image",
            "filter": query_filter
        },
    )
    assert response.ok, response.text
    discover_result = response.json()["result"]

    query_result = root_and_rescored_query(collection_name,
        {
            "discover": {
                "target": 2,
                "context": [{"positive": 3, "negative": 4}],
            }
        },
        "dense-image",
        filter=query_filter
    )

    assert discover_result == query_result


def test_context(collection_name):
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


def test_order_by(collection_name):
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

    query_result = root_and_rescored_query(collection_name, {"order_by": "count"}, "dense-image", with_payload=True)

    for record, scored_point in zip(scroll_result, query_result):
        assert record.get("id") == scored_point.get("id")
        assert record.get("payload") == scored_point.get("payload")


def test_rrf(collection_name):
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


def test_sparse_dense_rerank_colbert(collection_name):
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


def test_nearest_query_group(collection_name):
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
def test_recommend_group(strategy, collection_name):
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
def test_order_by_group(direction, collection_name):
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


def test_discover_group(collection_name):
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


def test_recommend_lookup_group(collection_name, lookup_collection_name):
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


def test_random_rescore_with_offset(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": { "limit": 1 },
            "query": {"sample": "random"},
        },
    )
    assert response.ok, response.json()
    random_result = response.json()["result"]["points"]
    assert len(random_result) == 1
    assert random_result[0]["id"] == 1
    
    # assert offset is propagated to prefetch
    seen = set()
    for _ in range(100):
        response = request_with_validation(
            api="/collections/{collection_name}/points/query",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "prefetch": { "limit": 1 },
                "query": {"sample": "random"},
                "offset": 1,
            },
        )
        assert response.ok, response.json()
        random_result = response.json()["result"]["points"]
        assert len(random_result) == 1
    
        seen.add(random_result[0]["id"])
        if seen == {1, 2}:
            return
    
    # Although prefetch limit is 1, offset should be propagated, so randomness is applied to points 1 and 2.
    # By this point we should've seen both points.
    assert False, f"after 100 tries, `seen` is expected to be {{1, 2}}, but it was {seen}"


@pytest.mark.parametrize("query_filter", [None, *get_uuid_index_filters()])
def test_nearest_query_batch(query_filter, collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/search/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "searches": [
                {
                    "limit": 3,
                    "vector": {
                        "vector": [-1.9, 1.1, -1.1, 1.1],
                        "name": "dense-image"
                    },
                    "with_payload": True,
                    "filter": query_filter
                },
                {
                    "limit": 3,
                    "vector": {
                        "vector": [0.19, 0.83, 0.75, -0.11],
                        "name": "dense-image",
                    },
                    "with_payload": True,
                    "filter": query_filter
                }
            ]
        },
    )
    assert response.ok, response.text
    search_result = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "searches": [
                {
                    "limit": 3,
                    "query": [-1.9, 1.1, -1.1, 1.1],
                    "using": "dense-image",
                    "with_payload": True,
                    "filter": query_filter
                },
                {
                    "limit": 3,
                    "query": [0.19, 0.83, 0.75, -0.11],
                    "using": "dense-image",
                    "with_payload": True,
                    "filter": query_filter
                }
            ]
        },
    )
    assert response.ok, response.text
    query_result = response.json()["result"]

    assert search_result[0] == query_result[0]["points"]
    assert search_result[1] == query_result[1]["points"]


@pytest.mark.parametrize("query_filter", [None, *get_uuid_index_filters()])
def test_recommend_batch(query_filter, collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/recommend/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "searches": [
                {
                    "positive": [1, 2, 3, 4],
                    "negative": [3],
                    "limit": 10,
                    "using": "dense-image",
                    "filter": query_filter,
                    "with_payload": True,
                },
                {
                    "positive": [3, 4],
                    "negative": [4],
                    "limit": 10,
                    "using": "dense-image",
                    "filter": query_filter,
                    "with_payload": True,
                }
            ]
        },
    )
    assert response.ok, response.text
    search_result = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "searches": [
                {
                    "limit": 10,
                    "query": {"recommend": {"positive": [1, 2, 3, 4], "negative": [3]}},
                    "using": "dense-image",
                    "with_payload": True,
                    "filter": query_filter
                },
                {
                    "limit": 10,
                    "query": {"recommend": {"positive": [3, 4], "negative": [4]}},
                    "using": "dense-image",
                    "with_payload": True,
                    "filter": query_filter
                }
            ]
        },
    )
    assert response.ok, response.text
    query_result = response.json()["result"]

    assert search_result[0] == query_result[0]["points"]
    assert search_result[1] == query_result[1]["points"]


@pytest.mark.parametrize("query_filter", [None, *get_uuid_index_filters()])
def test_discover_batch(query_filter, collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/discover/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "searches": [
                {
                    "target": 2,
                    "context": [{"positive": 3, "negative": 4}],
                    "limit": 10,
                    "using": "dense-image",
                    "filter": query_filter,
                    "with_payload": True,
                },
                {
                    "target": 4,
                    "context": [{"positive": 1, "negative": 2}],
                    "limit": 10,
                    "using": "dense-image",
                    "filter": query_filter,
                    "with_payload": True,
                }
            ]
        },
    )
    assert response.ok, response.text
    search_result = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/query/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "searches": [
                {
                    "limit": 10,
                    "query": {
                        "discover": {
                            "target": 2,
                            "context": [{"positive": 3, "negative": 4}],
                        }
                    },
                    "using": "dense-image",
                    "with_payload": True,
                    "filter": query_filter
                },
                {
                    "limit": 10,
                    "query": {
                        "discover": {
                            "target": 4,
                            "context": [{"positive": 1, "negative": 2}],
                        }
                    },
                    "using": "dense-image",
                    "with_payload": True,
                    "filter": query_filter
                }
            ]
        },
    )
    assert response.ok, response.text
    query_result = response.json()["result"]

    assert search_result[0] == query_result[0]["points"]
    assert search_result[1] == query_result[1]["points"]


# Qdrant did panic for some Query API requests when using a vector name that is not existing
# for the given point. This tests ensures that a proper error response gets returned.
# See https://github.com/qdrant/qdrant/issues/5208 for more details.
def test_query_with_missing_vector(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": 7,
            "using": "sparse-text"
        },
    )
    assert response.ok

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": 8,  # Point with ID=8 doesn't have a vector 'sparse-text' which caused Qdrant to panic before.
            "using": "sparse-text"
        },
    )
    assert not response.ok
    assert 'error' in response.json()['status']

    response2 = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": 8,  # Point with ID=8 doesn't have a default vector which caused Qdrant to panic before.
        },
    )
    assert not response2.ok
    assert 'error' in response2.json()['status']
