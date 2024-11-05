import uuid

import pytest

from .helpers.collection_setup import drop_collection, full_collection_setup
from .helpers.helpers import request_with_validation

uuid_1 = str(uuid.uuid4())
uuid_2 = str(uuid.uuid4())
uuid_3 = str(uuid.uuid4())


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, collection_name):
    full_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)

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

    # keyword index on `keyword`
    set_payload({"keyword": uuid_1}, [1])
    set_payload({"keyword": uuid_2}, [2])
    set_payload({"keyword": uuid_3}, [3])
    response = request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        query_params={'wait': 'true'},
        path_params={"collection_name": collection_name},
        body={"field_name": "keyword", "field_schema": "keyword"},
    )
    assert response.ok, response.text

    # UUID index on `uuid`
    set_payload({"uuid": uuid_1}, [1])
    set_payload({"uuid": uuid_2}, [2])
    set_payload({"uuid": uuid_3}, [3])

    # Create index
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"field_name": "uuid", "field_schema": "uuid"}
    )
    assert response.ok

    yield
    drop_collection(collection_name=collection_name)


def get_index_filters(filed_name):
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
                            "key": filed_name,
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
                            "key": filed_name,
                            "match": condition
                        }
                    ],
                "min_count": 2
            }
            }
        )
    return filters_arr


@pytest.mark.parametrize("query", [
    [0.1, 0.2, 0.3, 0.4],
    {"nearest": [0.1, 0.2, 0.3, 0.4]},
    2,
    {"recommend": {"positive": [1, 2, 3, 4], "negative": [3]}},
    {
        "discover": {
            "target": 2,
            "context": [{"positive": 3, "negative": 4}],
        }
    },
    {"context": [{"positive": 2, "negative": 4}]},
], ids=[
    "default query", "nearest query", "query by id",
    "recommend", "discover", "context"
])
def test_filtered_query_results_same_for_different_indexes(query, collection_name):
    for uuid_filter, keyword_filter in zip([None, *get_index_filters("uuid")], [None, *get_index_filters("keyword")]):
        uuid_response = request_with_validation(
            api="/collections/{collection_name}/points/query",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "query": query,
                "limit": 10,
                "filter": uuid_filter,
                "with_payload": True,
                "using": "dense-image",
            },
        )
        assert uuid_response.ok, uuid_response.text
        uuid_query_result = uuid_response.json()["result"]["points"]

        keyword_response = request_with_validation(
            api="/collections/{collection_name}/points/query",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "query": query,
                "limit": 10,
                "filter": keyword_filter,
                "with_payload": True,
                "using": "dense-image",
            },
        )
        assert keyword_response.ok, keyword_response.text
        keyword_query_result = keyword_response.json()["result"]["points"]

        assert uuid_query_result == keyword_query_result, uuid_filter


def test_filtered_query_groups_results_same_for_different_indexes(collection_name):
    for uuid_filter, keyword_filter in zip([None, *get_index_filters("uuid")], [None, *get_index_filters("keyword")]):
        uuid_response = request_with_validation(
            api="/collections/{collection_name}/points/query/groups",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "prefetch": [],
                "limit": 3,
                "query": [-1.9, 1.1, -1.1, 1.1],
                "using": "dense-image",
                "with_payload": True,
                "group_by": "uuid",
                "group_size": 2,
                "filter": uuid_filter
            },
        )
        assert uuid_response.ok, uuid_response.text
        uuid_query_result = uuid_response.json()["result"]["groups"]

        keyword_response = request_with_validation(
            api="/collections/{collection_name}/points/query/groups",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "prefetch": [],
                "limit": 3,
                "query": [-1.9, 1.1, -1.1, 1.1],
                "using": "dense-image",
                "with_payload": True,
                "group_by": "keyword",
                "group_size": 2,
                "filter": keyword_filter
            },
        )
        assert keyword_response.ok, keyword_response.text
        keyword_query_result = keyword_response.json()["result"]["groups"]

        assert uuid_query_result == keyword_query_result, uuid_filter


def test_filtered_query_batches_results_same_for_different_indexes(collection_name):
    for uuid_filter, keyword_filter in zip([None, *get_index_filters("uuid")], [None, *get_index_filters("keyword")]):
        uuid_response = request_with_validation(
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
                        "filter": uuid_filter,
                    },
                    {
                        "limit": 3,
                        "query": [0.19, 0.83, 0.75, -0.11],
                        "using": "dense-image",
                        "with_payload": True,
                        "filter": uuid_filter
                    }
                ]
            },
        )
        assert uuid_response.ok, uuid_response.text
        uuid_query_result = uuid_response.json()["result"][0]["points"]

        keyword_response = request_with_validation(
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
                        "filter": keyword_filter
                    },
                    {
                        "limit": 3,
                        "query": [0.19, 0.83, 0.75, -0.11],
                        "using": "dense-image",
                        "with_payload": True,
                        "filter": keyword_filter
                    }
                ]
            },
        )
        assert keyword_response.ok, keyword_response.text
        keyword_query_result = keyword_response.json()["result"][0]["points"]

        assert uuid_query_result == keyword_query_result, uuid_filter

