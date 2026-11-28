import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(scope="module")
def ordered_collection(collection_name):
    drop_collection(collection_name)
    path_params = {"collection_name": collection_name}
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PUT",
        path_params=path_params,
        body={"vectors": {"size": 2, "distance": "Dot"}, "shard_number": 1},
    )
    assert response.ok, response.text
    try:
        response = request_with_validation(
            api="/collections/{collection_name}/index",
            method="PUT",
            path_params=path_params,
            query_params={"wait": "true"},
            body={"field_name": "rank", "field_schema": "integer"},
        )
        assert response.ok, response.text

        response = request_with_validation(
            api="/collections/{collection_name}/points",
            method="PUT",
            path_params=path_params,
            query_params={"wait": "true"},
            body={
                "points": [
                    {"id": 1, "vector": [1, 0], "payload": {"rank": [1, 2]}},
                    {"id": 2, "vector": [0, 1], "payload": {"rank": [3]}},
                ]
            },
        )
        assert response.ok, response.text
        yield path_params
    finally:
        drop_collection(collection_name)


@pytest.mark.parametrize(
    ("direction", "expected"),
    [("asc", [(1, 1), (1, 2), (2, 3)]), ("desc", [(2, 3), (1, 2), (1, 1)])],
)
def test_query_order_by_keeps_adjacent_values(ordered_collection, direction, expected):
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params=ordered_collection,
        body={
            "query": {"order_by": {"key": "rank", "direction": direction}},
            "limit": 3,
            "with_payload": False,
        },
    )
    assert response.ok, response.text
    points = response.json()["result"]["points"]
    assert [(point["id"], point["order_value"]) for point in points] == expected
