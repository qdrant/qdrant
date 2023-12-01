import math
from os import path
import random

import pytest
from more_itertools import batched

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = "test_collection_order_by"
total_points = 1000


def upsert_points(collection_name, amount=100):
    def maybe_repeated():
        repeated_float = float(amount)
        while True:
            repeated_float = repeated_float if random.random() > 0.5 else repeated_float - 1.0
            yield repeated_float

    maybe_repeated_generator = maybe_repeated()
    points = [
        {
            "id": i,
            "vector": [0.1 * i] * 4,
            "payload": {
                "city": "London" if i % 3 == 0 else "Moscow",
                "price": float(amount - i),
                "payload_id": i,
                "multi_id": [i, amount - i],
                "maybe_repeated_float": next(maybe_repeated_generator),
            },
        }
        for i in range(amount)
    ]

    for batch in batched(points, 50):
        response = request_with_validation(
            api="/collections/{collection_name}/points",
            method="PUT",
            path_params={"collection_name": collection_name},
            query_params={"wait": "true"},
            body={"points": batch},
        )
        assert response.ok


def create_payload_index(collection_name, field_name, field_schema):
    response = request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={"field_name": field_name, "field_schema": field_schema},
    )
    assert response.ok, response.json()


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    upsert_points(collection_name=collection_name, amount=total_points)
    create_payload_index(collection_name=collection_name, field_name="city", field_schema="keyword")
    create_payload_index(collection_name=collection_name, field_name="price", field_schema="float")
    create_payload_index(
        collection_name=collection_name, field_name="maybe_repeated_float", field_schema="float"
    )
    create_payload_index(
        collection_name=collection_name, field_name="payload_id", field_schema="integer"
    )
    create_payload_index(
        collection_name=collection_name, field_name="multi_id", field_schema="integer"
    )
    yield
    drop_collection(collection_name=collection_name)


def test_order_by_int_ascending():
    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "order_by": {"key": "payload_id", "direction": "asc"},
            "limit": 5,
        },
    )
    assert response.ok, response.json()

    result = response.json()["result"]
    assert len(result["points"]) == 5

    ids = [x["id"] for x in result["points"]]
    assert [0, 1, 2, 3, 4] == ids
    assert result["next_page_offset"] == 5


def test_order_by_int_descending():
    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "order_by": {"key": "payload_id", "direction": "desc"},
            "limit": 5,
        },
    )
    assert response.ok, response.json()

    result = response.json()["result"]
    assert len(result["points"]) == 5

    ids = [x["id"] for x in result["points"]]
    assert [999, 998, 997, 996, 995] == ids
    assert result["next_page_offset"] == 994


@pytest.mark.parametrize(
    "key, direction",
    [
        ("payload_id", "asc"),
        ("payload_id", "desc"),
        ("price", "asc"),
        ("price", "desc"),
        ("maybe_repeated_float", "asc"),
        ("maybe_repeated_float", "desc"),
        #
        # ...Multi-valued fields don't work but they shouldn't be expected to work with pagination anyway
        # ("multi_id", "asc"),
        # ("multi_id", "desc"),
    ],
)
@pytest.mark.timeout(60)  # possibly break of an infinite loop
def test_paginate_whole_collection(key, direction):
    offset = None
    limit = 30
    pages = 0
    points_count = 0
    points_set = set()
    while True:
        response = request_with_validation(
            api="/collections/{collection_name}/points/scroll",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "order_by": {"key": key, "direction": direction},
                "limit": limit,
                "offset": offset,
            },
        )
        assert response.ok, response.json()
        offset = response.json()["result"]["next_page_offset"]

        points_len = len(response.json()["result"]["points"])

        points_count += points_len
        pages += 1

        # Check no duplicates
        for record in response.json()["result"]["points"]:
            assert record["id"] not in points_set
            points_set.add(record["id"])

        if offset is None:
            break
        else:
            assert points_len == limit

    assert math.ceil(total_points / limit) == pages
    assert total_points == points_count


@pytest.mark.parametrize(
    "key, direction",
    [
        ("payload_id", "asc"),
        ("payload_id", "desc"),
        ("price", "asc"),
        ("price", "desc"),
        ("maybe_repeated_float", "asc"),
        ("maybe_repeated_float", "desc"),
    ],
)
@pytest.mark.timeout(60)  # possibly break of an infinite loop
def test_order_by_with_filters(key, direction):
    offset = None
    limit = 30
    pages = 0
    points_count = 0
    points_set = set()
    
    filter_ = {
        "must": [
            {
                "key": "city",
                "match": {
                    "value": "London",
                }
            }
        ]
    }
    
    response = request_with_validation(
        api="/collections/{collection_name}/points/count",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "filter": filter_,
            "exact": True,
        },
    )
    assert response.ok, response.json()
    
    expected_points = response.json()["result"]["count"]
    
    while True:
        response = request_with_validation(
            api="/collections/{collection_name}/points/scroll",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "order_by": {"key": key, "direction": direction},
                "limit": limit,
                "offset": offset,
                "filter": filter_
            },
        )
        assert response.ok, response.json()
        offset = response.json()["result"]["next_page_offset"]

        points_len = len(response.json()["result"]["points"])

        points_count += points_len
        pages += 1

        # Check no duplicates
        for record in response.json()["result"]["points"]:
            assert record["id"] not in points_set
            points_set.add(record["id"])

        if offset is None:
            break
        else:
            assert points_len == limit

    assert math.ceil(expected_points / limit) == pages
    assert expected_points == points_count
