import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)

    # add extra fields with other datatypes
    request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={
            "points": [
                {
                    "id": 100,
                    "vector": {},
                    "payload": {
                        "integer": 3,
                        "uuid": "a64a8051-3ee7-4881-8bc6-691d25c70d54",
                        "boolean": True,
                    },
                },
                {
                    "id": 101,
                    "vector": {},
                    "payload": {
                        "integer": 3,
                        "uuid": "a64a8051-3ee7-4881-8bc6-691d25c70d54",
                        "boolean": True,
                    },
                },
                {
                    "id": 102,
                    "vector": {},
                    "payload": {
                        "integer": 3,
                        "uuid": "7cb46fb3-e348-4762-93ad-7fb983d2b85e",
                        "boolean": False,
                    },
                },
                {
                    "id": 103,
                    "vector": {},
                    "payload": {
                        "integer": 0,
                        "uuid": "2b016c50-282c-4d80-b784-cefead291180",
                        "boolean": True,
                    },
                },
                {
                    "id": 104,
                    "vector": {},
                    "payload": {
                        "integer": 3,
                        "uuid": "a64a8051-3ee7-4881-8bc6-691d25c70d54",
                        "boolean": True,
                    },
                },
                {
                    "id": 105,
                    "vector": {},
                    "payload": {
                        "integer": 1,
                        "uuid": "a64a8051-3ee7-4881-8bc6-691d25c70d54",
                        "boolean": False,
                    },
                },
                {
                    "id": 106,
                    "vector": {},
                    "payload": {
                        "integer": 2,
                        "uuid": "7cb46fb3-e348-4762-93ad-7fb983d2b85e",
                        "boolean": False,
                    },
                },
                {
                    "id": 107,
                    "vector": {},
                    "payload": {
                        "integer": 0,
                        "uuid": "a64a8051-3ee7-4881-8bc6-691d25c70d54",
                        "boolean": True,
                    },
                },
            ]
        },
    ).raise_for_status()

    request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={
            "field_name": "city",
            "field_schema": "keyword",
        },
    ).raise_for_status()

    request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={
            "field_name": "integer",
            "field_schema": "integer",
        },
    ).raise_for_status()

    request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={
            "field_name": "uuid",
            "field_schema": "uuid",
        },
    ).raise_for_status()

    request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={
            "field_name": "boolean",
            "field_schema": "bool",
        },
    ).raise_for_status()

    yield
    drop_collection(collection_name=collection_name)


def test_basic_facet(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/facet",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "key": "city",
            # limit is optional
        },
    )

    assert response.ok, response.json()

    city_facet = response.json()["result"]
    assert city_facet == {
        "hits": [
            # Sorted by count, then by value
            {"value": "Berlin", "count": 3},
            {"value": "London", "count": 2},
            {"value": "Moscow", "count": 2},
        ]
    }


def test_integer_facet(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/facet",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "key": "integer",
        },
    )

    assert response.ok, response.json()

    city_facet = response.json()["result"]
    assert city_facet == {
        "hits": [
            # Sorted by count, then by value
            {"value": 3, "count": 4},
            {"value": 0, "count": 2},
            {"value": 1, "count": 1},
            {"value": 2, "count": 1},
        ]
    }


def test_uuid_facet(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/facet",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "key": "uuid",
        },
    )

    assert response.ok, response.json()

    city_facet = response.json()["result"]
    assert city_facet == {
        "hits": [
            {"value": "a64a8051-3ee7-4881-8bc6-691d25c70d54", "count": 5},
            {"value": "7cb46fb3-e348-4762-93ad-7fb983d2b85e", "count": 2},
            {"value": "2b016c50-282c-4d80-b784-cefead291180", "count": 1},
        ]
    }


def test_boolean_facet(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/facet",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "key": "boolean",
        },
    )

    assert response.ok, response.json()

    city_facet = response.json()["result"]
    assert city_facet == {
        "hits": [
            # Sorted by count, then by value
            {"value": True, "count": 5},
            {"value": False, "count": 3},
        ]
    }
