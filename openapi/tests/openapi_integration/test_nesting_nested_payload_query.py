import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection_nesting_nested_payload_query'


def nesting_nested_payload_collection_setup(collection_name, on_disk_vectors, on_disk_payload):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 4,
                "distance": "Dot",
                "on_disk": on_disk_vectors,
            },
            "on_disk_payload": on_disk_payload,
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": [0.05, 0.61, 0.76, 0.74],
                    "payload": {
                        "arr1": [
                            {
                                "arr2": [
                                    {"a": 1, "b": 2}
                                ]
                            },
                            {
                                "arr2": [
                                    {"a": 3, "b": 4},
                                    {"a": 5, "b": 6}
                                ]
                            }
                        ]
                    }
                },
                {
                    "id": 2,
                    "vector": [0.15, 0.71, 0.78, 0.24],
                    "payload": {
                        "arr1": [
                            {
                                "arr2": [
                                    {"a": 2, "b": 3}
                                ]
                            },
                            {
                                "arr2": [
                                    {"a": 3, "b": 5},
                                    {"a": 5, "b": 7}
                                ]
                            }
                        ]

                    }
                }
            ]
        }
    )
    assert response.ok


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, on_disk_payload):
    nesting_nested_payload_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors, on_disk_payload=on_disk_payload)
    yield
    drop_collection(collection_name=collection_name)


def test_nesting_nested_payload_query_operations():
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    # Create nested array index
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": "arr1[].arr2[].a",
            "field_schema": "integer"
        }
    )
    assert response.ok

    # Validate index creation
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert response.json()['result']['payload_schema']['arr1[].arr2[].a']['data_type'] == "integer"
    assert response.json()['result']['payload_schema']['arr1[].arr2[].a']['points'] == 2

    # Search nested field with payload index
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "nested": {
                            "key": "arr1",
                            "filter": {
                                "must": [
                                    {
                                        "key": "arr2[].a",
                                        "match": {
                                            "value": 5
                                        }
                                    }
                                ]
                            }
                        }
                    }
                ]
            },
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 2

    # Search nested field with payload index (negative)
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "nested": {
                            "key": "arr1",
                            "filter": {
                                "must": [
                                    {
                                        "key": "arr2[].a",
                                        "match": {
                                            "value": 4
                                        }
                                    }
                                ]
                            }
                        }
                    }
                ]
            },
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 0

    # Search nested field without payload index
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "nested": {
                            "key": "arr1",
                            "filter": {
                                "must": [
                                    {
                                        "key": "arr2[].b",
                                        "match": {
                                            "value": 6
                                        }
                                    }
                                ]
                            }
                        }
                    }
                ]
            },
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 1

    # Search nested field without payload index (negative)
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "nested": {
                            "key": "arr1[].arr2",
                            "filter": {
                                "must": [
                                    {
                                        "key": "b",
                                        "match": {
                                            "value": 8
                                        }
                                    }
                                ]
                            }
                        }
                    }
                ]
            },
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 0

    # Search doubly nested field with payload index
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "nested": {
                            "key": "arr1",
                            "filter": {
                                "must": [
                                    {
                                        "nested": {
                                            "key": "arr2",
                                            "filter": {
                                                "must": [
                                                    {
                                                        "key": "a",
                                                        "match": {
                                                            "value": 5
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                ]
            },
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 2

    # Search doubly nested field with and without payload index
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "nested": {
                            "key": "arr1",
                            "filter": {
                                "must": [
                                    {
                                        "nested": {
                                            "key": "arr2",
                                            "filter": {
                                                "must": [
                                                    {
                                                        "key": "a",
                                                        "match": {
                                                            "value": 5
                                                        }
                                                    },
                                                    {
                                                        "key": "b",
                                                        "match": {
                                                            "value": 6
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                ]
            },
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 1
    assert response.json()['result']['points'][0]['id'] == 1

    # Search doubly nested field with and without payload index
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "nested": {
                            "key": "arr1",
                            "filter": {
                                "must": [
                                    {
                                        "nested": {
                                            "key": "arr2",
                                            "filter": {
                                                "should": [
                                                    {
                                                        "key": "a",
                                                        "match": {
                                                            "value": 5
                                                        }
                                                    },
                                                    {
                                                        "key": "b",
                                                        "match": {
                                                            "value": 6
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                ]
            },
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 2
