import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import basic_collection_setup, drop_collection

from operator import itemgetter

collection_name = 'test_collection_payload'


@pytest.fixture(autouse=True)
def setup():
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def assert_points(points, nonexisting_ids=None, with_vectors=False):
    ids = [point['id'] for point in points]
    ids.extend(nonexisting_ids or [])

    if not with_vectors:
        for point in points:
            point['vector'] = None

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method='POST',
        path_params={'collection_name': collection_name},
        body={'ids': ids, 'with_vector': with_vectors, 'with_payload': True},
    )
    assert response.ok

    assert sorted(response.json()['result'], key=itemgetter('id')) == sorted(
        points, key=itemgetter('id')
    )


def test_payload_operations():
    # create payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "payload": {"test_payload": "keyword"},
            "points": [6]
        }
    )
    assert response.ok

    # check payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 6},
    )
    assert response.ok
    assert len(response.json()['result']['payload']) == 1

    # clean payload by filter
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload/clear',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "filter": {
                "must": [
                    {"has_id": [6]}
                ]
            }
        }
    )
    assert response.ok

    # check payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 6},
    )
    assert response.ok
    assert len(response.json()['result']['payload']) == 0

    # create payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "payload": {"test_payload": "keyword"},
            "points": [6]
        }
    )
    assert response.ok

    # delete payload by id
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "keys": ["test_payload"],
            "points": [6]
        }
    )
    assert response.ok

    # check payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 6},
    )
    assert response.ok
    assert len(response.json()['result']['payload']) == 0

    #
    # test PUT vs POST of the payload - set vs overwrite
    #
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "payload": {"key1": "aaa", "key2": "bbb"},
            "points": [6]
        }
    )
    assert response.ok

    # check payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 6},
    )
    assert response.ok
    assert len(response.json()['result']['payload']) == 2

    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "payload": {"key1": "ccc"},
            "points": [6]
        }
    )
    assert response.ok

    # check payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 6},
    )
    assert response.ok
    assert len(response.json()['result']['payload']) == 2
    assert response.json()['result']['payload']["key1"] == "ccc"
    assert response.json()['result']['payload']["key2"] == "bbb"

    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "payload": {"key2": "eee"},
            "points": [6]
        }
    )
    assert response.ok

    # check payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 6},
    )
    assert response.ok
    assert len(response.json()['result']['payload']) == 1
    assert response.json()['result']['payload']["key2"] == "eee"

    #
    # Check set and update of payload by filter
    #
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "payload": {"key4": "aaa"},
            "points": [1, 2, 3]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "key": "key4",
                        "match": {
                            "value": "aaa"
                        }
                    }
                ]
            }
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 3

    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "payload": {"key5": "bbb"},
            "filter": {
                "must": [
                    {
                        "key": "key4",
                        "match": {
                            "value": "aaa"
                        }
                    }
                ]
            }
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "key": "key5",
                        "match": {
                            "value": "bbb"
                        }
                    }
                ]
            }
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 3

    response = request_with_validation(
        api='/collections/{collection_name}/points/payload/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "keys": ["key5"],
            "filter": {
                "must": [
                    {
                        "key": "key5",
                        "match": {
                            "value": "bbb"
                        }
                    }
                ]
            }
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "key": "key5",
                        "match": {
                            "value": "bbb"
                        }
                    }
                ]
            }
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 0


def test_batch_update_payload():
    # Batch on multiple points
    response = request_with_validation(
        api="/collections/{collection_name}/points/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body=[
            {
                "overwrite_payload": {
                    "payload": {
                        "test_payload": "1",
                    },
                    "points": [1],
                },
            },
            {
                "overwrite_payload": {
                    "payload": {
                        "test_payload": "2",
                    },
                    "points": [2],
                },
            },
        ],
    )
    assert response.ok

    assert_points(
        [
            {
                "id": 1,
                "payload": {
                    "test_payload": "1",
                },
                "vector": None,
            },
            {
                "id": 2,
                "payload": {
                    "test_payload": "2",
                },
                "vector": None,
            },
        ]
    )

    # Clear multiple
    response = request_with_validation(
        api="/collections/{collection_name}/points/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body=[
            {
                "clear_payload": {
                    "points": [1],
                },
            },
            {
                "clear_payload": {
                    "points": [2],
                },
            },
        ],
        query_params={"wait": "true"},
    )
    assert response.ok

    assert_points(
        [
            {
                "id": 1,
                "payload": {},
            },
            {
                "id": 2,
                "payload": {},
            },
        ]
    )

    # Batch update on the same point
    response = request_with_validation(
        api="/collections/{collection_name}/points/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body=[
            {
                "overwrite_payload": {
                    "payload": {
                        "test_payload_1": "1",
                    },
                    "points": [1],
                },
            },
            {
                "set_payload": {
                    "payload": {
                        "test_payload_2": "2",
                        "test_payload_3": "3",
                    },
                    "points": [1],
                }
            },
            {
                "delete_payload": {
                    "keys": [
                        "test_payload_2",
                    ],
                    "points": [1],
                },
            },
        ],
        query_params={"wait": "true"},
    )
    assert response.ok

    assert_points(
        [
            {
                "id": 1,
                "payload": {
                    "test_payload_1": "1",
                    "test_payload_3": "3",
                },
            },
        ]
    )

    # Upsert and delete points
    response = request_with_validation(
        api="/collections/{collection_name}/points/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body=[
            {
                "upsert": {
                    "points": [
                        {
                            "id": 7,
                            "vector": [1.0, 2.0, 3.0, 4.0],
                            "payload": {},
                        },
                    ]
                }
            },
            {
                "upsert": {
                    "points": [
                        {
                            "id": 8,
                            "vector": [1.0, 2.0, 3.0, 4.0],
                            "payload": {},
                        },
                    ]
                }
            },
            {"delete": {"points": [8]}},
            {
                "upsert": {
                    "points": [
                        {
                            "id": 7,
                            "vector": [2.0, 1.0, 3.0, 4.0],
                            "payload": {},
                        },
                    ]
                }
            },
        ],
        query_params={"wait": "true"},
    )
    assert response.ok

    assert_points(
        [
            {
                "id": 7,
                "vector": [2.0, 1.0, 3.0, 4.0],
                "payload": {},
            }
        ],
        nonexisting_ids=[8],
        with_vectors=True,
    )
