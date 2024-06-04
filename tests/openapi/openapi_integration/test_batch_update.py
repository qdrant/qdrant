from operator import itemgetter

import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection_batch_update'


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, on_disk_payload):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors,
                           on_disk_payload=on_disk_payload)
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

    for point, expected in zip(
        sorted(response.json()['result'], key=itemgetter('id')), 
        sorted(points, key=itemgetter('id'))
    ):
        assert point.get('id') == expected.get('id')
        assert point.get('payload') == expected.get('payload')
        assert point.get('vector') == expected.get('vector')


def test_batch_update():
    # Upsert and delete points
    response = request_with_validation(
        api="/collections/{collection_name}/points/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "operations": [
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
                {
                    "upsert": {
                        "points": [
                            {
                                "id": 9,
                                "vector": {
                                    "sparse-text": {
                                        "indices": [123, 321],
                                        "values": [5.5, 5.5]
                                    }
                                },
                                "payload": {},
                            },
                        ]
                    }
                },
                {"delete": {"points": [10]}},
            ]
        },
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

    assert_points(
        [
            {
                "id": 9,
                "vector":  {
                    "sparse-text": {
                        "indices": [123, 321],
                        "values": [5.5, 5.5]
                    },
                },
                "payload": {},
            }
        ],
        nonexisting_ids=[10],
        with_vectors=True,
    )

    # Update vector
    response = request_with_validation(
        api="/collections/{collection_name}/points/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "operations": [
                {
                    "update_vectors": {
                        "points": [
                            {
                                "id": 7,
                                "vector": [1.0, 2.0, 3.0, 4.0],
                            },
                        ]
                    }
                },
                {
                    "update_vectors": {
                        "points": [
                            {
                                "id": 7,
                                "vector": [9.0, 2.0, 4.0, 2.0],
                            },
                        ]
                    }
                },
            ]
        },
        query_params={"wait": "true"},
    )
    assert response.ok

    assert_points(
        [
            {
                "id": 7,
                "vector": [9.0, 2.0, 4.0, 2.0],
                "payload": {},
            }
        ],
        nonexisting_ids=[8],
        with_vectors=True,
    )

    # Upsert point and delete vector
    response = request_with_validation(
        api="/collections/{collection_name}/points/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "operations": [
                {
                    "upsert": {
                        "points": [
                            {
                                "id": 9,
                                "vector": [0.0, 5.0, 2.0, 1.0],
                                "payload": {},
                            },
                        ]
                    }
                },
                {
                    "delete_vectors": {
                        "points": [9],
                        "vector": [""],
                    }
                },
            ]
        },
        query_params={"wait": "true"},
    )
    assert response.ok

    assert_points(
        [
            {
                "id": 7,
                "vector": [9.0, 2.0, 4.0, 2.0],
                "payload": {},
            },
            {
                "id": 9,
                "vector": {},
                "payload": {},
            }
        ],
        nonexisting_ids=[8],
        with_vectors=True,
    )


def test_batch_update_payload():
    # Batch on multiple points
    response = request_with_validation(
        api="/collections/{collection_name}/points/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "operations": [
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
            ]
        },
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
        body={
            "operations": [
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
            ]
        },
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
        body={
            "operations": [
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
            ]
        },
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
