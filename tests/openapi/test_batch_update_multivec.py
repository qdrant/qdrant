import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import multivec_collection_setup, drop_collection

from operator import itemgetter


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, collection_name):
    multivec_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors, distance="Dot")
    yield
    drop_collection(collection_name=collection_name)


def assert_points(collection_name, points, nonexisting_ids=None, with_vectors=False):
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


def test_batch_update(collection_name):
    # Upsert and delete points
    response = request_with_validation(
        api="/collections/{collection_name}/points/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={"operations": [
            {
                "upsert": {
                    "points": [
                        {
                            "id": 7,
                            "vector": {
                                "image": [1.0, 0.0, 9.0, 1.0],
                            },
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
                            "vector": {
                                "image": [7.0, 1.0, 6.0, 9.0],
                            },
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
                            "vector": {
                                "image": [9.0, 9.0, 3.0, 0.0],
                                "text": [8.0, 1.0, 2.0, 5.0, 2.0, 7.0, 1.0, 6.0],
                            },
                            "payload": {},
                        },
                    ]
                }
            },
        ]
        },
        query_params={"wait": "true"},
    )
    assert response.ok

    assert_points(collection_name,
                  [
            {
                "id": 7,
                "vector": {
                    "image": [9.0, 9.0, 3.0, 0.0],
                    "text": [8.0, 1.0, 2.0, 5.0, 2.0, 7.0, 1.0, 6.0],
                },
                "payload": {},
            }
        ],
        nonexisting_ids=[8],
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
                                "vector": {
                                    "image": [2.0, 6.0, 3.0, 2.0],
                                    "text": [5.0, 7.0, 0.0, 8.0, 2.0, 7.0, 1.0, 6.0],
                                },
                            },
                        ]
                    }
                },
                {
                    "update_vectors": {
                        "points": [
                            {
                                "id": 7,
                                "vector": {
                                    "image": [0.0, 3.0, 1.0, 8.0],
                                },
                            },
                        ]
                    }
                },
            ]
        },
        query_params={"wait": "true"},
    )
    assert response.ok

    assert_points(collection_name,
                  [
            {
                "id": 7,
                "vector": {
                    "image": [0.0, 3.0, 1.0, 8.0],
                    "text": [5.0, 7.0, 0.0, 8.0, 2.0, 7.0, 1.0, 6.0],
                },
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
                                "vector": {
                                    "image": [5.0, 5.0, 3.0, 6.0],
                                    "text": [8.0, 4.0, 7.0, 3.0, 3.0, 8.0, 9.0, 0.0],
                                },
                                "payload": {},
                            },
                        ]
                    }
                },
                {
                    "upsert": {
                        "points": [
                            {
                                "id": 10,
                                "vector": {
                                    "image": [2.0, 9.0, 1.0, 4.0],
                                    "text": [1.0, 5.0, 5.0, 6.0, 9.0, 3.0, 3.0, 2.0],
                                },
                                "payload": {},
                            },
                        ]
                    }
                },
                {
                    "delete_vectors": {
                        "points": [7, 10],
                        "vector": ["text"],
                    }
                },
                {
                    "delete_vectors": {
                        "points": [9, 10],
                        "vector": ["image"],
                    }
                },
            ]
        },
        query_params={"wait": "true"},
    )
    assert response.ok

    assert_points(collection_name,
                  [
            {
                "id": 7,
                "vector": {
                    "image": [0.0, 3.0, 1.0, 8.0],
                },
                "payload": {},
            },
            {
                "id": 9,
                "vector": {
                    "text": [8.0, 4.0, 7.0, 3.0, 3.0, 8.0, 9.0, 0.0],
                },
                "payload": {},
            },
            {
                "id": 10,
                "vector": {},
                "payload": {},
            }
        ],
        nonexisting_ids=[8],
        with_vectors=True,
    )
