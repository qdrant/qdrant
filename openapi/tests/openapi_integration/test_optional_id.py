import pytest
import uuid

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation, is_valid_uuid

collection_name = 'test_collection'


@pytest.fixture(autouse=True)
def setup():
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_points_retrieve():
    points_retrieve()


def points_retrieve():
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "vector": [0.23, 0.74, 0.02, 0.93],
                    "payload": {"city": "Berlin"}
                },
                {
                    "vector": [0.41, 0.24, 0.90, 0.20],
                    "payload": {"city": ["Berlin", "London"]}
                },
                {
                    "vector": [0.43, 0.86, 0.91, 0.64]
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 1},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "ids": [1, 2]
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 2

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert response.json()['result']['vectors_count'] == 9

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.4, 0.2, 0.3, 0.8],
            "limit": 4
        }
    )
    assert response.ok
    assert response.json()['result'][0]['id'] == 3
    assert is_valid_uuid(response.json()['result'][1]['id'])
    assert is_valid_uuid(response.json()['result'][2]['id'])
    assert response.json()['result'][3]['id'] == 4
    assert len(response.json()['result']) == 4

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "should": [
                    {
                        "key": "city",
                        "match": {
                            "value": "London"
                        }
                    }
                ]
            },
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 3  # only 3 London records in collection


def test_points_unique():
    points_unique()


def points_unique():
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                { "vector": [0.23, 0.74, 0.02, 0.93] },
                { "vector": [0.23, 0.74, 0.02, 0.93] },
                { "vector": [0.23, 0.74, 0.02, 0.93] },
                { "vector": [0.41, 0.24, 0.90, 0.20] },
                { "vector": [0.41, 0.24, 0.90, 0.20] },
                { "vector": [0.41, 0.24, 0.90, 0.20] },
                { "vector": [0.43, 0.86, 0.91, 0.64] },
                { "vector": [0.43, 0.86, 0.91, 0.64] },
                { "vector": [0.43, 0.86, 0.91, 0.64] }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert response.json()['result']['vectors_count'] == 15
