import pytest

from openapi_integration.helpers.collection_setup import basic_collection_setup, drop_collection
from openapi_integration.helpers.helpers import request_with_validation

collection_name = 'test_collection'


@pytest.fixture(autouse=True)
def setup():
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_points_retrieve():
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 2},
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
    assert response.json()['result']['vectors_count'] == 6

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "top": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 3

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
                            "keyword": "London"
                        }
                    }
                ]
            },
            "vector": [0.2, 0.1, 0.9, 0.7],
            "top": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 2  # only 2 London records in collection

    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={"offset": 2, "limit": 2, "with_vector": True}
    )
    assert response.ok
    assert len(response.json()['result']) == 2


def test_exclude_payload():
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "top": 5,
            "filter": {
                "should": [
                    {
                        "key": "city",
                        "match": {
                            "keyword": "London"
                        }
                    }
                ]
            },
            "with_payload": {
                "exclude": ["city"]
            }
        }
    )
    assert response.ok
    assert len(response.json()['result']) > 0
    for result in response.json()['result']:
        assert 'city' not in result['payload']


def test_recommendation():
    response = request_with_validation(
        api='/collections/{collection_name}/points/recommend',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "top": 3,
            "negative": [],
            "positive": [1],
            "with_vector": False,
            "with_payload": True
        }
    )
    assert len(response.json()['result']) == 3
    assert response.json()['result'][0]['payload'] is not None
    assert response.ok
