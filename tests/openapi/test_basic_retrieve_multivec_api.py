import pytest

from .helpers.collection_setup import drop_collection, multivec_collection_setup
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, collection_name):
    multivec_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_points_retrieve(collection_name):
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
    assert response.json()['result']['points_count'] == 8

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": {"name": "image", "vector": [0.2, 0.1, 0.9, 0.7]},
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 3

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": {
                "name": "sparse-image",
                "vector": {
                    "indices": [2, 8],
                    "values": [0.2, 0.3]
                }
            },
            "limit": 2
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 2

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
            "vector": {
                "name": "text",
                "vector": [0.2, 0.1, 0.9, 0.7, 0.2, 0.1, 0.9, 0.7]
            },
            "limit": 3
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
    assert len(response.json()['result']['points']) == 2
    for point in response.json()['result']['points']:
        assert point['vector'] is not None
        assert len(point['vector']['text']) == 8
        assert len(point['vector']['image']) == 4

    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={"offset": 7, "limit": 2, "with_vector": True}
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 2
    for point in response.json()['result']['points']:
        assert point['vector'] is not None
        assert len(point['vector']['sparse-text']) == 2
        assert len(point['vector']['sparse-image']) == 2


def test_retrieve_invalid_vector(collection_name):
    # Retrieve nonexistent vector name
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "ids": [1, 2, 3, 4],
            "with_vectors": ["text", "image", "i_do_no_exist"],
            "with_payload": True,
        }
    )

    assert not response.ok
    assert response.status_code == 400
    error = response.json()["status"]["error"]
    assert error == "Wrong input: Not existing vector name error: i_do_no_exist"


def test_exclude_payload(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": {"name": "image", "vector": [0.2, 0.1, 0.9, 0.7]},
            "limit": 5,
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
            "with_payload": {
                "exclude": ["city"]
            }
        }
    )
    assert response.ok
    assert len(response.json()['result']) > 0
    for result in response.json()['result']:
        assert 'city' not in result['payload']


def test_is_empty_condition(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": {"name": "image", "vector": [0.2, 0.1, 0.9, 0.7]},
            "limit": 5,
            "filter": {
                "should": [
                    {
                        "is_empty": {
                            "key": "city"
                        }
                    }
                ]
            },
            "with_payload": True
        }
    )

    assert len(response.json()['result']) == 2
    for result in response.json()['result']:
        assert "city" not in result['payload']
    assert response.ok


def test_recommendation(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}/points/recommend',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "limit": 3,
            "negative": [],
            "positive": [1],
            "with_vector": False,
            "with_payload": True,
            "using": "image"
        }
    )
    assert len(response.json()['result']) == 3
    assert response.json()['result'][0]['payload'] is not None
    assert response.ok


def test_query_nested(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 8,
                    "vector": {
                        "image": [0.15, 0.31, 0.76, 0.74],
                        "text": [0.15, 0.31, 0.76, 0.74, 0.15, 0.31, 0.76, 0.74]
                    },
                    "payload": {
                        "database_id": {
                            "type": "keyword",
                            "value": "8594ff5d-265f-4785-a9f5-b3b4b9665506"
                        }
                    }
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "offset": None,
            "limit": 10,
            "with_vector": ["text"],
            "filter": {
                "must": [
                    {
                        "key": "database_id.value",
                        "match": {
                            "value": "8594ff5d-265f-4785-a9f5-b3b4b9665506"
                        }
                    }
                ]
            }
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 1
    assert 'text' in response.json()['result']['points'][0]['vector']
    assert len(response.json()['result']['points'][0]['vector']) == 1
