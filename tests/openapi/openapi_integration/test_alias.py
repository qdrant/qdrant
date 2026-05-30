import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection_alias'


@pytest.fixture(autouse=True)
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)
    drop_collection(collection_name=f'{collection_name}_2')


def test_cant_create_alias_if_collection_exists(on_disk_vectors):
    second_collection_name = f'{collection_name}_2'

    basic_collection_setup(collection_name=second_collection_name, on_disk_vectors=on_disk_vectors)

    response = request_with_validation(
        api='/collections/aliases',
        method="POST",
        body={
            "actions": [
                {
                    "create_alias": {
                        "alias_name": second_collection_name,
                        "collection_name": collection_name
                    }
                }
            ]
        }
    )
    assert not response.ok
    assert response.status_code == 409


def test_cant_create_collection_if_alias_exists(on_disk_vectors):
    second_collection_name = f'{collection_name}_3'

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': second_collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/aliases',
        method="POST",
        body={
            "actions": [
                {
                    "create_alias": {
                        "alias_name": second_collection_name,
                        "collection_name": collection_name
                    }
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/aliases',
        method="GET"
    )
    assert response.ok
    assert len(response.json()['result']['aliases']) == 1

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': second_collection_name},
        body={
            "vectors": {
                "size": 4,
                "distance": "Dot",
                "on_disk_vectors": on_disk_vectors,
            }
        }
    )

    assert not response.ok
    assert response.status_code == 400


def test_alias_operations():
    response = request_with_validation(
        api='/collections/aliases',
        method="POST",
        body={
            "actions": [
                {
                    "create_alias": {
                        "alias_name": "test_alias",
                        "collection_name": collection_name
                    }
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/aliases',
        method="GET"
    )
    assert response.ok
    assert len(response.json()['result']['aliases']) == 1
    first_alias = response.json()['result']['aliases'][0]
    assert first_alias['alias_name'] == 'test_alias'
    assert first_alias['collection_name'] == collection_name

    response = request_with_validation(
        api='/collections/{collection_name}/aliases',
        path_params={'collection_name': collection_name},
        method="GET"
    )
    assert response.ok
    assert len(response.json()['result']['aliases']) == 1
    first_alias = response.json()['result']['aliases'][0]
    assert first_alias['alias_name'] == 'test_alias'
    assert first_alias['collection_name'] == collection_name

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 3

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': "test_alias"},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 3

    response = request_with_validation(
        api='/collections/aliases',
        method="POST",
        body={
            "actions": [
                {
                    "delete_alias": {
                        "alias_name": "test_alias"
                    }
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/aliases',
        method="GET"
    )
    assert response.ok
    assert len(response.json()['result']['aliases']) == 0

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': "test_alias"},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 3
        }
    )
    assert response.status_code == 404
