import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import basic_collection_setup, drop_collection

collection_name = 'test_collection_alias'


@pytest.fixture(autouse=True)
def setup():
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


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
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': "test_alias"},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 3
        }
    )
    assert response.status_code == 404