import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection_filter'


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_match_any():
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 3,
            "filter": {
                "must": [
                    {
                        "key": "city",
                        "match": {
                            "any": ["London", "Moscow"]
                        }
                    }
                ]
            }
        }
    )
    assert response.ok

    json = response.json()
    assert len(json['result']) == 3

    ids = [x['id'] for x in json['result']]
    assert 2 in ids
    assert 3 in ids
    assert 4 in ids


def test_just_key():
    # the filter will be ignored as the condition is not well-formed
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 3,
            "filter": {
                "must": [
                    {
                        "key": "city",
                    }
                ]
            }
        }
    )
    assert not response.ok
    assert response.status_code == 422
    error = response.json()["status"]["error"]
    assert "Validation error in JSON body" in error
    assert "At least one field condition must be specified" in error
