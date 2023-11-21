import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection_filter_values_count'


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_filter_values_count():
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
                        "values_count": {
                            "gt": 2
                        }
                    }
                ]
            }
        }
    )
    assert response.ok

    json = response.json()
    assert len(json['result']) == 0

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
                        "values_count": {
                            "gte": 2
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
                        "values_count": {
                            "lt": 2
                        }
                    }
                ]
            }
        }
    )
    assert response.ok

    json = response.json()
    assert len(json['result']) == 3
    assert json['result'][0]['id'] == 1
