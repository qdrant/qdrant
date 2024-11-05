import pytest

from .helpers.collection_setup import multivec_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, collection_name):
    multivec_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_filter_has_vector(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}/points/query',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "limit": 3,
            "filter": {
                "must": [
                    {
                        "has_vector": "sparse-text"
                    }
                ]
            }
        }
    )
    assert response.ok

    json = response.json()
    assert len(json['result']['points']) == 2, json

    ids = [x['id'] for x in json['result']['points']]
    assert 7 in ids
    assert 8 in ids

    response = request_with_validation(
        api='/collections/{collection_name}/points/query',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "limit": 20,
            "filter": {
                "must": [
                    {
                        "has_vector": "text"
                    }
                ]
            }
        }
    )
    assert response.ok

    json = response.json()
    assert len(json['result']['points']) == 6, json

    ids = [x['id'] for x in json['result']['points']]
    assert 7 not in ids
    assert 8 not in ids
