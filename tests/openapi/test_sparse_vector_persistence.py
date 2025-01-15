import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(collection_name):
    sparse_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def sparse_collection_setup(collection_name='test_collection'):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "sparse_vectors": {
                "text": {}
            },
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok


def test_sparse_vector_persisted_sorted(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {
                        "text": {"indices": [3, 2, 1], "values": [0.3, 0.2, 0.1]}
                    }
                },
                {
                    "id": 2,
                    "vector": {
                        "text": {"indices": [1, 3, 2], "values": [0.1, 0.3, 0.2]}
                    }
                },
                {
                    "id": 3,
                    "vector": {
                        "text": {"indices": [1, 2, 3], "values": [0.1, 0.2, 0.3]}
                    }
                },
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={"limit": 10, "with_vector": True}
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 3
    results = response.json()['result']['points']

    for i in range(3):
        assert results[i]['id'] == i + 1
        assert results[i]['vector']['text']['indices'] == [1, 2, 3]  # sorted by indices
        assert results[i]['vector']['text']['values'] == [0.1, 0.2, 0.3]  # aligned to respective indices


def test_sparse_dense_vector_naming_validations(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "image": {
                    "size": 2,
                    "distance": "Euclid"
                }
            },
            "sparse_vectors": {
                "image": {}
            }
        }
    )
    assert not response.ok
    assert 'Dense and sparse vector names must be unique - duplicate found with \'image\'' in response.json()["status"]["error"]

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 2,
                "distance": "Euclid"
            },
            "sparse_vectors": {
                "": {}
            }
        }
    )
    assert not response.ok
    assert 'Dense and sparse vector names must be unique - duplicate found with \'\'' in response.json()["status"]["error"]