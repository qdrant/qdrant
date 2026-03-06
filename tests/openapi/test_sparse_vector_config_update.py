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
                "sparse-text": {
                    "index": {
                        "on_disk": False,
                    }
                },
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


def test_sparse_vector_config_update(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )

    assert response.ok
    result = response.json()["result"]
    config = result["config"]
    assert not config["params"]["sparse_vectors"]["sparse-text"]["index"]["on_disk"]

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': collection_name},
        body={
            "sparse_vectors": {
                "sparse-text": {
                    "index": {
                        "on_disk": True,
                    }
                },
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
    result = response.json()["result"]
    config = result["config"]
    assert config["params"]["sparse_vectors"]["sparse-text"]["index"]["on_disk"]
