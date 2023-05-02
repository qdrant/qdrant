import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection'


@pytest.fixture(autouse=True)
def setup():
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_validation():
    # Collection names are limited to 255 chars due to filesystem constraints
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={
            'collection_name': '''\
                extremelylongnameextremelylongnameextremelylongname\
                extremelylongnameextremelylongnameextremelylongname\
                extremelylongnameextremelylongnameextremelylongname\
                extremelylongnameextremelylongnameextremelylongname\
                extremelylongnameextremelylongnameextremelylongname\
                extremelylongnameextremelylongnameextremelylongname\
                ''',
            'id': 1,
        },
    )
    assert not response.ok
    assert 'Validation error' in response.json()["status"]["error"]

    # Illegal body parameters must trigger a validation error
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 4,
                "distance": "Dot"
            },
            "optimizers_config": {
                "memmap_threshold": 100,
                "indexing_threshold": 100,
            },
        }
    )
    assert not response.ok
    assert 'Validation error' in response.json()["status"]["error"]
    assert 'optimizers_config.memmap_threshold' in response.json()["status"]["error"]
    assert 'optimizers_config.indexing_threshold' in response.json()["status"]["error"]

    # Illegal URL parameters must trigger a validation error
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'timeout': 0},
        body={
            "vectors": {
                "size": 4,
                "distance": "Dot"
            },
        }
    )
    assert not response.ok
    assert 'Validation error' in response.json()["status"]["error"]
    assert 'timeout: value 0 invalid' in response.json()["status"]["error"]
