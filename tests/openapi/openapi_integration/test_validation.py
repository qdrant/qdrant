import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection'


@pytest.fixture(autouse=True, scope="module")
def setup():
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_validation_collection_name():
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


def test_validation_body_param():
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
            "hnsw_config": {
                "ef_construct": 0,
            }
        }
    )
    assert not response.ok
    assert 'Validation error' in response.json()["status"]["error"]
    assert 'hnsw_config.ef_construct' in response.json()["status"]["error"]


def test_validation_query_param():
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
