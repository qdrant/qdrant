import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import basic_collection_setup, drop_collection

collection_name = 'test_collection_payload'


@pytest.fixture(autouse=True)
def setup():
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_payload_operations():
    # create payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "payload": {"test_payload": "keyword"},
            "points": [6]
        }
    )
    assert response.ok

    # check payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 6},
    )
    assert response.ok
    assert len(response.json()['result']['payload']) == 1

    # clean payload by filter
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload/clear',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "filter": {
                "must": [
                    {"has_id": [6]}
                ]
            }
        }
    )
    assert response.ok

    # check payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 6},
    )
    assert response.ok
    assert len(response.json()['result']['payload']) == 0

    # create payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "payload": {"test_payload": "keyword"},
            "points": [6]
        }
    )
    assert response.ok

    # delete payload by id
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "keys": ["test_payload"],
            "points": [6]
        }
    )
    assert response.ok

    # check payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 6},
    )
    assert response.ok
    assert len(response.json()['result']['payload']) == 0

    #
    # test PUT vs POST of the payload - set vs overwrite
    #
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "payload": {"key1": "aaa", "key2": "bbb"},
            "points": [6]
        }
    )
    assert response.ok

    # check payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 6},
    )
    assert response.ok
    assert len(response.json()['result']['payload']) == 2

    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "payload": {"key1": "ccc"},
            "points": [6]
        }
    )
    assert response.ok

    # check payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 6},
    )
    assert response.ok
    assert len(response.json()['result']['payload']) == 2
    assert response.json()['result']['payload']["key1"] == "ccc"
    assert response.json()['result']['payload']["key2"] == "bbb"

    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "payload": {"key2": "eee"},
            "points": [6]
        }
    )
    assert response.ok

    # check payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 6},
    )
    assert response.ok
    assert len(response.json()['result']['payload']) == 1
    assert response.json()['result']['payload']["key2"] == "eee"

