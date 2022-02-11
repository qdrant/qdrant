import pytest

from ..openapi_integration.helpers import request_with_validation
from ..openapi_integration.collection_setup import basic_collection_setup, drop_collection

collection_name = 'test_collection_payload'


@pytest.fixture(autouse=True)
def setup():
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_payload_operations():
    # create payload
    response = request_with_validation(
        api='/collections/{name}/points/payload',
        method="POST",
        path_params={'name': collection_name},
        query_params={'wait': 'true'},
        body={
            "payload": {"test_payload": "keyword"},
            "points": [6]
        }
    )
    assert response.ok

    # check payload
    response = request_with_validation(
        api='/collections/{name}/points/{id}',
        method="GET",
        path_params={'name': collection_name, 'id': 6},
    )
    assert response.ok
    assert len(response.json()['result']['payload']) == 1

    # clean payload by filter
    response = request_with_validation(
        api='/collections/{name}/points/payload/clear',
        method="POST",
        path_params={'name': collection_name},
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
        api='/collections/{name}/points/{id}',
        method="GET",
        path_params={'name': collection_name, 'id': 6},
    )
    assert response.ok
    assert len(response.json()['result']['payload']) == 0
