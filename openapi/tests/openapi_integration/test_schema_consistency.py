import pytest

from openapi_integration.helpers.helpers import request_with_validation
from openapi_integration.helpers.collection_setup import basic_collection_setup, drop_collection

collection_name = 'test_collection_schema_consistency'


def test_uuid_operations():
    response = request_with_validation(
        api='/collections/{name}',
        method="DELETE",
        path_params={'name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{name}',
        method="PUT",
        path_params={'name': collection_name},
        body={
            "vector_size": 3,
            "distance": "Cosine"
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{name}/points',
        method="PUT",
        path_params={'name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "payload": {
                        "price": 11.8
                    },
                    "vector": [0.9, 0.1, 0.1]
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{name}/points',
        method="PUT",
        path_params={'name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "payload": {
                        "price": "cheap"
                    },
                    "vector": [0.9, 0.1, 0.1]
                }
            ]
        }
    )
    assert not response.ok

    response = request_with_validation(
        api='/collections/{name}',
        method="DELETE",
        path_params={'name': collection_name},
    )
    assert response.ok
