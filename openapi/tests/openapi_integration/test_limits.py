import os
import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.fixtures import on_disk_vectors, on_disk_payload
from .helpers.helpers import request_with_validation

collection_name = 'test_limits'


@pytest.fixture(autouse=True)
def setup(on_disk_vectors):
    yield
    drop_collection(collection_name=collection_name)


# Tests vulnerability related limits, see: <https://github.com/qdrant/qdrant/pull/2544>
def test_vector_dimension_limit():
    dim_max = 65536

    drop_collection(collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": dim_max,
                "distance": "Dot",
            },
        }
    )
    assert response.ok

    drop_collection(collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": dim_max + 1,
                "distance": "Dot",
            },
        }
    )
    assert not response.ok
    error = response.json()['status']['error']
    assert error == f"Validation error in JSON body: [vectors.size: value {dim_max + 1} invalid, must be from 1 to {dim_max}]"

    drop_collection(collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 1,
                "distance": "Dot",
            },
        }
    )
    assert response.ok

    drop_collection(collection_name)
