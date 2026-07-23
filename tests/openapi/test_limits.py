import pytest
import requests

from .helpers.collection_setup import drop_collection
from .helpers.helpers import qdrant_host_headers, request_with_validation
from .helpers.settings import QDRANT_HOST


@pytest.fixture(autouse=True)
def setup(collection_name):
    yield
    drop_collection(collection_name=collection_name)


# Tests vulnerability related limits, see: <https://github.com/qdrant/qdrant/pull/2544>
def test_vector_dimension_limit(collection_name):
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

    # `size` now carries a client-side OpenAPI maximum, so bypass
    # request_with_validation (which would short-circuit on it) to confirm the
    # server-side Validate derive is what rejects an over-limit dimension.
    response = requests.put(
        f"{QDRANT_HOST}/collections/{collection_name}",
        json={
            "vectors": {
                "size": dim_max + 1,
                "distance": "Dot",
            },
        },
        headers=qdrant_host_headers(),
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
