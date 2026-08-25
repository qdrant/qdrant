import jsonschema
import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


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

    # An oversized dimension must be rejected. Since #9942 the OpenAPI schema
    # documents the enforced 1..=dim_max bound, so request_with_validation may
    # reject the payload client-side before it ever reaches the server; both
    # layers of defense are accepted here.
    try:
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
    except jsonschema.exceptions.ValidationError:
        # Rejected by the documented schema before reaching the server.
        pass
    else:
        assert response.status_code == 422
        error = response.json()["status"]["error"]
        assert error == (
            f"Validation error in JSON body: "
            f"[vectors.size: value {dim_max + 1} invalid, must be from 1 to {dim_max}]"
        )

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
