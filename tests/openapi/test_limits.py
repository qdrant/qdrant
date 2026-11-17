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

    # An oversized dimension must be rejected by the documented schema
    # (see #9942): request_with_validation raises before sending.
    with pytest.raises(jsonschema.exceptions.ValidationError):
        request_with_validation(
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
