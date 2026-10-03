"""
Test that vector components the storage datatype cannot hold as a finite value
are rejected on upsert instead of being stored and read back as JSON `null`.

Write-side regression for https://github.com/qdrant/qdrant/issues/10350
"""

import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(collection_name):
    drop_collection(collection_name=collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 4,
                "distance": "Euclid",
                "datatype": "float16",
            },
        }
    )
    assert response.ok

    yield
    drop_collection(collection_name=collection_name)


@pytest.mark.parametrize("wait", ["true", "false"])
def test_upsert_out_of_range_float16_component_rejected(collection_name, wait):
    """A component beyond the finite f16 range is rejected with 4xx, sync and async."""
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': wait},
        body={
            "points": [
                {"id": 2, "vector": [70000.0, -70000.0, 0.5, 1.0]}
            ]
        }
    )
    assert response.status_code == 400
    assert "float16" in response.json()["status"]["error"]


def test_upsert_exact_float16_boundary_accepted(collection_name):
    """±65504 is the largest finite f16 magnitude and stays writable."""
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {"id": 3, "vector": [65504.0, -65504.0, 0.5, 1.0]}
            ]
        }
    )
    assert response.ok

    readback = request_with_validation(
        api='/collections/{collection_name}/points/{point_id}',
        method="GET",
        path_params={'collection_name': collection_name, 'point_id': 3},
    )
    assert readback.ok
    assert readback.json()["result"]["vector"] == [65504.0, -65504.0, 0.5, 1.0]
