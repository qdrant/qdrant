import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True, scope="module")
def setup(collection_name):
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_validation_collection_name(collection_name):
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


def test_validation_body_param(collection_name):
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


def test_validation_query_param(collection_name):
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


# Regression: two sibling validation errors used to panic
# `common::validation::validate_iter` (validator's internal `add_nested` panics
# on a second insert under the same key), surfacing as a dropped connection
# instead of a 4xx. Length-mismatched sparse vectors with non-empty indices
# fail validation without short-circuiting `VectorStruct::is_empty`.

_INVALID_SPARSE = {"indices": [0, 1], "values": [0.0]}


def test_validation_iter_named_vectors(collection_name):
    # Hits VectorStruct::Named -> validate_iter (lib/api/src/rest/schema.rs).
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {"a": _INVALID_SPARSE, "b": _INVALID_SPARSE},
                }
            ],
        },
    )
    assert not response.ok
    assert 'Validation error' in response.json()["status"]["error"]


def test_validation_iter_batch_named_vectors(collection_name):
    # Hits BatchVectorStruct::Named -> validate_iter (lib/api/src/rest/validate.rs).
    response = request_with_validation(
        api='/collections/{collection_name}/points/batch',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "operations": [
                {
                    "upsert": {
                        "batch": {
                            "ids": [1, 2],
                            "vectors": {"a": [_INVALID_SPARSE, _INVALID_SPARSE]},
                        }
                    }
                }
            ]
        },
    )
    assert not response.ok
    assert 'Validation error' in response.json()["status"]["error"]


# Regression for https://github.com/qdrant/qdrant/issues/9045
#
# Upserting an empty vector `[]` is rejected on the synchronous (`wait=true`)
# path but silently accepted on the asynchronous path: the response is HTTP 200
# `acknowledged`, the point is later discarded, and the zero-length vector can
# reach internal code paths that assert on non-zero length (see #7967).
@pytest.mark.parametrize("wait", ["true", "false"])
def test_validation_empty_vector_upsert(collection_name, wait):
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': wait},
        body={"points": [{"id": 1000, "vector": []}]},
    )
    assert not response.ok, (
        f"empty vector accepted with wait={wait}: "
        f"status={response.status_code}, body={response.text}"
    )


@pytest.mark.parametrize("wait", ["true", "false"])
def test_validation_empty_vector_batch_upsert(collection_name, wait):
    response = request_with_validation(
        api='/collections/{collection_name}/points/batch',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': wait},
        body={
            "operations": [
                {"upsert": {"points": [{"id": 1001, "vector": []}]}},
            ],
        },
    )
    assert not response.ok, (
        f"empty vector accepted in batch upsert with wait={wait}: "
        f"status={response.status_code}, body={response.text}"
    )
