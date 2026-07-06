import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


QUERY_VECTOR = [0.5, -1.0, 2.0, 0.0, 3.0]
POINT_VECTOR = [1.0, 1.0, -0.5, 4.0, 2.0]
# Dot-product terms: [0.5, -1.0, -1.0, 0.0, 6.0]
TOP_DIM_BY_ABS = "4"
TOP_TERM = 6.0


@pytest.fixture(autouse=True, scope="module")
def setup(collection_name):
    drop_collection(collection_name=collection_name)

    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={
            "vectors": {
                "size": 5,
                "distance": "Dot",
            },
        },
    )
    assert response.ok

    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={
            "points": [
                {"id": 1, "vector": POINT_VECTOR},
                {"id": 2, "vector": [0.0, 0.0, 0.0, 0.0, 0.0]},
            ],
        },
    )
    assert response.ok
    yield
    drop_collection(collection_name=collection_name)


def test_dims_explained_absent_by_default(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {"nearest": QUERY_VECTOR},
            "limit": 1,
        },
    )
    assert response.ok
    point = response.json()["result"]["points"][0]
    assert "dims_explained" not in point


def test_dims_explained_top_contributions(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {"nearest": QUERY_VECTOR},
            "limit": 1,
            "with_dims_explained": {"top": 3},
        },
    )
    assert response.ok
    point = response.json()["result"]["points"][0]
    explained = point["dims_explained"]
    assert len(explained) == 3
    assert explained[TOP_DIM_BY_ABS] == TOP_TERM
    assert all(abs(explained[str(dim)]) <= TOP_TERM for dim in explained)


def test_dims_explained_rejected_for_non_nearest(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {"recommend": {"positive": [1]}},
            "limit": 1,
            "with_dims_explained": True,
        },
    )
    assert not response.ok
    assert "with_dims_explained is only supported for nearest queries" in response.json()["status"]["error"]


def test_focus_rescore_with_dims_explained(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "nearest": {
                    "nearest": QUERY_VECTOR,
                    "focus": {"dims": [0, 4]},
                },
            },
            "limit": 2,
            "with_dims_explained": True,
        },
    )
    assert response.ok
    points = response.json()["result"]["points"]
    assert len(points) >= 1
    explained = points[0]["dims_explained"]
    assert explained is not None
    # Only focus dimensions should appear
    assert set(explained.keys()).issubset({"0", "4"})
    # Partial dot score on dims 0 and 4: 0.5 + 6.0
    assert points[0]["score"] == pytest.approx(6.5)
