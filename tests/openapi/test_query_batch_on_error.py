import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

MISSING_ID = 999999


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def query_batch(collection_name, searches, **params):
    return request_with_validation(
        api="/collections/{collection_name}/points/query/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={"searches": searches, **params},
    )


SEARCHES = [
    {"query": {"recommend": {"positive": [1]}}, "limit": 3},
    {"query": {"recommend": {"positive": [MISSING_ID]}}, "limit": 3},
    {"query": [0.2, 0.1, 0.9, 0.7], "limit": 3},
    {
        "prefetch": [{"query": MISSING_ID, "limit": 10}],
        "query": [0.2, 0.1, 0.9, 0.7],
        "limit": 3,
    },
]


@pytest.mark.parametrize("params", [{}, {"on_error": "fail_batch"}])
def test_query_batch_fails_whole_batch(collection_name, params):
    response = query_batch(collection_name, SEARCHES, **params)
    assert response.status_code == 404, response.text
    assert response.json()["status"]["error"] == f"Not found: No point with id {MISSING_ID} found"


def test_query_batch_fail_query(collection_name):
    response = query_batch(collection_name, SEARCHES, on_error="fail_query")
    assert response.ok, response.text
    assert response.json()["status"] == "ok"

    result = response.json()["result"]
    assert len(result) == len(SEARCHES)

    # Successful queries return the same points as when sent alone, and no error
    for idx in [0, 2]:
        single = query_batch(collection_name, [SEARCHES[idx]])
        assert single.ok, single.text
        assert "error" not in result[idx]
        assert result[idx]["points"] == single.json()["result"][0]["points"]
        assert len(result[idx]["points"]) == 3

    # Failed queries, in the root query and in a prefetch
    for idx in [1, 3]:
        assert result[idx]["points"] == []
        assert result[idx]["error"] == {
            "code": "point_not_found",
            "message": f"No point with id {MISSING_ID} found",
            "point_id": MISSING_ID,
        }


def test_query_batch_fail_query_without_errors(collection_name):
    response = query_batch(collection_name, [SEARCHES[0], SEARCHES[2]], on_error="fail_query")
    assert response.ok, response.text
    for item in response.json()["result"]:
        assert "error" not in item
        assert len(item["points"]) == 3


def test_query_batch_invalid_error_mode(collection_name):
    response = query_batch(collection_name, [SEARCHES[0]], on_error="ignore")
    assert response.status_code == 400 or response.status_code == 422, response.text
