import pytest
import requests

from .helpers.collection_setup import drop_collection
from .helpers.helpers import qdrant_host_headers, request_with_validation
from .helpers.settings import QDRANT_HOST

BODY_SENTINEL = "do-not-echo-this-request-body"


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
                "distance": "Cosine",
            },
        },
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": [0.1, 0.2, 0.3, 0.4],
                    "payload": {"group": "A"},
                }
            ]
        },
    )
    assert response.ok

    yield
    drop_collection(collection_name=collection_name)


# These requests intentionally bypass OpenAPI request validation so malformed field
# values reach Qdrant's server-side serde diagnostics.
@pytest.mark.parametrize(
    ("path", "body", "field"),
    [
        (
            "/points/search",
            {"vector": [0.1, 0.2, 0.3, 0.4], "limit": -1},
            "limit",
        ),
        (
            "/points/search/groups",
            {
                "vector": [0.1, 0.2, 0.3, 0.4],
                "group_by": "group",
                "group_size": -1,
                "limit": 5,
            },
            "group_size",
        ),
        (
            "/points/query",
            {"query": {"nearest": [0.1, 0.2, 0.3, 0.4]}, "limit": -1},
            "limit",
        ),
        (
            "/points/scroll",
            {"limit": -1},
            "limit",
        ),
        (
            "/points/scroll",
            {"limit": {"nested": BODY_SENTINEL}},
            "limit",
        ),
        (
            "/points/recommend",
            {"positive": [1], "limit": 5, "strategy": 123},
            "strategy",
        ),
        (
            "/points/search",
            {
                "vector": [0.1, 0.2, 0.3, 0.4],
                "limit": 5,
                "score_threshold": BODY_SENTINEL,
            },
            "score_threshold",
        ),
    ],
)
def test_serde_errors_name_invalid_fields(collection_name, path, body, field):
    body = {**body, "request_secret": BODY_SENTINEL}
    response = requests.post(
        f"{QDRANT_HOST}/collections/{collection_name}{path}",
        json=body,
        headers=qdrant_host_headers(),
    )

    assert response.status_code == 400
    error = response.json()["status"]["error"]
    assert f"{field}:" in error
    assert "usize" not in error
    assert "u32" not in error
    assert "f32" not in error
    assert BODY_SENTINEL not in error


@pytest.mark.parametrize(
    ("api", "body"),
    [
        (
            '/collections/{collection_name}/points/search',
            {
                "vector": [0.1, 0.2, 0.3, 0.4],
                "limit": 1,
                "score_threshold": 0.0,
            },
        ),
        (
            '/collections/{collection_name}/points/search/groups',
            {
                "vector": [0.1, 0.2, 0.3, 0.4],
                "group_by": "group",
                "group_size": 1,
                "limit": 1,
                "score_threshold": 0.0,
            },
        ),
        (
            '/collections/{collection_name}/points/query',
            {
                "query": {"nearest": [0.1, 0.2, 0.3, 0.4]},
                "limit": 1,
                "score_threshold": 0.0,
            },
        ),
        (
            '/collections/{collection_name}/points/scroll',
            {"limit": 1},
        ),
        (
            '/collections/{collection_name}/points/recommend',
            {
                "positive": [1],
                "strategy": "average_vector",
                "limit": 1,
                "offset": 0,
                "score_threshold": 0.0,
            },
        ),
    ],
)
def test_valid_serde_fields_still_work(collection_name, api, body):
    response = request_with_validation(
        api=api,
        method="POST",
        path_params={'collection_name': collection_name},
        body=body,
    )

    assert response.ok
