import pytest
import requests

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import qdrant_host_headers, request_with_validation
from .helpers.settings import QDRANT_HOST


@pytest.fixture(autouse=True, scope="module")
def setup(collection_name):
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def query(collection_name, body):
    return request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body=body,
    )


@pytest.mark.parametrize(
    "document",
    [
        "city",
        ["city", "count"],
        {"template": "{city} ({count})"},
    ],
)
def test_rerank_is_parsed_but_not_supported_yet(collection_name, document):
    response = query(
        collection_name,
        {
            "prefetch": {"query": [0.1, 0.2, 0.3, 0.4], "limit": 10},
            "query": {
                "rerank": {
                    "model": "qwen/qwen3-reranker-0.6b",
                    "query": "a city",
                    "document": document,
                    "options": {"instruction": "Prefer large cities"},
                }
            },
            "limit": 3,
        },
    )
    assert response.status_code == 400, response.text
    assert "rerank query is not supported yet" in response.json()["status"]["error"]


@pytest.mark.parametrize(
    "rerank",
    [
        {"model": "", "query": "a city", "document": "city"},
        {"model": "qwen/qwen3-reranker-0.6b", "query": "a city", "document": []},
        {"model": "qwen/qwen3-reranker-0.6b", "query": "a city", "document": {"template": ""}},
    ],
)
def test_rerank_rejects_invalid_input(collection_name, rerank):
    # Sent without schema validation, the server has to reject it
    response = requests.post(
        f"{QDRANT_HOST}/collections/{collection_name}/points/query",
        json={
            "prefetch": {"query": [0.1, 0.2, 0.3, 0.4], "limit": 10},
            "query": {"rerank": rerank},
            "limit": 3,
        },
        headers=qdrant_host_headers(),
    )
    assert response.status_code == 422, response.text
