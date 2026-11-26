import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

FIELD = "body"

# The same words, as an identifier or in a comment.
DOCUMENTS = {
    1: "def getUserName(self): return self.name",
    2: "# get the name of a user",
    3: "def get_user_name(self): return self.name",
}


@pytest.fixture(autouse=True)
def setup(collection_name):
    drop_collection(collection_name=collection_name)
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={"sparse_vectors": {}},
    )
    assert response.ok, response.text

    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"field_name": FIELD,
              "field_schema": {"type": "text", "tokenizer": "code", "scoring": True}},
    )
    assert response.ok, response.text

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"points": [{"id": i, "vector": {}, "payload": {FIELD: text}}
                         for i, text in DOCUMENTS.items()]},
    )
    assert response.ok, response.text
    yield
    drop_collection(collection_name=collection_name)


def rank(collection_name, query):
    response = request_with_validation(
        api='/collections/{collection_name}/points/query',
        method="POST",
        path_params={'collection_name': collection_name},
        body={"query": {"text": query}, "using": FIELD, "limit": 10},
    )
    assert response.ok, response.text
    return [(point["id"], point["score"]) for point in response.json()["result"]["points"]]


def test_exact_identifier_ranks_first(collection_name):
    # Only the identifiers have the joined token `getusername`
    for query in ["getUserName", "get_user_name", "getusername"]:
        ranked = rank(collection_name, query)
        assert {point_id for point_id, _ in ranked[:2]} == {1, 3}, (query, ranked)
        assert all(score < ranked[1][1] for _, score in ranked[2:]), (query, ranked)


def test_filter_needs_subwords_only(collection_name):
    # The comment has the subwords but not the joined token
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={"filter": {"must": [{"key": FIELD, "match": {"text": "getUserName"}}]},
              "limit": 10},
    )
    assert response.ok, response.text
    assert {point["id"] for point in response.json()["result"]["points"]} == set(DOCUMENTS)
