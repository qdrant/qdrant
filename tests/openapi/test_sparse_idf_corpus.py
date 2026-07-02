import math

import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

SPARSE_VECTOR_NAME = "text"


@pytest.fixture(autouse=True)
def setup(collection_name):
    corpus_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


# Deterministic layout, mirrored by the Rust integration tests:
#
# | point | tenant | sparse dims |
# |-------|--------|-------------|
# | 0     | a      | 0, 1        |
# | 1     | a      | 0           |
# | 2     | b      | 0, 1, 2     |
# | 3     | b      | 1           |
POINTS = [
    ("a", [0, 1]),
    ("a", [0]),
    ("b", [0, 1, 2]),
    ("b", [1]),
]

QUERY = {"indices": [0, 1, 2], "values": [1.0, 1.0, 1.0]}


def expected_idf(n, df):
    """Advanced IDF formula used by the engine for the `idf` modifier."""
    return math.log((n - df + 0.5) / (df + 0.5) + 1.0)


def tenant_filter(value):
    return {"must": [{"key": "tenant", "match": {"value": value}}]}


def corpus_collection_setup(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            # Dense vector without IDF, to check `idf` param rejection
            "vectors": {
                "dense": {
                    "size": 2,
                    "distance": "Dot",
                }
            },
            "sparse_vectors": {
                SPARSE_VECTOR_NAME: {
                    "modifier": "idf",
                }
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
                    "id": idx,
                    "vector": {
                        "dense": [1.0, 0.0],
                        SPARSE_VECTOR_NAME: {
                            "indices": dims,
                            "values": [1.0] * len(dims),
                        },
                    },
                    "payload": {"tenant": tenant},
                }
                for idx, (tenant, dims) in enumerate(POINTS)
            ]
        },
    )
    assert response.ok


def search(collection_name, filter=None, params=None, limit=10):
    body = {
        "vector": {
            "name": SPARSE_VECTOR_NAME,
            "vector": QUERY,
        },
        "limit": limit,
    }
    if filter is not None:
        body["filter"] = filter
    if params is not None:
        body["params"] = params

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body=body,
    )
    assert response.ok, response.json()
    return {point['id']: point['score'] for point in response.json()['result']}


def test_explicit_global_matches_default(collection_name):
    default_scores = search(collection_name)
    explicit_global = search(collection_name, params={"idf": "global"})
    assert default_scores == explicit_global

    # Global statistics: N = 4, df = [3, 3, 1]
    idf = [expected_idf(4, 3), expected_idf(4, 3), expected_idf(4, 1)]
    assert default_scores[0] == pytest.approx(idf[0] + idf[1])
    assert default_scores[1] == pytest.approx(idf[0])
    assert default_scores[2] == pytest.approx(idf[0] + idf[1] + idf[2])
    assert default_scores[3] == pytest.approx(idf[1])


def test_corpus_decoupled_from_retrieval_filter(collection_name):
    # IDF over tenant a only (N = 2, df = [2, 1, 0]), retrieval unfiltered:
    # every point is scored against tenant a's term statistics.
    scores = search(
        collection_name,
        params={"idf": {"corpus": tenant_filter("a")}},
    )
    idf = [expected_idf(2, 2), expected_idf(2, 1), expected_idf(2, 0)]
    assert scores[0] == pytest.approx(idf[0] + idf[1])
    assert scores[1] == pytest.approx(idf[0])
    assert scores[2] == pytest.approx(idf[0] + idf[1] + idf[2])
    assert scores[3] == pytest.approx(idf[1])


def test_filter_tightening_does_not_move_scores(collection_name):
    # With a fixed corpus, adding or tightening the retrieval filter narrows
    # the result set but must not change any returned score.
    params = {"idf": {"corpus": tenant_filter("b")}}

    broad = search(collection_name, params=params)
    narrow = search(collection_name, filter=tenant_filter("b"), params=params)

    assert set(narrow) == {2, 3}
    for point_id, score in narrow.items():
        assert score == broad[point_id]


def test_empty_corpus_never_falls_back_to_global(collection_name):
    # A corpus matching nothing yields degenerate but corpus-scoped scores:
    # idf(0, 0) = ln(2) per term, never another population's statistics.
    scores = search(
        collection_name,
        params={"idf": {"corpus": tenant_filter("missing")}},
    )
    ln2 = expected_idf(0, 0)
    assert scores[0] == pytest.approx(2 * ln2)
    assert scores[1] == pytest.approx(ln2)
    assert scores[2] == pytest.approx(3 * ln2)
    assert scores[3] == pytest.approx(ln2)


def test_query_api_supports_idf_corpus(collection_name):
    # The universal query API carries the same `params.idf`.
    response = request_with_validation(
        api='/collections/{collection_name}/points/query',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "query": QUERY,
            "using": SPARSE_VECTOR_NAME,
            "params": {"idf": {"corpus": tenant_filter("a")}},
            "limit": 10,
        },
    )
    assert response.ok, response.json()

    scores = {point['id']: point['score'] for point in response.json()['result']['points']}
    idf = [expected_idf(2, 2), expected_idf(2, 1), expected_idf(2, 0)]
    assert scores[2] == pytest.approx(idf[0] + idf[1] + idf[2])


def search_expecting_error(collection_name, vector, params, expected_status):
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": vector,
            "params": params,
            "limit": 10,
        },
    )
    assert not response.ok, response.json()
    assert response.status_code == expected_status
    return response.json()['status']['error']


def test_corpus_grammar_is_restricted(collection_name):
    sparse_query = {"name": SPARSE_VECTOR_NAME, "vector": QUERY}

    # Only `must` clauses are allowed.
    error = search_expecting_error(
        collection_name,
        sparse_query,
        {"idf": {"corpus": {"should": [{"key": "tenant", "match": {"value": "a"}}]}}},
        expected_status=422,
    )
    assert "must" in error

    # Only `match` conditions are allowed.
    error = search_expecting_error(
        collection_name,
        sparse_query,
        {"idf": {"corpus": {"must": [{"key": "dim", "range": {"gte": 0}}]}}},
        expected_status=422,
    )
    assert "match" in error

    # An empty corpus filter is rejected.
    error = search_expecting_error(
        collection_name,
        sparse_query,
        {"idf": {"corpus": {"must": []}}},
        expected_status=422,
    )
    assert "at least one condition" in error


def test_idf_params_require_idf_modifier(collection_name):
    # On a dense vector the `idf` param cannot apply — it is rejected rather
    # than silently ignored.
    error = search_expecting_error(
        collection_name,
        {"name": "dense", "vector": [1.0, 0.0]},
        {"idf": {"corpus": tenant_filter("a")}},
        expected_status=400,
    )
    assert "idf" in error
