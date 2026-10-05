import math

import pytest
import requests

from .helpers.collection_setup import drop_collection
from .helpers.helpers import qdrant_host_headers, request_with_validation
from .helpers.settings import QDRANT_HOST

# A text index that scores, and one that does not.
FIELD = "body"
UNSCORED_FIELD = "plain"
POINTS = 35
K1 = 1.2
B = 0.75


def text_of(i):
    """One document per pair of `alpha` (1 to 5) and `gamma` (0 to 6) counts, so
    no two documents score the same for a query of both terms. A query of one
    of them does tie."""
    return " ".join(["alpha"] * (i % 5 + 1) + ["gamma"] * (i // 5))


def bm25_reference(query_terms, k1=K1, b=B, ids=range(POINTS)):
    """BM25 by definition over the whole corpus, for the points in `ids`."""
    documents = [text_of(i).split(" ") for i in range(POINTS)]
    n = len(documents)
    avgdl = sum(len(d) for d in documents) / n
    scores = {}
    for i in ids:
        document = documents[i]
        score = 0.0
        for term in query_terms:
            tf = document.count(term)
            if tf == 0:
                continue
            df = sum(1 for d in documents if term in d)
            idf = max(0.0, math.log((n - df + 0.5) / (df + 0.5) + 1))
            score += idf * tf * (k1 + 1) / (tf + k1 * (1 - b + b * len(document) / avgdl))
        if score > 0:
            scores[i] = score
    return scores


def create_text_collection(collection_name, sparse_vectors=None):
    drop_collection(collection_name=collection_name)
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={"sparse_vectors": sparse_vectors or {}},
    )
    assert response.ok, response.text

    for field, field_schema in [
        (FIELD, {"type": "text", "tokenizer": "whitespace", "scoring": True}),
        (UNSCORED_FIELD, {"type": "text", "tokenizer": "whitespace"}),
    ]:
        response = request_with_validation(
            api='/collections/{collection_name}/index',
            method="PUT",
            path_params={'collection_name': collection_name},
            query_params={'wait': 'true'},
            body={"field_name": field, "field_schema": field_schema},
        )
        assert response.ok, response.text

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": i,
                    "vector": {},
                    "payload": {FIELD: text_of(i), UNSCORED_FIELD: text_of(i), "group": i % 3},
                }
                for i in range(POINTS)
            ]
        },
    )
    assert response.ok, response.text


@pytest.fixture(autouse=True)
def setup(collection_name):
    create_text_collection(collection_name)
    yield
    drop_collection(collection_name=collection_name)


def text(query, **params):
    """A text query: the string form, or the struct form when given parameters."""
    if not params:
        return {"text": query}
    return {"text": {"query": query, **params}}


def query(collection_name, body):
    return request_with_validation(
        api='/collections/{collection_name}/points/query',
        method="POST",
        path_params={'collection_name': collection_name},
        body=body,
    )


def assert_scores(points, reference):
    # By id and score rather than by position: some queries tie, and ties
    # have no defined order.
    assert {point["id"] for point in points} == set(reference)
    for point in points:
        expected = reference[point["id"]]
        assert math.isclose(point["score"], expected, rel_tol=1e-4), (point, expected)
    scores = [point["score"] for point in points]
    assert scores == sorted(scores, reverse=True), "ranked best first"


def assert_refused(response, expected):
    assert response.status_code == 400, response.text
    assert expected in response.json()["status"]["error"]


def top(reference, limit):
    return dict(sorted(reference.items(), key=lambda item: -item[1])[:limit])


def test_text_index_scores_by_definition(collection_name):
    # Mixed case on purpose: the index lowercases, and tokenizes the query itself.
    response = query(collection_name, {
        "query": text("Alpha GAMMA"),
        "using": FIELD,
        "limit": POINTS,
    })
    assert response.ok, response.text
    points = response.json()["result"]["points"]
    assert len(points) == POINTS
    assert_scores(points, bm25_reference(["alpha", "gamma"]))


def test_any_term_matches(collection_name):
    # OR semantics: a point scores with any of the terms, and a term nothing
    # holds changes nothing.
    response = query(collection_name, {
        "query": text("gamma absent"),
        "using": FIELD,
        "limit": POINTS,
    })
    assert response.ok, response.text
    assert_scores(response.json()["result"]["points"], bm25_reference(["gamma"]))


def test_struct_form_takes_k_and_b(collection_name):
    response = query(collection_name, {
        "query": text("alpha gamma", k=2.0, b=0.0),
        "using": FIELD,
        "limit": POINTS,
    })
    assert response.ok, response.text
    assert_scores(
        response.json()["result"]["points"],
        bm25_reference(["alpha", "gamma"], k1=2.0, b=0.0),
    )


def test_struct_form_defaults_match_the_string_form(collection_name):
    responses = [
        query(collection_name, {"query": body, "using": FIELD, "limit": POINTS})
        for body in [text("alpha gamma"), {"text": {"query": "alpha gamma"}}]
    ]
    assert all(response.ok for response in responses)
    string_form, struct_form = (response.json()["result"]["points"] for response in responses)
    assert string_form == struct_form


def test_filter_applies(collection_name):
    ids = list(range(10))
    response = query(collection_name, {
        "query": text("alpha gamma"),
        "using": FIELD,
        "filter": {"must": [{"has_id": ids}]},
        "limit": POINTS,
    })
    assert response.ok, response.text
    # Statistics still cover the whole corpus, only the candidates are filtered.
    assert_scores(response.json()["result"]["points"], bm25_reference(["alpha", "gamma"], ids=ids))


def test_text_index_in_a_prefetch(collection_name):
    response = query(collection_name, {
        "prefetch": [
            {"query": text("alpha"), "using": FIELD, "limit": 5},
            {"query": text("gamma"), "using": FIELD, "limit": 5},
        ],
        "query": {"fusion": "rrf"},
        "limit": 10,
    })
    assert response.ok, response.text
    expected = set(top(bm25_reference(["alpha"]), 5)) | set(top(bm25_reference(["gamma"]), 5))
    assert {point["id"] for point in response.json()["result"]["points"]} == expected


def test_text_index_in_query_groups(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}/points/query/groups',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "query": text("gamma"),
            "using": FIELD,
            "group_by": "group",
            "group_size": 1,
            "limit": 3,
        },
    )
    assert response.ok, response.text
    groups = response.json()["result"]["groups"]
    reference = bm25_reference(["gamma"])
    assert len(groups) == 3
    for group in groups:
        best = max((i for i in reference if i % 3 == group["id"]), key=lambda i: reference[i])
        assert group["hits"][0]["id"] == best


def test_text_query_needs_using(collection_name):
    assert_refused(query(collection_name, {"query": text("alpha")}), "needs `using`")


def test_field_without_text_index_is_refused(collection_name):
    assert_refused(
        query(collection_name, {"query": text("alpha"), "using": "group"}),
        "which has none",
    )


def test_unscored_text_index_is_refused(collection_name):
    assert_refused(
        query(collection_name, {"query": text("alpha"), "using": UNSCORED_FIELD}),
        "does not score",
    )


@pytest.mark.parametrize("params", [{"b": 1.5}, {"k": -1.0}])
def test_out_of_range_parameters_are_refused(collection_name, params):
    # Outside the schema, so sent without client-side validation.
    response = requests.post(
        f"{QDRANT_HOST}/collections/{collection_name}/points/query",
        json={"query": text("alpha", **params), "using": FIELD},
        headers=qdrant_host_headers(),
    )
    assert response.status_code == 422, response.text


def test_a_vector_of_the_same_name_does_not_interfere(collection_name):
    # A text query resolves `using` against payload fields only.
    shared = f"{collection_name}_shared"
    create_text_collection(shared, sparse_vectors={FIELD: {"modifier": "idf"}})
    try:
        response = query(shared, {"query": text("alpha"), "using": FIELD, "limit": POINTS})
        assert response.ok, response.text
        assert len(response.json()["result"]["points"]) == POINTS
    finally:
        drop_collection(collection_name=shared)


def test_bm25_document_keeps_the_sparse_route(collection_name):
    # A `qdrant/bm25` document still embeds for a sparse vector: naming a
    # payload field with it is a missing vector, as before text queries.
    response = query(collection_name, {
        "query": {"text": "alpha", "model": "qdrant/bm25"},
        "using": FIELD,
    })
    assert response.status_code == 400, response.text
    assert "Not existing vector name" in response.json()["status"]["error"]
