import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation
from .test_sparse_idf_corpus import (
    SPARSE_VECTOR_NAME,
    corpus_collection_setup,
    expected_idf,
    search,
    tenant_filter,
)

# Unlike a sparse search query, an IDF query carries no values: only the term
# indices participate in the statistics.
QUERY = {"indices": [0, 1, 2]}


@pytest.fixture(autouse=True)
def setup(collection_name):
    corpus_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def estimate(collection_name, query=QUERY, corpus=None, using=SPARSE_VECTOR_NAME):
    body = {
        "using": using,
        "query": query,
    }
    if corpus is not None:
        body["corpus"] = corpus

    response = request_with_validation(
        api='/collections/{collection_name}/points/idf',
        method="POST",
        path_params={'collection_name': collection_name},
        body=body,
    )
    assert response.ok, response.json()
    return response.json()['result']['idf']


def assert_estimate(idf, document_count, df_by_index):
    assert idf['document_count'] == document_count
    assert len(idf['terms']) == len(df_by_index)
    for term, (index, df) in zip(idf['terms'], df_by_index):
        assert term['index'] == index
        assert term['document_frequency'] == df
        assert term['idf'] == pytest.approx(expected_idf(document_count, df))


def test_estimate_global(collection_name):
    # Global statistics: N = 4, df = [3, 3, 1]
    idf = estimate(collection_name)
    assert_estimate(idf, 4, [(0, 3), (1, 3), (2, 1)])

    # The estimate reports exactly the statistics search scores with:
    # point 2 has all three dims, so its score is the sum of the IDF values.
    scores = search(collection_name)
    assert scores[2] == pytest.approx(sum(term['idf'] for term in idf['terms']))


def test_estimate_corpus(collection_name):
    # Corpus = tenant a: N = 2, df = [2, 1, 0]
    idf = estimate(collection_name, corpus=tenant_filter("a"))
    assert_estimate(idf, 2, [(0, 2), (1, 1), (2, 0)])

    # Corpus = tenant b: N = 2, df = [1, 2, 1]
    idf = estimate(collection_name, corpus=tenant_filter("b"))
    assert_estimate(idf, 2, [(0, 1), (1, 2), (2, 1)])

    # An empty corpus reports degenerate but corpus-scoped statistics —
    # never a fallback to global. idf(0, 0) = ln(2) for every term.
    idf = estimate(collection_name, corpus=tenant_filter("missing"))
    assert_estimate(idf, 0, [(0, 0), (1, 0), (2, 0)])


def test_estimate_corpus_accepts_any_filter(collection_name):
    # A range corpus over `idx < 2` selects points {0, 1} — the same
    # population as tenant a, so the same statistics: N = 2, df = [2, 1, 0].
    idf = estimate(
        collection_name,
        corpus={"must": [{"key": "idx", "range": {"lt": 2}}]},
    )
    assert_estimate(idf, 2, [(0, 2), (1, 1), (2, 0)])


def test_estimate_missing_term(collection_name):
    # A term missing from the collection reports a zero frequency.
    idf = estimate(collection_name, query={"indices": [2, 7]})
    assert_estimate(idf, 4, [(2, 1), (7, 0)])


def estimate_expecting_error(collection_name, body, expected_status):
    response = request_with_validation(
        api='/collections/{collection_name}/points/idf',
        method="POST",
        path_params={'collection_name': collection_name},
        body=body,
    )
    assert not response.ok, response.json()
    assert response.status_code == expected_status
    return response.json()['status']['error']


def test_estimate_requires_idf_modifier(collection_name):
    # IDF estimation on a vector without the IDF modifier is rejected, same
    # as the `idf` search param.
    for using in ["dense", "missing"]:
        error = estimate_expecting_error(
            collection_name,
            {"using": using, "query": QUERY},
            expected_status=400,
        )
        assert "idf" in error


def test_estimate_rejects_invalid_query(collection_name):
    # Duplicated term indices fail validation, same as in a sparse vector.
    estimate_expecting_error(
        collection_name,
        {"using": SPARSE_VECTOR_NAME, "query": {"indices": [0, 1, 0]}},
        expected_status=422,
    )
