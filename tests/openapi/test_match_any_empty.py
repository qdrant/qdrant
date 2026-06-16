import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

COLLECTION_NAME = "test_match_any_empty"


@pytest.fixture(autouse=True, scope="module")
def setup():
    create_collection(COLLECTION_NAME)
    yield
    drop_collection(collection_name=COLLECTION_NAME)


def create_collection(collection_name):
    drop_collection(collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 2,
                "distance": "Dot",
            },
        }
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
                    "id": 1, "vector": [1.0, 0.0],
                    "payload": {
                        "n": 1,
                        "tag": "a",
                        "uid": "550e8400-e29b-41d4-a716-446655440001",
                    },
                },
                {
                    "id": 2, "vector": [0.0, 1.0],
                    "payload": {
                        "n": 2,
                        "tag": "b",
                        "uid": "550e8400-e29b-41d4-a716-446655440002",
                    },
                },
                {
                    "id": 3, "vector": [1.0, 1.0],
                    "payload": {
                        "n": 3,
                        "tag": "c",
                        "uid": "550e8400-e29b-41d4-a716-446655440003",
                    },
                },
            ]
        }
    )
    assert response.ok


def _scroll(collection_name, filter_body):
    return request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": filter_body,
            "limit": 10,
            "with_payload": True,
        }
    )


def _scroll_ids(collection_name, filter_body):
    response = _scroll(collection_name, filter_body)
    assert response.ok, response.json()
    return sorted(p['id'] for p in response.json()['result']['points'])


def _create_index(collection_name, field_name, field_schema):
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": field_name,
            "field_schema": field_schema,
        }
    )
    assert response.ok


def _set_strict_mode(collection_name, strict_mode_config):
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PATCH",
        path_params={"collection_name": collection_name},
        body={"strict_mode_config": strict_mode_config},
    )
    response.raise_for_status()


# ---------------------------------------------------------------------------
# 1. Empty any list — no index, no strict mode (baseline semantics)
#    `match: {"any": []}` means "match nothing", so the result must be empty.
# ---------------------------------------------------------------------------

def test_match_any_empty_integer_no_index():
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "n", "match": {"any": []}}]
    })
    assert ids == []


def test_match_any_empty_keyword_no_index():
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "tag", "match": {"any": []}}]
    })
    assert ids == []


def test_match_any_empty_uuid_no_index():
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "uid", "match": {"any": []}}]
    })
    assert ids == []


# ---------------------------------------------------------------------------
# 2. Non-empty any list with matching type — no index (baseline)
# ---------------------------------------------------------------------------

def test_match_any_integers_no_index():
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "n", "match": {"any": [1]}}]
    })
    assert ids == [1]


def test_match_any_strings_no_index():
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "tag", "match": {"any": ["a"]}}]
    })
    assert ids == [1]


# ---------------------------------------------------------------------------
# 3. Create indexes for all three field types.
# ---------------------------------------------------------------------------

def test_create_indexes():
    _create_index(COLLECTION_NAME, "n", "integer")
    _create_index(COLLECTION_NAME, "tag", "keyword")
    _create_index(COLLECTION_NAME, "uid", "uuid")


# ---------------------------------------------------------------------------
# 4. Empty any list — with index, no strict mode.
#    Semantics: still "match nothing".
# ---------------------------------------------------------------------------

def test_match_any_empty_integer_with_index():
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "n", "match": {"any": []}}]
    })
    assert ids == []


def test_match_any_empty_keyword_with_index():
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "tag", "match": {"any": []}}]
    })
    assert ids == []


def test_match_any_empty_uuid_with_index():
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "uid", "match": {"any": []}}]
    })
    assert ids == []


# ---------------------------------------------------------------------------
# 5. Non-empty any list with matching type — with index
# ---------------------------------------------------------------------------

def test_match_any_integers_with_index():
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "n", "match": {"any": [1, 2]}}]
    })
    assert ids == [1, 2]


def test_match_any_strings_with_index():
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "tag", "match": {"any": ["a", "b"]}}]
    })
    assert ids == [1, 2]


# ---------------------------------------------------------------------------
# 6. Strict mode reproduction (the actual reported bug).
#
#    With strict mode `unindexed_filtering_retrieve = False`, filtering on
#    `n` (which has an integer index) must be permitted regardless of
#    whether the `any` list is empty.
#
#    Before the fix, MatchAny(any=[]) on an integer-indexed field is
#    rejected with:
#       INVALID_ARGUMENT: Index required but not found for "n" of one of
#       the following types: [keyword, uuid]
#    because an empty list deserializes as AnyVariants::Strings(∅) and
#    strict-mode infers a keyword/uuid index requirement from the variant.
#
#    Expected: an empty `any` is a no-op (matches nothing) and should not
#    trip the strict-mode unindexed-field check on a field that does have
#    an index of any supported type.
# ---------------------------------------------------------------------------

def test_match_any_empty_with_strict_mode_integer_index():
    _set_strict_mode(COLLECTION_NAME, {
        "enabled": True,
        "unindexed_filtering_retrieve": False,
    })
    try:
        # Non-empty `any` on the integer-indexed field must succeed
        # (sanity check: integer index is recognized by strict mode).
        ok_resp = _scroll(COLLECTION_NAME, {
            "must": [{"key": "n", "match": {"any": [1]}}]
        })
        assert ok_resp.ok, ok_resp.json()

        # Empty `any` on the same integer-indexed field — currently fails.
        empty_resp = _scroll(COLLECTION_NAME, {
            "must": [{"key": "n", "match": {"any": []}}]
        })
        assert empty_resp.ok, (
            "MatchAny(any=[]) on an integer-indexed field was rejected by "
            f"strict mode: {empty_resp.json()}"
        )
        assert sorted(p['id'] for p in empty_resp.json()['result']['points']) == []
    finally:
        _set_strict_mode(COLLECTION_NAME, {"enabled": False})


def test_match_any_empty_with_strict_mode_uuid_index():
    _set_strict_mode(COLLECTION_NAME, {
        "enabled": True,
        "unindexed_filtering_retrieve": False,
    })
    try:
        # Empty `any` on the UUID-indexed field.
        # UUID is in the inferred set today, so this happens to pass even
        # before the fix — but we still assert no-op semantics.
        empty_resp = _scroll(COLLECTION_NAME, {
            "must": [{"key": "uid", "match": {"any": []}}]
        })
        assert empty_resp.ok, empty_resp.json()
        assert sorted(p['id'] for p in empty_resp.json()['result']['points']) == []
    finally:
        _set_strict_mode(COLLECTION_NAME, {"enabled": False})


def test_match_any_empty_with_strict_mode_in_must_not():
    """Reproduces the bug inside a must_not clause as reported."""
    _set_strict_mode(COLLECTION_NAME, {
        "enabled": True,
        "unindexed_filtering_retrieve": False,
    })
    try:
        resp = _scroll(COLLECTION_NAME, {
            "must_not": [{"key": "n", "match": {"any": []}}]
        })
        assert resp.ok, (
            "MatchAny(any=[]) inside must_not on an integer-indexed field "
            f"was rejected by strict mode: {resp.json()}"
        )
        # must_not over a no-op condition → all points returned.
        assert sorted(p['id'] for p in resp.json()['result']['points']) == [1, 2, 3]
    finally:
        _set_strict_mode(COLLECTION_NAME, {"enabled": False})


def test_match_any_empty_with_strict_mode_in_formula_query():
    """Reproduces the bug inside a FormulaQuery FieldCondition as reported."""
    _set_strict_mode(COLLECTION_NAME, {
        "enabled": True,
        "unindexed_filtering_retrieve": False,
    })
    try:
        resp = request_with_validation(
            api='/collections/{collection_name}/points/query',
            method="POST",
            path_params={'collection_name': COLLECTION_NAME},
            body={
                "prefetch": {
                    "query": [0.1, 0.2],
                    "limit": 10,
                },
                "query": {
                    "formula": {
                        "sum": [
                            {
                                "key": "n",
                                "match": {"any": []},
                            },
                            "$score",
                        ],
                    },
                },
                "limit": 10,
            },
        )
        assert resp.ok, (
            "MatchAny(any=[]) inside a FormulaQuery condition on an "
            f"integer-indexed field was rejected by strict mode: {resp.json()}"
        )
    finally:
        _set_strict_mode(COLLECTION_NAME, {"enabled": False})
