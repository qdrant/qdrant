import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

COLLECTION_NAME = "test_match_except_empty"


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


def _scroll_ids(collection_name, filter_body):
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": filter_body,
            "limit": 10,
            "with_payload": True,
        }
    )
    assert response.ok
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


# ---------------------------------------------------------------------------
# 1. Empty except list — no index (baseline)
# ---------------------------------------------------------------------------

def test_match_except_empty_integer_no_index():
    """except: [] without index should return all points that have the field."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "n", "match": {"except": []}}]
    })
    assert ids == [1, 2, 3]


def test_match_except_empty_keyword_no_index():
    """except: [] without index should return all points that have the field."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "tag", "match": {"except": []}}]
    })
    assert ids == [1, 2, 3]


def test_match_except_empty_uuid_no_index():
    """except: [] without index should return all points that have the field."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "uid", "match": {"except": []}}]
    })
    assert ids == [1, 2, 3]


# ---------------------------------------------------------------------------
# 2. Non-empty except list with matching type — no index (baseline)
# ---------------------------------------------------------------------------

def test_match_except_integers_no_index():
    """except: [1] should exclude point 1."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "n", "match": {"except": [1]}}]
    })
    assert ids == [2, 3]


def test_match_except_strings_no_index():
    """except: ["a"] should exclude point 1."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "tag", "match": {"except": ["a"]}}]
    })
    assert ids == [2, 3]


def test_match_except_uuids_no_index():
    """except one UUID should exclude that point."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "uid", "match": {"except": [
            "550e8400-e29b-41d4-a716-446655440001"
        ]}}]
    })
    assert ids == [2, 3]


# ---------------------------------------------------------------------------
# 3. Create indexes for all three field types.
#    Tests below run after index creation and verify indexed path matches
#    the non-indexed semantics.
# ---------------------------------------------------------------------------

def test_create_indexes():
    _create_index(COLLECTION_NAME, "n", "integer")
    _create_index(COLLECTION_NAME, "tag", "keyword")
    _create_index(COLLECTION_NAME, "uid", "uuid")


# ---------------------------------------------------------------------------
# 4. Empty except list — with index (the original bug from #9050)
# ---------------------------------------------------------------------------

def test_match_except_empty_integer_with_index():
    """
    Bug: https://github.com/qdrant/qdrant/issues/9050

    After creating an integer payload index, `match: {"except": []}` must
    still return all points — same as the non-indexed path. An empty except
    list means "exclude nothing", i.e. match everything.
    """
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "n", "match": {"except": []}}]
    })
    assert ids == [1, 2, 3], (
        f"match except [] with integer index should return all points, got {ids}"
    )


def test_match_except_empty_keyword_with_index():
    """Same check for keyword index."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "tag", "match": {"except": []}}]
    })
    assert ids == [1, 2, 3], (
        f"match except [] with keyword index should return all points, got {ids}"
    )


def test_match_except_empty_uuid_with_index():
    """Same check for UUID index."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "uid", "match": {"except": []}}]
    })
    assert ids == [1, 2, 3], (
        f"match except [] with UUID index should return all points, got {ids}"
    )


# ---------------------------------------------------------------------------
# 5. Non-empty except list with matching type — with index
# ---------------------------------------------------------------------------

def test_match_except_integers_with_index():
    """except: [1] with integer index should exclude point 1."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "n", "match": {"except": [1]}}]
    })
    assert ids == [2, 3]


def test_match_except_strings_with_index():
    """except: ["a"] with keyword index should exclude point 1."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "tag", "match": {"except": ["a"]}}]
    })
    assert ids == [2, 3]


def test_match_except_uuids_with_index():
    """except one UUID with UUID index should exclude that point."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "uid", "match": {"except": [
            "550e8400-e29b-41d4-a716-446655440001"
        ]}}]
    })
    assert ids == [2, 3]


# ---------------------------------------------------------------------------
# 6. Cross-type except: string values on integer index, integer values on
#    keyword/UUID index. These are type mismatches, so no value in the index
#    actually matches the exclusion list — meaning nothing is excluded and
#    all points should be returned (same as except: []).
# ---------------------------------------------------------------------------

def test_match_except_strings_on_integer_index():
    """String except values on an integer-indexed field: type mismatch → match all."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "n", "match": {"except": ["foo", "bar"]}}]
    })
    assert ids == [1, 2, 3], (
        f"cross-type string except on integer index should return all points, got {ids}"
    )


def test_match_except_integers_on_keyword_index():
    """Integer except values on a keyword-indexed field: type mismatch → match all."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "tag", "match": {"except": [999, 888]}}]
    })
    assert ids == [1, 2, 3], (
        f"cross-type integer except on keyword index should return all points, got {ids}"
    )


def test_match_except_integers_on_uuid_index():
    """Integer except values on a UUID-indexed field: type mismatch → match all."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "uid", "match": {"except": [999, 888]}}]
    })
    assert ids == [1, 2, 3], (
        f"cross-type integer except on UUID index should return all points, got {ids}"
    )


# ---------------------------------------------------------------------------
# 7. Exclude-all: when every value in the field is listed in except,
#    no points should match.
# ---------------------------------------------------------------------------

def test_match_except_all_integers_with_index():
    """Excluding every integer value should return no points."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "n", "match": {"except": [1, 2, 3]}}]
    })
    assert ids == []


def test_match_except_all_strings_with_index():
    """Excluding every keyword value should return no points."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "tag", "match": {"except": ["a", "b", "c"]}}]
    })
    assert ids == []


def test_match_except_all_uuids_with_index():
    """Excluding every UUID value should return no points."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "uid", "match": {"except": [
            "550e8400-e29b-41d4-a716-446655440001",
            "550e8400-e29b-41d4-a716-446655440002",
            "550e8400-e29b-41d4-a716-446655440003",
        ]}}]
    })
    assert ids == []
