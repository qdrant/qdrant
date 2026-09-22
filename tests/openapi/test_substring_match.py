import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

COLLECTION_NAME = "test_substring_match"

# id -> url payload; 4 holds an array value, 6 has no url at all.
POINT_URLS = {
    1: "https://qdrant.tech",
    2: "https://qdrant.tech/docs",
    3: "https://example.com",
    4: ["http://example.com", "https://qdrant.tech/blog"],
    5: "ftp://files.example.com",
    6: None,
}


def expected_ids(substring):
    result = []
    for point_id, urls in POINT_URLS.items():
        if urls is None:
            continue
        values = urls if isinstance(urls, list) else [urls]
        if any(substring in value for value in values):
            result.append(point_id)
    return result


SUBSTRING_PROBES = [
    "qdrant",
    "example.com",
    "://",
    "tech/",
    "files",
    "QDRANT",  # case-sensitive: matches nothing
    "nonexistent",
    "",  # matches every point with a url value
]


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

    points = []
    for point_id, urls in POINT_URLS.items():
        payload = {"tag": "even" if point_id % 2 == 0 else "odd"}
        if urls is not None:
            payload["url"] = urls
            payload["url_prefix"] = urls
            payload["description"] = urls
        points.append({
            "id": point_id,
            "vector": [1.0, 0.0],
            "payload": payload,
        })

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"points": points},
    )
    assert response.ok


def _substring_filter(key, substring):
    return {"must": [{"key": key, "match": {"substring": substring}}]}


def _scroll(filter_body):
    return request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': COLLECTION_NAME},
        body={"filter": filter_body, "limit": 100},
    )


def _scroll_ids(filter_body):
    response = _scroll(filter_body)
    assert response.ok, response.json()
    return sorted(point['id'] for point in response.json()['result']['points'])


def _create_index(field_name, field_schema):
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': COLLECTION_NAME},
        query_params={'wait': 'true'},
        body={"field_name": field_name, "field_schema": field_schema},
    )
    assert response.ok


def _set_strict_mode(strict_mode_config):
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PATCH",
        path_params={"collection_name": COLLECTION_NAME},
        body={"strict_mode_config": strict_mode_config},
    )
    response.raise_for_status()


# ---------------------------------------------------------------------------
# 1. Without any index, substring match executes via the payload fallback.
# ---------------------------------------------------------------------------

def test_substring_match_without_index():
    for substring in SUBSTRING_PROBES:
        assert _scroll_ids(_substring_filter("url", substring)) == expected_ids(substring), substring


# ---------------------------------------------------------------------------
# 2. A keyword index without the `prefix` option has no key dictionary to
#    scan, so the condition keeps running through the payload fallback.
# ---------------------------------------------------------------------------

def test_substring_match_with_keyword_index_falls_back():
    _create_index("url", "keyword")
    for substring in SUBSTRING_PROBES:
        assert _scroll_ids(_substring_filter("url", substring)) == expected_ids(substring), substring


# ---------------------------------------------------------------------------
# 3. A keyword index with the `prefix` option serves the condition from its
#    key dictionary, with the same results.
# ---------------------------------------------------------------------------

def test_substring_match_with_keyword_prefix_index():
    _create_index("url_prefix", {"type": "keyword", "prefix": True})
    for substring in SUBSTRING_PROBES:
        assert _scroll_ids(_substring_filter("url_prefix", substring)) == expected_ids(substring), substring


def test_substring_match_count():
    response = request_with_validation(
        api='/collections/{collection_name}/points/count',
        method="POST",
        path_params={'collection_name': COLLECTION_NAME},
        body={"filter": _substring_filter("url_prefix", "qdrant"), "exact": True},
    )
    assert response.ok
    assert response.json()['result']['count'] == len(expected_ids("qdrant"))


# ---------------------------------------------------------------------------
# 4. A text index on the field cannot serve substring (it stores tokens, not
#    raw values), so the condition falls back to the payload scan and still
#    works. Only strict mode may reject it.
# ---------------------------------------------------------------------------

def test_substring_match_with_text_index_falls_back():
    _create_index("description", "text")
    for substring in SUBSTRING_PROBES:
        assert _scroll_ids(_substring_filter("description", substring)) == expected_ids(substring), substring


# ---------------------------------------------------------------------------
# 5. Strict mode: substring filtering is rejected whatever index the field
#    carries. No index bounds the condition to less than every distinct value
#    of the field, so there is none a user could create to make it allowed.
# ---------------------------------------------------------------------------

def test_strict_mode_rejects_substring():
    _set_strict_mode({
        "enabled": True,
        "unindexed_filtering_retrieve": False,
    })
    try:
        # Unindexed, text-indexed, keyword-indexed, and keyword index with the
        # `prefix` option: all rejected the same way.
        for key in ["tag", "description", "url", "url_prefix"]:
            response = _scroll(_substring_filter(key, "qdrant"))
            assert response.status_code == 400, response.json()
            assert "Substring matching is not allowed" in response.json()['status']['error'], key

        # Prefix matching on an index that supports it stays allowed.
        response = _scroll({"must": [{"key": "url_prefix", "match": {"prefix": "https://"}}]})
        assert response.ok, response.json()
    finally:
        _set_strict_mode({"enabled": False})
