import re

import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

COLLECTION_NAME = "test_regex_match"

# id -> url payload; 4 holds an array value, 6 has no url at all.
POINT_URLS = {
    1: "https://qdrant.tech",
    2: "https://qdrant.tech/docs/regex.pdf",
    3: "https://example.com/Report-2024.PDF",
    4: ["http://example.com", "https://qdrant.tech/blog"],
    5: "ftp://files.example.com/archive-2023.tar.gz",
    6: None,
}

# Patterns whose semantics agree between Rust `regex` and Python `re`.
REGEX_PROBES = [
    "qdrant",
    r"\.pdf$",
    r"(?i)\.pdf$",
    "^ftp://",
    r"-\d{4}\.",
    "(blog|docs)",
    "example.com$",
    "nonexistent",
    "",  # matches every point with a url value
]


def expected_ids(regex):
    pattern = re.compile(regex)
    result = []
    for point_id, urls in POINT_URLS.items():
        if urls is None:
            continue
        values = urls if isinstance(urls, list) else [urls]
        if any(pattern.search(value) for value in values):
            result.append(point_id)
    return result


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
            payload["unindexed_url"] = urls
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


def _regex_filter(key, regex):
    return {"must": [{"key": key, "match": {"regex": regex}}]}


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


def _set_strict_mode(strict_mode_config):
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PATCH",
        path_params={"collection_name": COLLECTION_NAME},
        body={"strict_mode_config": strict_mode_config},
    )
    response.raise_for_status()


# ---------------------------------------------------------------------------
# 1. Without any index, regex match executes via the payload fallback.
# ---------------------------------------------------------------------------

def test_regex_match_without_index():
    for regex in REGEX_PROBES:
        assert _scroll_ids(_regex_filter("url", regex)) == expected_ids(regex), regex


def test_invalid_regex_is_rejected():
    for regex in ["(unclosed", r"(a)\1", "a(?=b)"]:
        response = _scroll(_regex_filter("url", regex))
        assert response.status_code == 422, (regex, response.json())
        assert "invalid regex" in response.json()["status"]["error"]


def test_non_re2_regex_is_rejected():
    # Valid for some engines, but not part of RE2 syntax.
    for regex in ["[a-z&&[^aeiou]]", r"\p{Emoji}", "(?x) a b", r"\u0041"]:
        response = _scroll(_regex_filter("url", regex))
        assert response.status_code == 422, (regex, response.json())
        assert "not part of RE2" in response.json()["status"]["error"]


# ---------------------------------------------------------------------------
# 2. Create a regex-enabled keyword index; schema is echoed in collection
#    info, and results are unchanged.
# ---------------------------------------------------------------------------

def test_create_regex_index():
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': COLLECTION_NAME},
        query_params={'wait': 'true'},
        body={
            "field_name": "url",
            "field_schema": {
                "type": "keyword",
                "regex": True,
            },
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': COLLECTION_NAME},
    )
    assert response.ok
    url_schema = response.json()['result']['payload_schema']['url']
    assert url_schema['data_type'] == "keyword"
    assert url_schema['params']['regex'] is True


def test_regex_match_with_index():
    for regex in REGEX_PROBES:
        assert _scroll_ids(_regex_filter("url", regex)) == expected_ids(regex), regex


def test_regex_match_count():
    response = request_with_validation(
        api='/collections/{collection_name}/points/count',
        method="POST",
        path_params={'collection_name': COLLECTION_NAME},
        body={"filter": _regex_filter("url", r"(?i)\.pdf$"), "exact": True},
    )
    assert response.ok
    assert response.json()['result']['count'] == len(expected_ids(r"(?i)\.pdf$"))


# ---------------------------------------------------------------------------
# 3. Strict mode: reading payload to answer a regex is allowed only when
#    unindexed filtering is; otherwise the field needs a keyword index with
#    the `regex` option.
# ---------------------------------------------------------------------------

def test_strict_mode_requires_regex_capability():
    # Plain keyword index (no `regex`) on another field.
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': COLLECTION_NAME},
        query_params={'wait': 'true'},
        body={"field_name": "tag", "field_schema": "keyword"},
    )
    assert response.ok

    _set_strict_mode({
        "enabled": True,
        "unindexed_filtering_retrieve": False,
    })
    try:
        # A regex on an unindexed field or a plain keyword index is rejected.
        for key in ["unindexed_url", "tag"]:
            response = _scroll(_regex_filter(key, "od"))
            assert response.status_code == 400, response.json()
            assert "Index required but not found" in response.json()['status']['error']

        # On the regex-enabled index the query is allowed.
        assert _scroll_ids(_regex_filter("url", "qdrant")) == expected_ids("qdrant")
    finally:
        _set_strict_mode({"enabled": False})

    _set_strict_mode({
        "enabled": True,
        "unindexed_filtering_retrieve": True,
    })
    try:
        # Unindexed filtering allowed: the payload fallback answers it.
        assert _scroll_ids(_regex_filter("unindexed_url", r"\.pdf$")) == expected_ids(r"\.pdf$")
    finally:
        _set_strict_mode({"enabled": False})
