import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

COLLECTION_NAME = "test_prefix_match"

# id -> url payload; 4 holds an array value, 6 has no url at all.
POINT_URLS = {
    1: "https://qdrant.tech",
    2: "https://qdrant.tech/docs",
    3: "https://example.com",
    4: ["http://example.com", "https://qdrant.tech/blog"],
    5: "ftp://files.example.com",
    6: None,
}


def expected_ids(prefix):
    result = []
    for point_id, urls in POINT_URLS.items():
        if urls is None:
            continue
        values = urls if isinstance(urls, list) else [urls]
        if any(value.startswith(prefix) for value in values):
            result.append(point_id)
    return result


PREFIX_PROBES = [
    "https://qdrant.",
    "https://",
    "http",
    "ftp://",
    "https://example.com",
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


def _prefix_filter(key, prefix):
    return {"must": [{"key": key, "match": {"prefix": prefix}}]}


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
# 1. Without any index, prefix match executes via the payload fallback.
# ---------------------------------------------------------------------------

def test_prefix_match_without_index():
    for prefix in PREFIX_PROBES:
        assert _scroll_ids(_prefix_filter("url", prefix)) == expected_ids(prefix), prefix


# ---------------------------------------------------------------------------
# 2. Create a prefix-enabled keyword index; schema is echoed in collection
#    info, and results are unchanged.
# ---------------------------------------------------------------------------

def test_create_prefix_index():
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': COLLECTION_NAME},
        query_params={'wait': 'true'},
        body={
            "field_name": "url",
            "field_schema": {
                "type": "keyword",
                "prefix": True,
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
    assert url_schema['params']['prefix'] is True


def test_prefix_match_with_index():
    for prefix in PREFIX_PROBES:
        assert _scroll_ids(_prefix_filter("url", prefix)) == expected_ids(prefix), prefix


def test_prefix_match_count():
    response = request_with_validation(
        api='/collections/{collection_name}/points/count',
        method="POST",
        path_params={'collection_name': COLLECTION_NAME},
        body={"filter": _prefix_filter("url", "https://qdrant."), "exact": True},
    )
    assert response.ok
    assert response.json()['result']['count'] == len(expected_ids("https://qdrant."))


# ---------------------------------------------------------------------------
# 3. Facet with a prefix filter on the same key — the autocompletion flow.
#    Facet counts all values of the matching points.
# ---------------------------------------------------------------------------

def test_facet_with_prefix_filter():
    response = request_with_validation(
        api='/collections/{collection_name}/facet',
        method="POST",
        path_params={'collection_name': COLLECTION_NAME},
        body={
            "key": "url",
            "filter": _prefix_filter("url", "https://q"),
        },
    )
    assert response.ok
    hits = {hit['value']: hit['count'] for hit in response.json()['result']['hits']}
    # Points 1, 2 and 4 match; facet counts every value they hold.
    assert hits == {
        "https://qdrant.tech": 1,
        "https://qdrant.tech/docs": 1,
        "https://qdrant.tech/blog": 1,
        "http://example.com": 1,
    }


# ---------------------------------------------------------------------------
# 4. Strict mode: prefix filtering requires a keyword index with the
#    `prefix` option; a plain keyword index is not enough.
# ---------------------------------------------------------------------------

def test_strict_mode_requires_prefix_capability():
    # Plain keyword index (no `prefix`) on another field.
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
        # Exact match on the plain keyword index is allowed...
        response = _scroll({"must": [{"key": "tag", "match": {"value": "odd"}}]})
        assert response.ok, response.json()

        # ...but prefix match is rejected: the index lacks the prefix option.
        response = _scroll(_prefix_filter("tag", "od"))
        assert response.status_code == 400, response.json()
        assert "Index required but not found" in response.json()['status']['error']

        # On the prefix-enabled index the query is allowed.
        response = _scroll(_prefix_filter("url", "https://qdrant."))
        assert response.ok, response.json()
    finally:
        _set_strict_mode({"enabled": False})
