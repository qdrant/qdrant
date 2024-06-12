import pytest
import requests
from consensus_tests import fixtures

from .utils import API_KEY_HEADERS, READ_ONLY_API_KEY, REST_URI, SECRET, encode_jwt

COLL_NAME = "jwt_test_collection"


@pytest.fixture(scope="module")
def berlin_token():
    return encode_jwt(
        {
            "access": [
                {"collection": COLL_NAME, "access": "r", "payload": {"city": "Berlin"}}
            ]
        },
        SECRET,
    )


@pytest.fixture(scope="module", autouse=True)
def setup(jwt_cluster):
    peer_api_uris, peer_dirs, bootstrap_uri = jwt_cluster

    fixtures.create_collection(
        REST_URI,
        collection=COLL_NAME,
        headers=API_KEY_HEADERS,
    )

    fixtures.upsert_random_points(REST_URI, 100, COLL_NAME, headers=API_KEY_HEADERS)

    yield peer_api_uris, peer_dirs, bootstrap_uri

    fixtures.drop_collection(REST_URI, COLL_NAME, headers=API_KEY_HEADERS)


def create_validation_collection(collection: str, timeout=10):
    res = requests.put(
        f"{REST_URI}/collections/{collection}?timeout={timeout}",
        json={},
        headers=API_KEY_HEADERS,
    )
    res.raise_for_status()


def scroll_with_token(collection: str, token: str) -> requests.Response:
    res = requests.post(
        f"{REST_URI}/collections/{collection}/points/scroll",
        json={
            "limit": 10,
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    res.raise_for_status()
    return res


def test_value_exists_claim():
    validation_collection = "jwt_validation_collection"

    key = "tokenId"
    value = "token_42"

    claims = {
        "value_exists": {
            "collection": validation_collection,
            "matches": [{"key": key, "value": value}],
        },
    }
    token = encode_jwt(claims, SECRET)

    # Check that token does not work with unexisting collection
    with pytest.raises(requests.HTTPError):
        scroll_with_token(COLL_NAME, token)

    # Create collection
    create_validation_collection(validation_collection)

    # Check it still does not work
    with pytest.raises(requests.HTTPError):
        res = scroll_with_token(COLL_NAME, token)

    # Upload validation point
    requests.put(
        f"{REST_URI}/collections/{validation_collection}/points?wait=true",
        json={
            "points": [
                {
                    "id": 42,
                    "vectors": {},
                    "payload": {key: value},
                }
            ]
        },
        headers=API_KEY_HEADERS,
    ).raise_for_status()

    # Check that token works now
    res = scroll_with_token(COLL_NAME, token)
    assert len(res.json()["result"]["points"]) == 10

    # Delete validation point
    res = requests.post(
        f"{REST_URI}/collections/{validation_collection}/points/delete?wait=true",
        json={"points": [42]},
        headers=API_KEY_HEADERS,
    ).raise_for_status()

    # Check it does not work now
    with pytest.raises(requests.HTTPError):
        scroll_with_token(COLL_NAME, token)

    fixtures.drop_collection(REST_URI, validation_collection, headers=API_KEY_HEADERS)


def test_payload_filters_scroll(berlin_token):
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/scroll",
        json={
            "limit": 100,
            "with_payload": True,
        },
        headers={"Authorization": f"Bearer {berlin_token}"},
    )
    res.raise_for_status()

    cities_in_payload = set(
        point["payload"].get("city") for point in res.json()["result"]["points"]
    )

    assert cities_in_payload == {"Berlin"}


def test_payload_filters_search(berlin_token):
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/search",
        json={
            "vector": [1, 2, 3, 4],
            "limit": 100,
            "with_payload": True,
        },
        headers={"Authorization": f"Bearer {berlin_token}"},
    )
    res.raise_for_status()

    cities_in_payload = set(
        point["payload"].get("city") for point in res.json()["result"]
    )

    assert cities_in_payload == {"Berlin"}


def test_payload_filters_count(berlin_token):
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/count",
        json={},
        headers={"Authorization": f"Bearer {berlin_token}"},
    )
    res.raise_for_status()

    res_expected = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/count",
        json={"filter": {"must": [{"key": "city", "match": {"value": "Berlin"}}]}},
        headers=API_KEY_HEADERS,
    )
    res_expected.raise_for_status()

    assert res.json()["result"] == res_expected.json()["result"]


@pytest.mark.parametrize(
    "prefetch",
    [
        None,
        {"query": [0.1, 0.2, 0.3, 0.4]},
        [{"query": [0.1, 0.2, 0.3, 0.4]}, {"limit": 100}],
    ],
)
def test_payload_filters_query(berlin_token, prefetch):
    # Check that there are more cities in the payload
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/query",
        json={
            "prefetch": prefetch,
            "query": [0.1, 0.2, 0.3, 0.4],
            "limit": 100,
            "with_payload": True,
        },
        headers=API_KEY_HEADERS,
    )

    cities_in_payload = set(
        point["payload"].get("city") for point in res.json()["result"]
    )

    assert len(cities_in_payload) > 1

    # Now check that there is only Berlin with the payload claim in the token
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/query",
        json={
            "prefetch": prefetch,
            "query": [0.1, 0.2, 0.3, 0.4],
            "limit": 100,
            "with_payload": True,
        },
        headers={"Authorization": f"Bearer {berlin_token}"},
    )

    cities_in_payload = set(
        point["payload"].get("city") for point in res.json()["result"]
    )

    assert cities_in_payload == {"Berlin"}


def test_payload_claim_fails_with_ids_in_recommend(berlin_token):
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/recommend",
        json={"positive": [1], "limit": 10},
        headers={"Authorization": f"Bearer {berlin_token}"},
    )
    assert res.status_code == 403, res.json()
    assert (
        res.json()["status"]["error"]
        == 'Forbidden: This operation is not allowed when "payload" restriction is present for collection jwt_test_collection'
    )


@pytest.mark.parametrize("target", [1, None])
@pytest.mark.parametrize("context", [None, [{"positive": 1, "negative": 3}]])
def test_payload_claim_fails_with_ids_in_discover(berlin_token, target, context):
    if not target and not context:
        return

    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/discover",
        json={"target": target, "context": context, "limit": 10},
        headers={"Authorization": f"Bearer {berlin_token}"},
    )
    assert res.status_code == 403, res.json()
    assert (
        res.json()["status"]["error"]
        == 'Forbidden: This operation is not allowed when "payload" restriction is present for collection jwt_test_collection'
    )


@pytest.mark.parametrize(
    "query",
    [
        1,
        {"nearest": 1},
        {"recommend": {"positive": [1]}},
        {"recommend": {"negative": [1]}},
        {"recommend": {"negative": [1], "strategy": "best_score"}},
        {"discover": {"target": 1, "context": [{"positive": 1, "negative": 2}]}},
        {"context": [{"positive": 1, "negative": 2}]},
    ],
)
@pytest.mark.parametrize("in_prefetch", [False, True])
def test_payload_claim_fails_with_ids_in_query(berlin_token, query, in_prefetch):
    if in_prefetch:
        res = requests.post(
            f"{REST_URI}/collections/{COLL_NAME}/points/query",
            json={
                "prefetch": {"query": query},
                "query": [0.1, 0.2, 0.3, 0.4],
                "limit": 100,
                "with_payload": True,
            },
            headers={"Authorization": f"Bearer {berlin_token}"},
        )
    else:
        res = requests.post(
            f"{REST_URI}/collections/{COLL_NAME}/points/query",
            json={
                "query": query,
                "limit": 100,
                "with_payload": True,
            },
            headers={"Authorization": f"Bearer {berlin_token}"},
        )

    assert res.status_code == 403, res.json()
    assert (
        res.json()["status"]["error"]
        == 'Forbidden: This operation is not allowed when "payload" restriction is present for collection jwt_test_collection'
    )
