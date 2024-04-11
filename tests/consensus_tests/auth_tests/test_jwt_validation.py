import pytest
import requests
from consensus_tests import fixtures
from consensus_tests.utils import kill_all_processes

from .utils import (
    API_KEY_HEADERS,
    COLL_NAME,
    REST_URI,
    SECRET,
    encode_jwt,
    start_jwt_protected_cluster,
)


@pytest.fixture(scope="module", autouse=True)
def setup(tmp_path_factory: pytest.TempPathFactory):
    tmp_path = tmp_path_factory.mktemp("api_key_instance")

    peer_api_uris, peer_dirs, bootstrap_uri = start_jwt_protected_cluster(tmp_path)

    fixtures.create_collection(
        REST_URI,
        collection=COLL_NAME,
        headers=API_KEY_HEADERS,
    )

    fixtures.upsert_random_points(REST_URI, 100, COLL_NAME, headers=API_KEY_HEADERS)

    yield peer_api_uris, peer_dirs, bootstrap_uri

    fixtures.drop_collection(REST_URI, COLL_NAME, headers=API_KEY_HEADERS)
    kill_all_processes()


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


def test_payload_filters_queries():
    token = encode_jwt(
        {"access": [{"collection": COLL_NAME, "access": "r", "payload": {"city": "Berlin"}}]},
        SECRET,
    )

    # With scroll
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/scroll",
        json={
            "limit": 100,
            "with_payload": True,
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    res.raise_for_status()

    cities_in_payload = set(point["payload"].get("city") for point in res.json()["result"]["points"])

    assert cities_in_payload == {"Berlin"}

    # With search
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/search",
        json={
            "vector": [1, 2, 3, 4],
            "limit": 100,
            "with_payload": True,
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    res.raise_for_status()

    cities_in_payload = set(point["payload"].get("city") for point in res.json()["result"])

    assert cities_in_payload == {"Berlin"}

    # With count
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/count",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )
    res.raise_for_status()

    res_expected = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/count",
        json={"filter": {"must": [{"key": "city", "match": {"value": "Berlin"}}]}},
        headers=API_KEY_HEADERS,
    )
    res_expected.raise_for_status()

    assert res.json()["result"] == res_expected.json()["result"]
    res_expected = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/count",
        json={"filter": {"must": [{"key": "city", "match": {"value": "Berlin"}}]}},
        headers=API_KEY_HEADERS,
    )
    res_expected.raise_for_status()

    assert res.json()["result"] == res_expected.json()["result"]
