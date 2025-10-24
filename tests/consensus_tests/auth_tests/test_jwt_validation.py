import pytest
import requests
from consensus_tests import fixtures

from .utils import API_KEY_HEADERS, READ_ONLY_API_KEY, REST_URI, SECRET, encode_jwt

COLL_NAME = "jwt_test_collection"
OTHER_COLL_NAME = "jwt_other_test_collection"


@pytest.fixture(scope="module")
def collection_readonly_token():
    return encode_jwt(
        {
            "access": [
                {"collection": COLL_NAME, "access": "r"}
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


def test_recommend_collection_lookup(collection_readonly_token):
    # delete other collection if exists
    fixtures.drop_collection(REST_URI, OTHER_COLL_NAME, headers=API_KEY_HEADERS)

    # create other collection
    fixtures.create_collection(
        REST_URI,
        collection=OTHER_COLL_NAME,
        headers=API_KEY_HEADERS,
    )

    # populate collection with points
    fixtures.upsert_random_points(REST_URI, 100, COLL_NAME, headers=API_KEY_HEADERS)

    # recommending points from same collection should work
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/recommend",
        json={"positive": [1], "limit": 10},
        headers={"Authorization": f"Bearer {collection_readonly_token}"},
    )
    assert res.status_code == 200, res.json()

    # recommending points from other collection should fail
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/recommend",
        json={
            "positive": [1],
            "limit": 10,
            "lookup_from": {
                "collection": OTHER_COLL_NAME,
            }
        },
        headers={"Authorization": f"Bearer {collection_readonly_token}"},
    )
    assert res.status_code == 403, res.json()
    assert (
            res.json()["status"]["error"]
            == f"Forbidden: Access to collection {OTHER_COLL_NAME} is required"
    )


def test_query_recommendation_collection_lookup(collection_readonly_token):
    # delete other collection if exists
    fixtures.drop_collection(REST_URI, OTHER_COLL_NAME, headers=API_KEY_HEADERS)

    # create other collection
    fixtures.create_collection(
        REST_URI,
        collection=OTHER_COLL_NAME,
        headers=API_KEY_HEADERS,
    )

    # populate collection with points
    fixtures.upsert_random_points(REST_URI, 100, COLL_NAME, headers=API_KEY_HEADERS)

    # query recommendation with points from same collection should work
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/query",
        json={
            "query": {
                "recommend": {
                    "positive": [1],
                },
                "limit": 10,
            }
        },
        headers={"Authorization": f"Bearer {collection_readonly_token}"},
    )
    assert res.status_code == 200, res.json()

    # query recommending with points from other collection should fail
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/query",
        json={
            "recommend": {
                "positive": [1],
            },
            "limit": 10,
            "lookup_from": {
                "collection": OTHER_COLL_NAME,
            }
        },
        headers={"Authorization": f"Bearer {collection_readonly_token}"},
    )
    assert res.status_code == 403, res.json()
    assert (
            res.json()["status"]["error"]
            == f"Forbidden: Access to collection {OTHER_COLL_NAME} is required"
    )

    # query nested recommendation with points from other collection should fail
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/points/query",
        json={
            "prefetch": [
                {
                    "query": {
                        "recommend": {
                            "positive": [1],
                        }
                    },
                    "lookup_from": {
                        "collection": OTHER_COLL_NAME,
                    }
                }
            ],
            "limit": 10,
        },
        headers={"Authorization": f"Bearer {collection_readonly_token}"},
    )
    assert res.status_code == 403, res.json()
    assert (
            res.json()["status"]["error"]
            == f"Forbidden: Access to collection {OTHER_COLL_NAME} is required"
    )
