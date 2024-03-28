import pathlib
import time
from typing import Tuple

import pytest
import requests
from consensus_tests.assertions import assert_http_ok
from consensus_tests.fixtures import create_collection, drop_collection, upsert_random_points

from .utils import encode_jwt, make_peer_folder, start_first_peer, wait_for

N_PEERS = 1
N_REPLICA = 1
N_SHARDS = 1

SECRET = "my_top_secret_key"

API_KEY_HEADERS = {"Api-Key": SECRET}

COLL_NAME = "primary_test_collection"


def check_readyz(uri: str) -> bool:
    res = requests.get(f"{uri}/readyz")
    return res.ok


def start_api_key_instance(tmp_path: pathlib.Path, port_seed=10000) -> Tuple[str, str]:
    extra_env = {
        "QDRANT__SERVICE__API_KEY": SECRET,
        "QDRANT__SERVICE__JWT_RBAC": "true",
    }

    peer_dir = make_peer_folder(tmp_path, 0)

    (uri, bootstrap_uri) = start_first_peer(
        peer_dir, "api_key_peer.log", port=port_seed, extra_env=extra_env
    )

    time.sleep(0.5)

    wait_for(check_readyz, uri)

    return uri, bootstrap_uri


@pytest.fixture(scope="module")
def uri(tmp_path_factory: pytest.TempPathFactory):
    tmp_path = tmp_path_factory.mktemp("api_key_instance")

    uri, _bootstrap_uri = start_api_key_instance(tmp_path)

    create_collection(
        uri,
        collection=COLL_NAME,
        shard_number=N_SHARDS,
        replication_factor=N_REPLICA,
        headers=API_KEY_HEADERS,
    )

    upsert_random_points(uri, 100, COLL_NAME, headers=API_KEY_HEADERS)

    yield uri

    drop_collection(uri, COLL_NAME, headers=API_KEY_HEADERS)


def create_validation_collection(
        uri: str, collection: str, shard_number: int, replication_factor: int, timeout=10
):
    res = requests.put(
        f"{uri}/collections/{collection}?timeout={timeout}",
        json={
            "vectors": {"size": 1, "distance": "Dot"},
            "shard_number": shard_number,
            "replication_factor": replication_factor,
        },
        headers=API_KEY_HEADERS,
    )
    assert_http_ok(res)


def scroll_with_token(uri: str, collection: str, token: str) -> requests.Response:
    res = requests.post(
        f"{uri}/collections/{collection}/points/scroll",
        json={
            "limit": 10,
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    res.raise_for_status()
    return res


def test_value_exists_claim(uri: str):
    secondary_collection = "secondary_test_collection"

    key = "tokenId"
    value = "token_42"

    claims = {
        "access": "rw",
        "value_exists": {
            "collection": secondary_collection,
            "matches": [
                {"key": key, "value": value}
            ]
        }
    }
    token = encode_jwt(claims, SECRET)

    # Check that token does not work with unexisting collection
    with pytest.raises(requests.HTTPError):
        scroll_with_token(uri, COLL_NAME, token)

    # Create collection
    create_validation_collection(
        uri, secondary_collection, shard_number=N_SHARDS, replication_factor=N_REPLICA
    )

    # Check it does not work now
    with pytest.raises(requests.HTTPError):
        res = scroll_with_token(uri, COLL_NAME, token)

    # Upload validation point
    res = requests.put(
        f"{uri}/collections/{secondary_collection}/points?wait=true",
        json={
            "points": [
                {
                    "id": 42,
                    "vector": [0],
                    "payload": {key: value},
                }
            ]
        },
        headers=API_KEY_HEADERS,
    )
    assert_http_ok(res)

    # Check that token works now
    res = scroll_with_token(uri, COLL_NAME, token)
    assert len(res.json()["result"]["points"]) == 10

    # Delete validation point
    res = requests.post(
        f"{uri}/collections/{secondary_collection}/points/delete?wait=true",
        json={"points": [42]},
        headers=API_KEY_HEADERS,
    )
    res.raise_for_status()

    # Check it does not work now
    with pytest.raises(requests.HTTPError):
        scroll_with_token(uri, COLL_NAME, token)

    drop_collection(uri, secondary_collection, headers=API_KEY_HEADERS)
    drop_collection(uri, secondary_collection, headers=API_KEY_HEADERS)
