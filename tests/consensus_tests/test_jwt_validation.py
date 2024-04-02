import json
import pathlib
import time
from typing import List, Tuple

import grpc_requests
import pytest
import requests
from grpc_interceptor import ClientCallDetails, ClientInterceptor
from consensus_tests.fixtures import (create_collection, drop_collection,
                                      upsert_random_points)

from .utils import encode_jwt, make_peer_folder, start_first_peer, wait_for

N_PEERS = 1
N_REPLICA = 1
N_SHARDS = 1

SECRET = "my_top_secret_key"

API_KEY_HEADERS = {"Api-Key": SECRET}
API_KEY_METADATA = [("api-key", SECRET)]

COLL_NAME = "primary_test_collection"
SHARD_ID = 0
SNAPSHOT_NAME = "test_snapshot"
POINT_ID = 0
FIELD_NAME = "test_field"
PEER_ID = 0

# "endpoint": [allowed_with_r, allowed_with_rw, allowed_with_manage]
# allowed_with_r - token with read access
# allowed_with_rw - token with read-write access
# allowed_with_manage - token with manage access (global write access)
TABLE_OF_ACCESS = {
    # Collections
    "PUT /collections/{collection_name}/shards": [False, True, True],
    "POST /collections/{collection_name}/shards/delete": [False, True, True],
    "GET /collections": [True, True, True],
    "GET /collections/{collection_name}": [True, True, True],
    "PUT /collections/{collection_name}": [False, False, True],
    "PATCH /collections/{collection_name}": [False, False, True],
    "DELETE /collections/{collection_name}": [False, False, True],
    "POST /collections/aliases": [False, True, True],
    "PUT /collections/{collection_name}/index": [False, False, True],
    "GET /collections/{collection_name}/exists": [True, True, True],
    "DELETE /collections/{collection_name}/index/{field_name}": [False, False, True],
    "GET /collections/{collection_name}/cluster": [True, True, True],
    "POST /collections/{collection_name}/cluster": [False, False, True],
    "GET /collections/{collection_name}/aliases": [True, True, True],
    "GET /aliases": [True, True, True],
    "POST /collections/{collection_name}/snapshots/upload": [False, False, True],
    "PUT /collections/{collection_name}/snapshots/recover": [False, False, True],
    "GET /collections/{collection_name}/snapshots": [False, True, True],
    "POST /collections/{collection_name}/snapshots": [False, True, True],
    "DELETE /collections/{collection_name}/snapshots/{snapshot_name}": [False, True, True],  #
    "GET /collections/{collection_name}/snapshots/{snapshot_name}": [True, True, True],  #
    "POST /collections/{collection_name}/shards/{shard_id}/snapshots/upload": [
        False,
        False,
        True,
    ],  #
    "PUT /collections/{collection_name}/shards/{shard_id}/snapshots/recover": [
        False,
        False,
        True,
    ],  #
    "GET /collections/{collection_name}/shards/{shard_id}/snapshots": [False, True, True],  #
    "POST /collections/{collection_name}/shards/{shard_id}/snapshots": [False, True, True],  #
    "DELETE /collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}": [
        False,
        True,
        True,
    ],  #
    "GET /collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}": [
        False,
        True,
        True,
    ],  #
    # Points
    "GET /collections/{collection_name}/points/{id}": [True, True, True],
    "POST /collections/{collection_name}/points": [True, True, True],
    "PUT /collections/{collection_name}/points": [False, True, True],
    "POST /collections/{collection_name}/points/delete": [False, True, True],
    "PUT /collections/{collection_name}/points/vectors": [False, True, True],
    "POST /collections/{collection_name}/points/vectors/delete": [False, True, True],
    "POST /collections/{collection_name}/points/payload": [False, True, True],
    "PUT /collections/{collection_name}/points/payload": [False, True, True],
    "POST /collections/{collection_name}/points/payload/delete": [False, True, True],
    "POST /collections/{collection_name}/points/payload/clear": [False, True, True],
    "POST /collections/{collection_name}/points/batch": [False, True, True],
    "POST /collections/{collection_name}/points/scroll": [True, True, True],
    "POST /collections/{collection_name}/points/search": [True, True, True],
    "POST /collections/{collection_name}/points/search/batch": [True, True, True],
    "POST /collections/{collection_name}/points/search/groups": [True, True, True],
    "POST /collections/{collection_name}/points/recommend": [True, True, True],
    "POST /collections/{collection_name}/points/recommend/batch": [True, True, True],
    "POST /collections/{collection_name}/points/recommend/groups": [True, True, True],
    "POST /collections/{collection_name}/points/discover": [True, True, True],
    "POST /collections/{collection_name}/points/discover/batch": [True, True, True],
    "POST /collections/{collection_name}/points/count": [True, True, True],
    # Cluster
    "GET /cluster": [True, True, True],
    "POST /cluster/recover": [False, False, True],
    "DELETE /cluster/peer/{peer_id}": [False, False, True],
    # Snapshots
    "GET /snapshots": [False, False, True],
    "POST /snapshots": [False, False, True],
    "DELETE /snapshots/{snapshot_name}": [False, False, True],
    "GET /snapshots/{snapshot_name}": [False, False, True],
    # Service
    "GET /": [True, True, True],
    "GET /readyz": [True, True, True],
    "GET /healthz": [True, True, True],
    "GET /livez": [True, True, True],
    "GET /telemetry": [False, False, True],
    "GET /metrics": [False, False, True],
    "POST /locks": [False, False, True],
    "GET /locks": [True, True, True],
}

GRPC_TO_REST_MAPPING = {
    "/qdrant.Collections/Get": "GET /collections/{collection_name}",
    "/qdrant.Collections/List": "GET /collections",
    "/qdrant.Collections/Create": "PUT /collections/{collection_name}",
    "/qdrant.Collections/Update": "PATCH /collections/{collection_name}",
    "/qdrant.Collections/Delete": "DELETE /collections/{collection_name}",
    "/qdrant.Collections/UpdateAliases": "POST /collections/aliases",
    "/qdrant.Collections/ListCollectionAliases": "GET /collections/{collection_name}/aliases",
    "/qdrant.Collections/ListAliases": "GET /aliases",
    "/qdrant.Collections/CollectionClusterInfo": "GET /collections/{collection_name}/cluster",
    "/qdrant.Collections/CollectionExists": "GET /collections/{collection_name}/exists",
    "/qdrant.Collections/UpdateCollectionClusterSetup": "POST /collections/{collection_name}/cluster",
    "/qdrant.Collections/CreateShardKey": "PUT /collections/{collection_name}/shards",
    "/qdrant.Collections/DeleteShardKey": "POST /collections/{collection_name}/shards/delete",
    "/qdrant.Points/Upsert": "PUT /collections/{collection_name}/points",
    "/qdrant.Points/Delete": "POST /collections/{collection_name}/points/delete",
    "/qdrant.Points/Get": "POST /collections/{collection_name}/points",
    "/qdrant.Points/UpdateVectors": "PUT /collections/{collection_name}/points/vectors",
    "/qdrant.Points/DeleteVectors": "POST /collections/{collection_name}/points/vectors/delete",
    "/qdrant.Points/SetPayload": "POST /collections/{collection_name}/points/payload",
    "/qdrant.Points/OverwritePayload": "PUT /collections/{collection_name}/points/payload",
    "/qdrant.Points/DeletePayload": "POST /collections/{collection_name}/points/payload/delete",
    "/qdrant.Points/ClearPayload": "POST /collections/{collection_name}/points/payload/clear",
    "/qdrant.Points/CreateFieldIndex": "PUT /collections/{collection_name}/index",
    "/qdrant.Points/DeleteFieldIndex": "DELETE /collections/{collection_name}/index/{field_name}",
    "/qdrant.Points/Search": "POST /collections/{collection_name}/points/search",
    "/qdrant.Points/SearchBatch": "POST /collections/{collection_name}/points/search/batch",
    "/qdrant.Points/SearchGroups": "POST /collections/{collection_name}/points/search/groups",
    "/qdrant.Points/Scroll": "POST /collections/{collection_name}/points/scroll",
    "/qdrant.Points/Recommend": "POST /collections/{collection_name}/points/recommend",
    "/qdrant.Points/RecommendBatch": "POST /collections/{collection_name}/points/recommend/batch",
    "/qdrant.Points/RecommendGroups": "POST /collections/{collection_name}/points/recommend/groups",
    "/qdrant.Points/Discover": "POST /collections/{collection_name}/points/discover",
    "/qdrant.Points/DiscoverBatch": "POST /collections/{collection_name}/points/discover/batch",
    "/qdrant.Points/Count": "POST /collections/{collection_name}/points/count",
    "/qdrant.Points/UpdateBatch": "POST /collections/{collection_name}/points/batch",
    "/qdrant.ShardSnapshots/Create": "POST /collections/{collection_name}/shards/{shard_id}/snapshots",
    "/qdrant.ShardSnapshots/List": "GET /collections/{collection_name}/shards/{shard_id}/snapshots",
    "/qdrant.ShardSnapshots/Delete": "DELETE /collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}",
    "/qdrant.ShardSnapshots/Recover": "PUT /collections/{collection_name}/shards/{shard_id}/snapshots/recover",
    "/qdrant.Snapshots/Create": "POST /collections/{collection_name}/snapshots",
    "/qdrant.Snapshots/List": "GET /collections/{collection_name}/snapshots",
    "/qdrant.Snapshots/Delete": "DELETE /collections/{collection_name}/snapshots/{snapshot_name}",
    "/qdrant.Snapshots/CreateFull": "POST /snapshots",
    "/qdrant.Snapshots/ListFull": "GET /snapshots",
    "/qdrant.Snapshots/DeleteFull": "DELETE /snapshots/{snapshot_name}",
    "/qdrant.Qdrant/HealthCheck": "GET /healthz",
    "/grpc.health.v1.Health/Check": "GET /healthz",
}


def test_grpc_to_rest_mapping():
    for rest_endpoint in GRPC_TO_REST_MAPPING.values():
        assert (
            rest_endpoint in TABLE_OF_ACCESS
        ), f"REST endpoint `{rest_endpoint}` not found in TABLE_OF_ACCESS"


def test_all_rest_endpoints_are_covered():
    # Load the JSON content from the openapi.json file
    with open('./docs/redoc/master/openapi.json', 'r') as file:
        openapi_data = json.load(file)

    # Extract all endpoint paths
    endpoint_paths = []
    for path in openapi_data['paths'].keys():
        for method in openapi_data['paths'][path]:
            method = method.upper()
            endpoint_paths.append(f"{method} {path}")
            print(f"{method} {path}")

    # check that all endpoints are covered in TABLE_OF_ACCESS
    for endpoint in endpoint_paths:
        assert (
            endpoint in TABLE_OF_ACCESS
        ), f"REST endpoint `{endpoint}` not found in TABLE_OF_ACCESS"

class MetadataInterceptor(ClientInterceptor):
    """A test interceptor that injects invocation metadata."""

    def __init__(self, metadata: List[Tuple[str, str]]):
        self._metadata = metadata

    def intercept(self, method, request_or_iterator, call_details: ClientCallDetails):
        """Add invocation metadata to request."""
        new_details = call_details._replace(metadata=self._metadata)
        return method(request_or_iterator, new_details)


def test_all_grpc_endpoints_are_covered(uris: Tuple[str, str]):
    _rest_uri, grpc_uri = uris
    
    # read grpc services from the reflection server
    client: grpc_requests.Client = grpc_requests.Client(
        grpc_uri, interceptors=[MetadataInterceptor(API_KEY_METADATA)]
    )

    # check that all endpoints are covered in GRPC_TO_REST_MAPPING
    for service_name in client.service_names:
        service = client.service(service_name)
        for method in service.method_names:
            grpc_endpoint = f"/{service_name}/{method}"
            assert (
                grpc_endpoint in GRPC_TO_REST_MAPPING
            ), f"GRPC endpoint `{grpc_endpoint}` not found in GRPC_TO_REST_MAPPING"


def start_api_key_instance(tmp_path: pathlib.Path, port_seed=10000) -> Tuple[str, str]:
    extra_env = {
        "QDRANT__SERVICE__API_KEY": SECRET,
        "QDRANT__SERVICE__JWT_RBAC": "true",
    }

    peer_dir = make_peer_folder(tmp_path, 0)

    (rest_uri, _bootstrap_uri) = start_first_peer(
        peer_dir, "api_key_peer.log", port=port_seed, extra_env=extra_env
    )

    time.sleep(0.5)

    def check_readyz(uri: str) -> bool:
        res = requests.get(f"{uri}/readyz")
        return res.ok

    wait_for(check_readyz, rest_uri)

    grpc_uri = f"127.0.0.1:{port_seed + 1}"

    return rest_uri, grpc_uri


@pytest.fixture(scope="module")
def uris(tmp_path_factory: pytest.TempPathFactory):
    tmp_path = tmp_path_factory.mktemp("api_key_instance")

    rest_uri, grpc_uri = start_api_key_instance(tmp_path)

    create_collection(
        rest_uri,
        collection=COLL_NAME,
        shard_number=N_SHARDS,
        replication_factor=N_REPLICA,
        headers=API_KEY_HEADERS,
    )

    upsert_random_points(rest_uri, 100, COLL_NAME, headers=API_KEY_HEADERS)

    # TODO: create fixtures for payload index, snapshots, shards, shard_key, alias,

    yield rest_uri, grpc_uri

    drop_collection(rest_uri, COLL_NAME, headers=API_KEY_HEADERS)


def create_validation_collection(
    uris: Tuple[str, str], collection: str, shard_number: int, replication_factor: int, timeout=10
):
    uri = uris[0]
    res = requests.put(
        f"{uri}/collections/{collection}?timeout={timeout}",
        json={
            "vectors": {"size": 1, "distance": "Dot"},
            "shard_number": shard_number,
            "replication_factor": replication_factor,
        },
        headers=API_KEY_HEADERS,
    )
    res.raise_for_status()


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


def test_value_exists_claim(uris: Tuple[str, str]):
    rest_uri, grpc_uri = uris

    secondary_collection = "secondary_test_collection"

    key = "tokenId"
    value = "token_42"

    claims = {
        "value_exists": {
            "collection": secondary_collection,
            "matches": [{"key": key, "value": value}],
        },
    }
    token = encode_jwt(claims, SECRET)

    # Check that token does not work with unexisting collection
    with pytest.raises(requests.HTTPError):
        scroll_with_token(rest_uri, COLL_NAME, token)

    # Create collection
    create_validation_collection(
        rest_uri, secondary_collection, shard_number=N_SHARDS, replication_factor=N_REPLICA
    )

    # Check it does not work now
    with pytest.raises(requests.HTTPError):
        res = scroll_with_token(rest_uri, COLL_NAME, token)

    # Upload validation point
    res = requests.put(
        f"{rest_uri}/collections/{secondary_collection}/points?wait=true",
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
    res.raise_for_status()

    # Check that token works now
    res = scroll_with_token(rest_uri, COLL_NAME, token)
    assert len(res.json()["result"]["points"]) == 10

    # Delete validation point
    res = requests.post(
        f"{rest_uri}/collections/{secondary_collection}/points/delete?wait=true",
        json={"points": [42]},
        headers=API_KEY_HEADERS,
    )
    res.raise_for_status()

    # Check it does not work now
    with pytest.raises(requests.HTTPError):
        scroll_with_token(rest_uri, COLL_NAME, token)

    drop_collection(rest_uri, secondary_collection, headers=API_KEY_HEADERS)
    drop_collection(rest_uri, secondary_collection, headers=API_KEY_HEADERS)


def test_access(uris: Tuple[str, str]):
    rest_uri, grpc_uri = uris

    # Global read access token
    token_read = encode_jwt({"access": "r"}, SECRET)

    # Collection read access token
    token_coll_r = encode_jwt({"access": {"collections": [COLL_NAME], "access": "r"}}, SECRET)

    # Collection read-write access token
    token_coll_rw = encode_jwt({"access": {"collections": [COLL_NAME], "access": "rw"}}, SECRET)

    # Global manage access token
    token_manage = encode_jwt({"access": "rw"}, SECRET)

    # Check REST endpoints
    for endpoint, access in TABLE_OF_ACCESS.items():
        method, path = endpoint.split(" ")
        path = path.format(
            collection_name=COLL_NAME,
            shard_id=SHARD_ID,
            snapshot_name=SNAPSHOT_NAME,
            id=POINT_ID,
            field_name=FIELD_NAME,
            peer_id=PEER_ID,
        )

        check_rest_access(rest_uri, method, path, access[0], token_read)
        check_rest_access(rest_uri, method, path, access[0], token_coll_r)
        check_rest_access(rest_uri, method, path, access[1], token_coll_rw)
        check_rest_access(rest_uri, method, path, access[2], token_manage)

    # Check GRPC endpoints
    grpc_read = grpc_requests.Client(
        grpc_uri, interceptors=[MetadataInterceptor([("authorization", f"Bearer {token_read}")])]
    )
    
    grpc_coll_r = grpc_requests.Client(
        grpc_uri, interceptors=[MetadataInterceptor([("authorization", f"Bearer {token_coll_r}")])]
    )
    
    grpc_coll_rw = grpc_requests.Client(
        grpc_uri, interceptors=[MetadataInterceptor([("authorization", f"Bearer {token_coll_rw}")])]
    )
    
    grpc_manage = grpc_requests.Client(
        grpc_uri, interceptors=[MetadataInterceptor([("authorization", f"Bearer {token_manage}")])]
    )
    for grpc_endpoint, rest_endpoint in GRPC_TO_REST_MAPPING.items():
        access = TABLE_OF_ACCESS[rest_endpoint]
        service = grpc_endpoint.split("/")[1]
        method = grpc_endpoint.split("/")[2]
        
        check_grpc_access(grpc_read, service, method, access[0])
        check_grpc_access(grpc_coll_r, service, method, access[0])
        check_grpc_access(grpc_coll_rw, service, method, access[1])
        check_grpc_access(grpc_manage, service, method, access[2])


def check_rest_access(uri: str, method: str, path: str, should_succeed: bool, token: str):
    res = requests.request(method, f"{uri}{path}", headers={"Authorization": f"Bearer {token}"})

    if should_succeed:
        assert res.ok
    else:
        assert res.status_code == 403
        
def check_grpc_access(client: grpc_requests.Client, service: str, method: str, should_succeed: bool):
    res = client.request(service=service, method=method)
    if should_succeed:
        assert res.ok
    else:
        assert res.status_code == 403
