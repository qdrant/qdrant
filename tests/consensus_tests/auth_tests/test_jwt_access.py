import json
import tempfile
from inspect import isfunction
from typing import Callable, List, Optional, Tuple, Union

import grpc
import grpc_requests
import pytest
import requests
from consensus_tests import fixtures
from grpc_interceptor import ClientCallDetails, ClientInterceptor

from .utils import (
    API_KEY_HEADERS,
    API_KEY_METADATA,
    GRPC_URI,
    READ_ONLY_API_KEY,
    READ_ONLY_API_KEY_METADATA,
    REST_URI,
    SECRET,
    encode_jwt,
    random_str,
)

COLL_NAME = "jwt_test_collection"

MAX_CALLS_IN_CHECK_ACCESS = 16

# Global read access token
TOKEN_R = encode_jwt({"access": "r"}, SECRET)

# Collection read access token
TOKEN_COLL_R = encode_jwt({"access": [{"collection": COLL_NAME, "access": "r"}]}, SECRET)

# Collection read-write access token
TOKEN_COLL_RW = encode_jwt({"access": [{"collection": COLL_NAME, "access": "rw"}]}, SECRET)

# Collection read-write access token with payload constraint
TOKEN_COLL_RW_PAYLOAD = encode_jwt(
    {"access": [{"collection": COLL_NAME, "access": "rw", "payload": {"user_id": "123"}}]}, SECRET
)

# Collection read access token with payload constraint
TOKEN_COLL_R_PAYLOAD = encode_jwt(
    {"access": [{"collection": COLL_NAME, "access": "r", "payload": {"user_id": 123}}]}, SECRET
)

# Global manage access token
TOKEN_M = encode_jwt({"access": "m"}, SECRET)

SHARD_ID = 1
SNAPSHOT_NAME = "test_snapshot"
POINT_ID = 0
FIELD_NAME = "test_field"
PEER_ID = 0
SHARD_KEY = "existing_shard_key"

_cached_grpc_clients = None

SHARD_KEY_SELECTOR = {"shard_key_selector": {"shard_keys": [{"keyword": SHARD_KEY}]}}


@pytest.fixture(scope="module", autouse=True)
def setup(jwt_cluster):
    peer_api_uris, peer_dirs, bootstrap_uri = jwt_cluster

    fixtures.create_collection(
        REST_URI,
        collection=COLL_NAME,
        sharding_method="custom",
        headers=API_KEY_HEADERS,
    )

    requests.put(
        f"{REST_URI}/collections/{COLL_NAME}/shards",
        json={"shard_key": SHARD_KEY},
        headers=API_KEY_HEADERS,
    ).raise_for_status()

    yield peer_api_uris, peer_dirs, bootstrap_uri

    fixtures.drop_collection(REST_URI, COLL_NAME, headers=API_KEY_HEADERS)


class Access:
    def __init__(self, r, coll_rw, m=True, coll_r=None, coll_rw_payload=None):
        self.read = r
        self.coll_rw = coll_rw
        self.manage = m
        self.coll_r = r if coll_r is None else coll_r
        self.coll_rw_payload = coll_rw if coll_rw_payload is None else coll_rw_payload
        self.coll_r_payload = self.read and self.coll_rw_payload


class EndpointAccess:
    def __init__(self, r, coll_rw, m, rest_endpoint, grpc_endpoint=None, **kwargs):
        self.access = Access(r, coll_rw, m, **kwargs)
        self.rest_endpoint = rest_endpoint
        self.grpc_endpoint = grpc_endpoint


ACTION_ACCESS = {
    ### Collections ###
    "list_collections": EndpointAccess(
        True, True, True, "GET /collections", "qdrant.Collections/List"
    ),
    "get_collection": EndpointAccess(
        True,
        True,
        True,
        "GET /collections/{collection_name}",
        "qdrant.Collections/Get",
        coll_rw_payload=False,
    ),
    "create_collection": EndpointAccess(
        False, False, True, "PUT /collections/{collection_name}", "qdrant.Collections/Create"
    ),
    "delete_collection": EndpointAccess(
        False, False, True, "DELETE /collections/{collection_name}", "qdrant.Collections/Delete"
    ),
    "update_collection_params": EndpointAccess(
        False, False, True, "PATCH /collections/{collection_name}", "qdrant.Collections/Update"
    ),
    "get_collection_cluster_info": EndpointAccess(
        True,
        True,
        True,
        "GET /collections/{collection_name}/cluster",
        "qdrant.Collections/CollectionClusterInfo",
        coll_rw_payload=False,
    ),
    "collection_exists": EndpointAccess(
        True,
        True,
        True,
        "GET /collections/{collection_name}/exists",
        "qdrant.Collections/CollectionExists",
    ),
    "replicate_shard_operation": EndpointAccess(
        False,
        False,
        True,
        "POST /collections/{collection_name}/cluster",
        "qdrant.Collections/UpdateCollectionClusterSetup",
    ),
    "create_default_shard_key_operation": EndpointAccess(
        False,
        False,
        True,
        "POST /collections/{collection_name}/cluster",
        "qdrant.Collections/UpdateCollectionClusterSetup",
    ),
    "create_custom_shard_key_operation": EndpointAccess(
        False,
        False,
        True,
        "POST /collections/{collection_name}/cluster",
        "qdrant.Collections/UpdateCollectionClusterSetup",
    ),
    "drop_shard_key_operation": EndpointAccess(
        False,
        False,
        True,
        "POST /collections/{collection_name}/cluster",
        "qdrant.Collections/UpdateCollectionClusterSetup",
    ),
    "move_shard_operation": EndpointAccess(
        False,
        False,
        True,
        "POST /collections/{collection_name}/cluster",
        "qdrant.Collections/UpdateCollectionClusterSetup",
    ),
    "abort_shard_transfer_operation": EndpointAccess(
        False,
        False,
        True,
        "POST /collections/{collection_name}/cluster",
        "qdrant.Collections/UpdateCollectionClusterSetup",
    ),
    "drop_shard_replica_operation": EndpointAccess(
        False,
        False,
        True,
        "POST /collections/{collection_name}/cluster",
        "qdrant.Collections/UpdateCollectionClusterSetup",
    ),
    "restart_transfer_operation": EndpointAccess(
        False,
        False,
        True,
        "POST /collections/{collection_name}/cluster",
        "qdrant.Collections/UpdateCollectionClusterSetup",
    ),
    ### Aliases ###
    "create_alias": EndpointAccess(
        False,
        False,
        True,
        "POST /collections/aliases",
        "qdrant.Collections/UpdateAliases",
    ),
    "rename_alias": EndpointAccess(
        False,
        False,
        True,
        "POST /collections/aliases",
        "qdrant.Collections/UpdateAliases",
    ),
    "delete_alias": EndpointAccess(
        False,
        False,
        True,
        "POST /collections/aliases",
        "qdrant.Collections/UpdateAliases",
    ),
    "list_collection_aliases": EndpointAccess(
        True,
        True,
        True,
        "GET /collections/{collection_name}/aliases",
        "qdrant.Collections/ListCollectionAliases",
    ),
    "list_aliases": EndpointAccess(
        True, True, True, "GET /aliases", "qdrant.Collections/ListAliases"
    ),
    ### Shard Keys ###
    "create_default_shard_key": EndpointAccess(
        False,
        False,
        True,
        "PUT /collections/{collection_name}/shards",
        "qdrant.Collections/CreateShardKey",
    ),
    "create_custom_shard_key": EndpointAccess(
        False,
        False,
        True,
        "PUT /collections/{collection_name}/shards",
        "qdrant.Collections/CreateShardKey",
    ),
    "delete_shard_key": EndpointAccess(
        False,
        False,
        True,
        "POST /collections/{collection_name}/shards/delete",
        "qdrant.Collections/DeleteShardKey",
    ),
    ### Payload Indexes ###
    "create_index": EndpointAccess(
        False,
        True,
        True,
        "PUT /collections/{collection_name}/index",
        "qdrant.Points/CreateFieldIndex",
        coll_rw_payload=False,
    ),
    "delete_index": EndpointAccess(
        False,
        True,
        True,
        "DELETE /collections/{collection_name}/index/{field_name}",
        "qdrant.Points/DeleteFieldIndex",
        coll_rw_payload=False,
    ),
    ### Collection Snapshots ###
    "list_collection_snapshots": EndpointAccess(
        True,
        True,
        True,
        "GET /collections/{collection_name}/snapshots",
        "qdrant.Snapshots/List",
        coll_rw_payload=False,
    ),
    "create_collection_snapshot": EndpointAccess(
        False,
        True,
        True,
        "POST /collections/{collection_name}/snapshots",
        "qdrant.Snapshots/Create",
        coll_rw_payload=False,
    ),
    "delete_collection_snapshot": EndpointAccess(
        False,
        True,
        True,
        "DELETE /collections/{collection_name}/snapshots/{snapshot_name}",
        "qdrant.Snapshots/Delete",
        coll_rw_payload=False,
    ),
    "download_collection_snapshot": EndpointAccess(
        True,
        True,
        True,
        "GET /collections/{collection_name}/snapshots/{snapshot_name}",
        coll_rw_payload=False,
    ),
    "upload_collection_snapshot": EndpointAccess(
        False, False, True, "POST /collections/{collection_name}/snapshots/upload"
    ),
    "recover_collection_snapshot": EndpointAccess(
        False,
        False,
        True,
        "PUT /collections/{collection_name}/snapshots/recover",
    ),
    ### Shard Snapshots ###
    "upload_shard_snapshot": EndpointAccess(
        False,
        False,
        True,
        "POST /collections/{collection_name}/shards/{shard_id}/snapshots/upload",
    ),
    "recover_shard_snapshot": EndpointAccess(
        False,
        False,
        True,
        "PUT /collections/{collection_name}/shards/{shard_id}/snapshots/recover",
    ),
    "create_shard_snapshot": EndpointAccess(
        False,
        True,
        True,
        "POST /collections/{collection_name}/shards/{shard_id}/snapshots",
        coll_rw_payload=False,
    ),
    "list_shard_snapshots": EndpointAccess(
        True,
        True,
        True,
        "GET /collections/{collection_name}/shards/{shard_id}/snapshots",
        coll_rw_payload=False,
    ),
    "delete_shard_snapshot": EndpointAccess(
        False,
        True,
        True,
        "DELETE /collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}",
        coll_rw_payload=False,
    ),
    "download_shard_snapshot": EndpointAccess(
        True,
        True,
        True,
        "GET /collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}",
        coll_rw_payload=False,
    ),
    ### Full Snapshots ###
    "list_full_snapshots": EndpointAccess(
        True,
        False,
        True,
        "GET /snapshots",
        "qdrant.Snapshots/ListFull",
        coll_r=False,
    ),
    "create_full_snapshot": EndpointAccess(
        False, False, True, "POST /snapshots", "qdrant.Snapshots/CreateFull"
    ),
    "delete_full_snapshot": EndpointAccess(
        False, False, True, "DELETE /snapshots/{snapshot_name}", "qdrant.Snapshots/DeleteFull"
    ),
    "download_full_snapshot": EndpointAccess(
        True, False, True, "GET /snapshots/{snapshot_name}", coll_r=False
    ),
    ### Cluster ###
    "get_cluster": EndpointAccess(True, False, True, "GET /cluster", coll_r=False),
    "recover_raft_state": EndpointAccess(False, False, True, "POST /cluster/recover"),
    "delete_peer": EndpointAccess(False, False, True, "DELETE /cluster/peer/{peer_id}"),
    ### Points ###
    "get_point": EndpointAccess(
        True,
        True,
        True,
        "GET /collections/{collection_name}/points/{id}",
        coll_rw_payload=False,
    ),
    "get_points": EndpointAccess(
        True,
        True,
        True,
        "POST /collections/{collection_name}/points",
        "qdrant.Points/Get",
        coll_rw_payload=False,
    ),
    "upsert_points": EndpointAccess(
        False,
        True,
        True,
        "PUT /collections/{collection_name}/points",
        "qdrant.Points/Upsert",
        coll_rw_payload=False,
    ),
    "update_points_batch": EndpointAccess(
        False,
        True,
        True,
        "POST /collections/{collection_name}/points/batch",
        "qdrant.Points/UpdateBatch",
        coll_rw_payload=False,
    ),
    "delete_points": EndpointAccess(
        False,
        True,
        True,
        "POST /collections/{collection_name}/points/delete",
        "qdrant.Points/Delete",
    ),
    "update_vectors": EndpointAccess(
        False,
        True,
        True,
        "PUT /collections/{collection_name}/points/vectors",
        "qdrant.Points/UpdateVectors",
        coll_rw_payload=False,
    ),
    "delete_vectors": EndpointAccess(
        False,
        True,
        True,
        "POST /collections/{collection_name}/points/vectors/delete",
        "qdrant.Points/DeleteVectors",
    ),
    "set_payload": EndpointAccess(
        False,
        True,
        True,
        "POST /collections/{collection_name}/points/payload",
        "qdrant.Points/SetPayload",
        coll_rw_payload=False,
    ),
    "overwrite_payload": EndpointAccess(
        False,
        True,
        True,
        "PUT /collections/{collection_name}/points/payload",
        "qdrant.Points/OverwritePayload",
        coll_rw_payload=False,
    ),
    "delete_payload": EndpointAccess(
        False,
        True,
        True,
        "POST /collections/{collection_name}/points/payload/delete",
        "qdrant.Points/DeletePayload",
        coll_rw_payload=False,
    ),
    "clear_payload": EndpointAccess(
        False,
        True,
        True,
        "POST /collections/{collection_name}/points/payload/clear",
        "qdrant.Points/ClearPayload",
        coll_rw_payload=False,
    ),
    "scroll_points": EndpointAccess(
        True,
        True,
        True,
        "POST /collections/{collection_name}/points/scroll",
        "qdrant.Points/Scroll",
    ),
    "search_points": EndpointAccess(
        True,
        True,
        True,
        "POST /collections/{collection_name}/points/search",
        "qdrant.Points/Search",
    ),
    "search_points_batch": EndpointAccess(
        True,
        True,
        True,
        "POST /collections/{collection_name}/points/search/batch",
        "qdrant.Points/SearchBatch",
    ),
    "search_point_groups": EndpointAccess(
        True,
        True,
        True,
        "POST /collections/{collection_name}/points/search/groups",
        "qdrant.Points/SearchGroups",
    ),
    "recommend_points": EndpointAccess(
        True,
        True,
        True,
        "POST /collections/{collection_name}/points/recommend",
        "qdrant.Points/Recommend",
        coll_rw_payload=False,
    ),
    "recommend_points_batch": EndpointAccess(
        True,
        True,
        True,
        "POST /collections/{collection_name}/points/recommend/batch",
        "qdrant.Points/RecommendBatch",
        coll_rw_payload=False,
    ),
    "recommend_point_groups": EndpointAccess(
        True,
        True,
        True,
        "POST /collections/{collection_name}/points/recommend/groups",
        "qdrant.Points/RecommendGroups",
        coll_rw_payload=False,
    ),
    "discover_points": EndpointAccess(
        True,
        True,
        True,
        "POST /collections/{collection_name}/points/discover",
        "qdrant.Points/Discover",
        coll_rw_payload=False,
    ),
    "discover_points_batch": EndpointAccess(
        True,
        True,
        True,
        "POST /collections/{collection_name}/points/discover/batch",
        "qdrant.Points/DiscoverBatch",
        coll_rw_payload=False,
    ),
    "count_points": EndpointAccess(
        True, True, True, "POST /collections/{collection_name}/points/count", "qdrant.Points/Count"
    ),
    ### Service ###
    "root": EndpointAccess(True, True, True, "GET /", "qdrant.Qdrant/HealthCheck"),
    "readyz": EndpointAccess(True, True, True, "GET /readyz", "grpc.health.v1.Health/Check"),
    "healthz": EndpointAccess(True, True, True, "GET /healthz", "grpc.health.v1.Health/Check"),
    "livez": EndpointAccess(True, True, True, "GET /livez", "grpc.health.v1.Health/Check"),
    "telemetry": EndpointAccess(True, False, True, "GET /telemetry", coll_r=False),
    "metrics": EndpointAccess(True, False, True, "GET /metrics", coll_r=False),
    "post_locks": EndpointAccess(False, False, True, "POST /locks"),
    "get_locks": EndpointAccess(True, False, True, "GET /locks", coll_r=False),
}


def test_all_actions_have_tests():
    # for each action
    for action_name in ACTION_ACCESS.keys():
        # a test_{action_name} exists in this file
        test_name = f"test_{action_name}"
        assert (
            test_name in globals()
        ), f"An action is not tested: `{test_name}` was not found in this file"


def test_all_rest_endpoints_are_covered():
    # Load the JSON content from the openapi.json file
    with open("./docs/redoc/master/openapi.json", "r") as file:
        openapi_data = json.load(file)

    # Extract all endpoint paths
    endpoint_paths = []
    for path in openapi_data["paths"].keys():
        for method in openapi_data["paths"][path]:
            method = method.upper()
            endpoint_paths.append(f"{method} {path}")

    # check that all endpoints are covered in ACTION_ACCESS
    covered_endpoints = set(v.rest_endpoint for v in ACTION_ACCESS.values())
    for endpoint in endpoint_paths:
        assert (
            endpoint in covered_endpoints
        ), f"REST endpoint `{endpoint}` not found in any of the `ACTION_ACCESS` REST endpoints"


class MetadataInterceptor(ClientInterceptor):
    """A test interceptor that injects invocation metadata."""

    def __init__(self, metadata: List[Tuple[str, str]]):
        self._metadata = metadata

    def intercept(self, method, request_or_iterator, call_details: ClientCallDetails):
        """Add invocation metadata to request."""
        new_details = call_details._replace(metadata=self._metadata)
        return method(request_or_iterator, new_details)


def test_all_grpc_endpoints_are_covered():
    # read grpc services from the reflection server
    client: grpc_requests.Client = grpc_requests.Client(
        GRPC_URI, interceptors=[MetadataInterceptor(API_KEY_METADATA)]
    )

    # check that all endpoints are covered in ACTION_ACCESS
    covered_endpoints = set(v.grpc_endpoint for v in ACTION_ACCESS.values())

    for service_name in client.service_names:
        service = client.service(service_name)
        for method in service.method_names:
            grpc_endpoint = f"{service_name}/{method}"
            assert (
                grpc_endpoint in covered_endpoints
            ), f"gRPC endpoint `{grpc_endpoint}` not found in ACTION_ACCESS gRPC endpoints"


def check_rest_access(
    method: str,
    path: str,
    body: Optional[Union[dict, Callable[[], dict]]],
    should_succeed: bool,
    token: str,
    path_params: dict = {},
    request_kwargs: dict = {},
):
    if isfunction(body):
        body = body()

    concrete_path_params = {}
    for key, value in path_params.items():
        concrete_path_params[key] = value() if isfunction(value) else value

    path = path.format(**concrete_path_params)

    try:
        res = requests.request(
            method,
            f"{REST_URI}{path}",
            headers={"authorization": f"Bearer {token}"},
            json=body,
            **request_kwargs,
        )

        if should_succeed:
            assert (
                res.status_code < 500 and res.status_code != 403
            ), f"{method} {path} failed with {res.status_code}: {res.text}"
        else:
            assert (
                res.status_code == 403
            ), f"{method} {path} should've gotten `403` status code, but got `{res.status_code}: {res.text}`"

    except requests.exceptions.ConnectionError as e:
        # File upload requests might hang if we get an early response (like 401 or 403),
        # but `requests` does not handle those properly
        # https://github.com/psf/requests/issues/5425
        if should_succeed or not isinstance(e.args[0].args[1], TimeoutError):
            raise e


def check_grpc_access(
    client: grpc_requests.Client,
    service: str,
    method: str,
    request: Optional[dict],
    should_succeed: bool,
):
    if isfunction(request):
        request = request()

    try:
        _res = client.request(service=service, method=method, request=request)
    except grpc.RpcError as e:
        if should_succeed:
            if e.code() not in [grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.NOT_FOUND]:
                pytest.fail(f"{service}/{method} failed with {e.code()}: {e.details()}")
        else:
            assert (
                e.code() == grpc.StatusCode.PERMISSION_DENIED
            ), f"{service}/{method} should've gotten `PERMISSION_DENIED` status code, but got `{e.code()}: {e.details()}`"


class GrpcClients:
    def __init__(self):
        self.r = grpc_requests.Client(
            GRPC_URI, interceptors=[MetadataInterceptor([("authorization", f"Bearer {TOKEN_R}")])]
        )
        self.coll_r = grpc_requests.Client(
            GRPC_URI,
            interceptors=[MetadataInterceptor([("authorization", f"Bearer {TOKEN_COLL_R}")])],
        )
        self.coll_rw = grpc_requests.Client(
            GRPC_URI,
            interceptors=[MetadataInterceptor([("authorization", f"Bearer {TOKEN_COLL_RW}")])],
        )
        self.coll_rw_payload = grpc_requests.Client(
            GRPC_URI,
            interceptors=[
                MetadataInterceptor([("authorization", f"Bearer {TOKEN_COLL_RW_PAYLOAD}")])
            ],
        )
        self.coll_r_payload = grpc_requests.Client(
            GRPC_URI,
            interceptors=[
                MetadataInterceptor([("authorization", f"Bearer {TOKEN_COLL_R_PAYLOAD}")])
            ],
        )
        self.m = grpc_requests.Client(
            GRPC_URI, interceptors=[MetadataInterceptor([("authorization", f"Bearer {TOKEN_M}")])]
        )
        self.api_key = grpc_requests.Client(
            GRPC_URI, interceptors=[MetadataInterceptor(API_KEY_METADATA)]
        )
        self.read_only_api_key = grpc_requests.Client(
            GRPC_URI, interceptors=[MetadataInterceptor(READ_ONLY_API_KEY_METADATA)]
        )


def get_auth_grpc_clients() -> GrpcClients:
    global _cached_grpc_clients
    if _cached_grpc_clients is None:
        _cached_grpc_clients = GrpcClients()

    return _cached_grpc_clients


def check_access(
    action_name: str, rest_request=None, grpc_request=None, path_params={}, rest_req_kwargs={}
):
    action_access: EndpointAccess = ACTION_ACCESS[action_name]

    assert isinstance(action_access, EndpointAccess)

    ## Check Rest
    method, path = action_access.rest_endpoint.split(" ")

    allowed_for = action_access.access

    check_rest_access(
        method, path, rest_request, allowed_for.read, TOKEN_R, path_params, rest_req_kwargs
    )
    check_rest_access(
        method, path, rest_request, allowed_for.coll_r, TOKEN_COLL_R, path_params, rest_req_kwargs
    )
    check_rest_access(
        method,
        path,
        rest_request,
        allowed_for.coll_rw,
        TOKEN_COLL_RW,
        path_params,
        rest_req_kwargs,
    )
    check_rest_access(
        method,
        path,
        rest_request,
        allowed_for.coll_r_payload,
        TOKEN_COLL_R_PAYLOAD,
        path_params,
        rest_req_kwargs,
    )
    check_rest_access(
        method,
        path,
        rest_request,
        allowed_for.coll_rw_payload,
        TOKEN_COLL_RW_PAYLOAD,
        path_params,
        rest_req_kwargs,
    )
    check_rest_access(
        method, path, rest_request, allowed_for.manage, TOKEN_M, path_params, rest_req_kwargs
    )

    # Check that API key is the same as manage token
    check_rest_access(
        method,
        path,
        rest_request,
        allowed_for.manage,
        SECRET,
        path_params,
        rest_req_kwargs,
    )

    # Check that read-only API key is the same as read-only token
    check_rest_access(
        method,
        path,
        rest_request,
        allowed_for.read,
        READ_ONLY_API_KEY,
        path_params,
        rest_req_kwargs,
    )

    ## Check GRPC
    grpc_endpoint = action_access.grpc_endpoint
    if grpc_endpoint is not None:
        service, method = grpc_endpoint.split("/")

        allowed_for = action_access.access

        grpc = get_auth_grpc_clients()

        check_grpc_access(grpc.r, service, method, grpc_request, allowed_for.read)
        check_grpc_access(grpc.coll_r, service, method, grpc_request, allowed_for.coll_r)
        check_grpc_access(grpc.coll_rw, service, method, grpc_request, allowed_for.coll_rw)
        check_grpc_access(
            grpc.coll_r_payload, service, method, grpc_request, allowed_for.coll_r_payload
        )
        check_grpc_access(
            grpc.coll_rw_payload, service, method, grpc_request, allowed_for.coll_rw_payload
        )
        check_grpc_access(grpc.m, service, method, grpc_request, allowed_for.manage)

        # Check that API key is the same as manage token
        check_grpc_access(grpc.api_key, service, method, grpc_request, allowed_for.manage)

        # Check that read-only API key is the same as read-only token
        check_grpc_access(grpc.read_only_api_key, service, method, grpc_request, allowed_for.read)


def test_list_collections():
    check_access("list_collections")


def test_get_collection():
    check_access(
        "get_collection",
        grpc_request={"collection_name": COLL_NAME},
        path_params={"collection_name": COLL_NAME},
    )


def test_create_collection():
    coll_names = [random_str() for _ in range(MAX_CALLS_IN_CHECK_ACCESS)]

    coll_names_iter = iter(coll_names)

    def grpc_req():
        return {"collection_name": next(coll_names_iter)}

    try:
        check_access(
            "create_collection",
            rest_request={},
            grpc_request=grpc_req,
            path_params={"collection_name": lambda: next(coll_names_iter)},
        )

    finally:
        for collection_name in coll_names:
            requests.delete(f"{REST_URI}/collections/{collection_name}", headers=API_KEY_HEADERS)


def test_delete_collection():
    fake_name = random_str()
    check_access(
        "delete_collection",
        grpc_request={"collection_name": fake_name},
        path_params={"collection_name": fake_name},
    )


def test_update_collection_params():
    check_access(
        "update_collection_params",
        rest_request={},
        grpc_request={"collection_name": COLL_NAME},
        path_params={"collection_name": COLL_NAME},
    )


def test_create_alias():
    def req():
        return {
            "actions": [
                {
                    "create_alias": {
                        "collection_name": COLL_NAME,
                        "alias_name": random_str(),
                    }
                }
            ]
        }

    check_access(
        "create_alias",
        rest_request=req,
        grpc_request=req,
    )


def test_rename_alias():
    alias_names = [random_str() for _ in range(MAX_CALLS_IN_CHECK_ACCESS)]

    names_iter = iter(alias_names)

    def req():
        return {
            "actions": [
                {
                    "rename_alias": {
                        "old_alias_name": next(names_iter),
                        "new_alias_name": random_str(),
                    }
                }
            ]
        }

    check_access(
        "rename_alias",
        rest_request=req,
        grpc_request=req,
    )


def test_delete_alias():
    req = {"actions": [{"delete_alias": {"alias_name": random_str()}}]}

    check_access(
        "delete_alias",
        rest_request=req,
        grpc_request=req,
    )


def test_list_collection_aliases():
    check_access(
        "list_collection_aliases",
        grpc_request={"collection_name": COLL_NAME},
        path_params={"collection_name": COLL_NAME},
    )


def test_list_aliases():
    check_access("list_aliases")


def test_get_collection_cluster_info():
    check_access(
        "get_collection_cluster_info",
        path_params={"collection_name": COLL_NAME},
        grpc_request={"collection_name": COLL_NAME},
    )


def test_collection_exists():
    check_access(
        "collection_exists",
        path_params={"collection_name": COLL_NAME},
        grpc_request={"collection_name": COLL_NAME},
    )


def test_replicate_shard_operation():
    peer_ids = [PEER_ID + 5, PEER_ID + 3]
    replicate_shard = {
        "replicate_shard": {
            "shard_id": SHARD_ID,
            "from_peer_id": peer_ids[0],
            "to_peer_id": peer_ids[1],
        }
    }
    check_access(
        "replicate_shard_operation",
        rest_request=replicate_shard,
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            **replicate_shard,
        },
    )


@pytest.fixture
def new_shard_keys():
    new_shard_keys = [random_str() for _ in range(MAX_CALLS_IN_CHECK_ACCESS)]

    try:
        yield new_shard_keys

    finally:
        for shard_key in new_shard_keys:
            requests.post(
                f"{REST_URI}/collections/{COLL_NAME}/shards/delete",
                json={"shard_key": shard_key},
                headers=API_KEY_HEADERS,
            )


def test_create_default_shard_key_operation(new_shard_keys):
    keys_iter = iter(new_shard_keys)

    def rest_req():
        return {"create_sharding_key": {"shard_key": next(keys_iter)}}

    def grpc_req():
        return {
            "collection_name": COLL_NAME,
            "create_shard_key": {"shard_key": {"keyword": next(keys_iter)}},
        }

    check_access(
        "create_default_shard_key_operation",
        rest_request=rest_req,
        path_params={"collection_name": COLL_NAME},
        grpc_request=grpc_req,
    )


def test_create_custom_shard_key_operation(new_shard_keys):
    keys_iter = iter(new_shard_keys)

    def rest_req():
        return {"create_sharding_key": {"shard_key": next(keys_iter), "replication_factor": 3}}

    def grpc_req():
        return {
            "collection_name": COLL_NAME,
            "create_shard_key": {
                "shard_key": {"keyword": next(keys_iter)},
                "replication_factor": 3,
            },
        }

    check_access(
        "create_custom_shard_key_operation",
        rest_request=rest_req,
        path_params={"collection_name": COLL_NAME},
        grpc_request=grpc_req,
    )


def test_drop_shard_key_operation():
    check_access(
        "drop_shard_key_operation",
        rest_request={"drop_sharding_key": {"shard_key": random_str()}},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "delete_shard_key": {"shard_key": {"keyword": random_str()}},
        },
    )


def test_move_shard_operation():
    move_shard_op = {
        "move_shard": {
            "shard_id": SHARD_ID,
            "from_peer_id": PEER_ID + 6,
            "to_peer_id": PEER_ID + 7,
        }
    }
    check_access(
        "move_shard_operation",
        rest_request=move_shard_op,
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            **move_shard_op,
        },
    )


def test_abort_shard_transfer_operation():
    abort_shard_transfer = {
        "abort_transfer": {
            "shard_id": SHARD_ID,
            "from_peer_id": PEER_ID + 6,
            "to_peer_id": PEER_ID + 7,
        }
    }
    check_access(
        "abort_shard_transfer_operation",
        rest_request=abort_shard_transfer,
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            **abort_shard_transfer,
        },
    )


def test_drop_shard_replica_operation():
    drop_shard_replica = {
        "drop_replica": {
            "shard_id": SHARD_ID,
            "peer_id": PEER_ID + 6,
        }
    }
    check_access(
        "drop_shard_replica_operation",
        rest_request=drop_shard_replica,
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            **drop_shard_replica,
        },
    )


def test_restart_transfer_operation():
    check_access(
        "restart_transfer_operation",
        rest_request={
            "restart_transfer": {
                "shard_id": SHARD_ID,
                "from_peer_id": PEER_ID,
                "to_peer_id": PEER_ID,
                "method": "stream_records",
            }
        },
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "restart_transfer": {
                "shard_id": SHARD_ID,
                "from_peer_id": PEER_ID,
                "to_peer_id": PEER_ID,
                "method": 0,
            },
        },
    )


def test_create_default_shard_key(new_shard_keys):
    keys_iter = iter(new_shard_keys)

    def rest_req():
        return {"shard_key": next(keys_iter)}

    def grpc_req():
        return {
            "collection_name": COLL_NAME,
            "request": {"shard_key": {"keyword": next(keys_iter)}},
        }

    check_access(
        "create_default_shard_key",
        rest_request=rest_req,
        path_params={"collection_name": COLL_NAME},
        grpc_request=grpc_req,
    )


def test_create_custom_shard_key(new_shard_keys):
    keys_iter = iter(new_shard_keys)

    def rest_req():
        return {"shard_key": next(keys_iter), "replication_factor": 3}

    def grpc_req():
        return {
            "collection_name": COLL_NAME,
            "request": {"shard_key": {"keyword": next(keys_iter)}, "replication_factor": 3},
        }

    check_access(
        "create_custom_shard_key",
        rest_request=rest_req,
        path_params={"collection_name": COLL_NAME},
        grpc_request=grpc_req,
    )


def test_delete_shard_key():
    check_access(
        "delete_shard_key",
        rest_request={"shard_key": random_str()},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "request": {"shard_key": {"keyword": random_str()}},
        },
    )


def test_create_index():
    check_access(
        "create_index",
        rest_request={"field_name": FIELD_NAME, "field_schema": "keyword"},
        path_params={"collection_name": COLL_NAME},
        grpc_request={"collection_name": COLL_NAME, "field_name": FIELD_NAME, "field_type": 0},
    )


def test_delete_index():
    check_access(
        "delete_index",
        path_params={"collection_name": COLL_NAME, "field_name": "fake_field_name"},
        grpc_request={"collection_name": COLL_NAME, "field_name": "fake_field_name"},
    )


def test_list_collection_snapshots():
    check_access(
        "list_collection_snapshots",
        path_params={"collection_name": COLL_NAME},
        grpc_request={"collection_name": COLL_NAME},
    )


def test_create_collection_snapshot():
    check_access(
        "create_collection_snapshot",
        path_params={"collection_name": COLL_NAME},
        grpc_request={"collection_name": COLL_NAME},
    )


def test_delete_collection_snapshot():
    fake_name = random_str()
    check_access(
        "delete_collection_snapshot",
        path_params={
            "collection_name": COLL_NAME,
            "snapshot_name": fake_name,
        },
        grpc_request={"collection_name": COLL_NAME, "snapshot_name": fake_name},
    )


def test_download_collection_snapshot():
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/snapshots?wait=true",
        headers=API_KEY_HEADERS,
    )
    res.raise_for_status()
    filename = res.json()["result"]["name"]

    check_access(
        "download_collection_snapshot",
        path_params={"collection_name": COLL_NAME, "snapshot_name": filename},
    )


@pytest.fixture(scope="module")
def collection_snapshot():
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/snapshots?wait=true",
        headers=API_KEY_HEADERS,
    )
    res.raise_for_status()
    filename = res.json()["result"]["name"]

    res = requests.get(
        f"{REST_URI}/collections/{COLL_NAME}/snapshots/{filename}",
        headers=API_KEY_HEADERS,
    )
    res.raise_for_status()

    return res.content


def test_upload_collection_snapshot(collection_snapshot: bytes):
    check_access(
        "upload_collection_snapshot",
        rest_req_kwargs={"files": {"snapshot": collection_snapshot}, "timeout": 1},
        path_params={"collection_name": COLL_NAME},
    )


def test_recover_collection_snapshot(collection_snapshot: bytes):
    # Save file to temp file
    temp_file = tempfile.NamedTemporaryFile(suffix=".snapshot")
    temp_file.write(collection_snapshot)
    temp_file.seek(0)
    file = temp_file.name

    check_access(
        "recover_collection_snapshot",
        rest_request={"location": f"file://{file}"},
        path_params={"collection_name": COLL_NAME},
    )


@pytest.fixture(scope="module")
def shard_snapshot_name():
    res = requests.post(
        f"{REST_URI}/collections/{COLL_NAME}/shards/{SHARD_ID}/snapshots?wait=true",
        headers=API_KEY_HEADERS,
    )
    res.raise_for_status()
    return res.json()["result"]["name"]


@pytest.fixture(scope="module")
def shard_snapshot(shard_snapshot_name):
    res = requests.get(
        f"{REST_URI}/collections/{COLL_NAME}/shards/{SHARD_ID}/snapshots/{shard_snapshot_name}",
        headers=API_KEY_HEADERS,
    )
    res.raise_for_status()
    return res.content


def test_upload_shard_snapshot(shard_snapshot: bytes):
    check_access(
        "upload_shard_snapshot",
        rest_req_kwargs={"files": {"snapshot": shard_snapshot}, "timeout": 1},
        path_params={"collection_name": COLL_NAME, "shard_id": SHARD_ID},
    )


def test_recover_shard_snapshot(shard_snapshot_name: str):
    check_access(
        "recover_shard_snapshot",
        rest_request={"location": shard_snapshot_name},
        path_params={"collection_name": COLL_NAME, "shard_id": SHARD_ID},
    )


def test_list_shard_snapshots():
    check_access(
        "list_shard_snapshots",
        path_params={"collection_name": COLL_NAME, "shard_id": SHARD_ID},
    )


def test_create_shard_snapshot():
    check_access(
        "create_shard_snapshot",
        path_params={"collection_name": COLL_NAME, "shard_id": SHARD_ID},
    )


def test_delete_shard_snapshot():
    check_access(
        "delete_shard_snapshot",
        path_params={
            "collection_name": COLL_NAME,
            "shard_id": SHARD_ID,
            "snapshot_name": random_str(),
        },
        grpc_request={
            "collection_name": COLL_NAME,
            "shard_id": SHARD_ID,
            "snapshot_name": random_str(),
        },
    )


def test_download_shard_snapshot(shard_snapshot_name: str):
    check_access(
        "download_shard_snapshot",
        path_params={
            "collection_name": COLL_NAME,
            "shard_id": SHARD_ID,
            "snapshot_name": shard_snapshot_name,
        },
    )


def test_list_full_snapshots():
    check_access("list_full_snapshots")


def test_create_full_snapshot():
    check_access("create_full_snapshot")


def test_delete_full_snapshot():
    check_access(
        "delete_full_snapshot",
        path_params={"snapshot_name": random_str()},
        grpc_request={"snapshot_name": random_str()},
    )


def test_download_full_snapshot():
    check_access(
        "download_full_snapshot",
        path_params={"snapshot_name": random_str()},
    )


def test_get_cluster():
    check_access("get_cluster")


def test_recover_raft_state():
    check_access("recover_raft_state")


def test_delete_peer():
    check_access("delete_peer", path_params={"peer_id": "2000"})


def test_get_point():
    check_access(
        "get_point",
        path_params={"collection_name": COLL_NAME, "id": 1},
    )


def test_get_points():
    check_access(
        "get_points",
        rest_request={"ids": [1]},
        path_params={"collection_name": COLL_NAME},
        grpc_request={"collection_name": COLL_NAME, "ids": [{"num": 1}]},
    )


def test_upsert_points():
    check_access(
        "upsert_points",
        rest_request={"points": [{"id": 1, "vector": [1, 2, 3, 4]}], "shard_key": SHARD_KEY},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "points": [{"id": {"num": 1}, "vectors": {"vector": {"data": [1, 2, 3, 4]}}}],
            "shard_key_selector": {"shard_keys": [{"keyword": SHARD_KEY}]},
        },
    )


def test_update_points_batch():
    rest_operations = [
        {"upsert": {"shard_key": SHARD_KEY, "points": [{"id": 1, "vector": [1, 2, 3, 4]}]}},
        {"delete": {"shard_key": SHARD_KEY, "points": [3]}},
        {"set_payload": {"shard_key": SHARD_KEY, "points": [1], "payload": {"key": "value"}}},
        {
            "overwrite_payload": {
                "shard_key": SHARD_KEY,
                "points": [1],
                "payload": {"key": "value"},
            }
        },
        {"delete_payload": {"shard_key": SHARD_KEY, "points": [1], "keys": ["key"]}},
        {"clear_payload": {"shard_key": SHARD_KEY, "points": [1]}},
        {
            "update_vectors": {
                "shard_key": SHARD_KEY,
                "points": [{"id": 1, "vector": [1, 2, 3, 4]}],
            }
        },
        {"delete_vectors": {"shard_key": SHARD_KEY, "points": [2], "vector": [""]}},
    ]

    grpc_operations = [
        {
            "upsert": {
                **SHARD_KEY_SELECTOR,
                "points": [
                    {
                        "id": {"num": 1},
                        "vectors": {"vector": {"data": [1, 2, 3, 4]}},
                        "payload": {},
                    }
                ],
            }
        },
        {"delete_points": {**SHARD_KEY_SELECTOR, "points": {"points": {"ids": [{"num": 10}]}}}},
        {
            "set_payload": {
                **SHARD_KEY_SELECTOR,
                "points_selector": {"points": {"ids": [{"num": 11}]}},
                "payload": {"my_key": {"string_value": "value"}},
            }
        },
        {
            "overwrite_payload": {
                **SHARD_KEY_SELECTOR,
                "points_selector": {"points": {"ids": [{"num": 11}]}},
                "payload": {"my_key": {"string_value": "value"}},
            }
        },
        {
            "delete_payload": {
                **SHARD_KEY_SELECTOR,
                "points_selector": {"points": {"ids": [{"num": 11}]}},
                "keys": ["my_key"],
            }
        },
        {
            "update_vectors": {
                **SHARD_KEY_SELECTOR,
                "points": [{"id": {"num": 11}, "vectors": {"vector": {"data": [0, 1, 2, 3]}}}],
            }
        },
        {
            "delete_vectors": {
                **SHARD_KEY_SELECTOR,
                "points_selector": {"points": {"ids": [{"num": 11}]}},
                "vectors": {"names": ["my_name"]},
            }
        },
        {"delete_points": {**SHARD_KEY_SELECTOR, "points": {"points": {"ids": [{"num": 11}]}}}},
        {"clear_payload": {**SHARD_KEY_SELECTOR, "points": {"points": {"ids": [{"num": 11}]}}}},
    ]

    check_access(
        "update_points_batch",
        rest_request={"operations": rest_operations},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "operations": grpc_operations,
        },
    )


def test_delete_points():
    check_access(
        "delete_points",
        rest_request={"points": [3], "shard_key": SHARD_KEY},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "points": {"points": {"ids": [{"num": 3}]}},
            **SHARD_KEY_SELECTOR,
        },
    )


def test_update_vectors():
    check_access(
        "update_vectors",
        rest_request={"points": [{"id": 1, "vector": [1, 2, 3, 4]}], "shard_key": SHARD_KEY},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "points": [{"id": {"num": 1}, "vectors": {"vector": {"data": [1, 2, 3, 4]}}}],
            "shard_key_selector": {"shard_keys": [{"keyword": SHARD_KEY}]},
        },
    )


def test_delete_vectors():
    check_access(
        "delete_vectors",
        rest_request={"points": [2], "vector": [""], "shard_key": SHARD_KEY},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "points_selector": {"points": {"ids": [{"num": 2}]}},
            "vectors": {"names": [""]},
            **SHARD_KEY_SELECTOR,
        },
    )


def test_set_payload():
    check_access(
        "set_payload",
        rest_request={"points": [1], "payload": {"my_key": "value"}, "shard_key": SHARD_KEY},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "points_selector": {"points": {"ids": [{"num": 1}]}},
            "payload": {"my_key": {"string_value": "value"}},
            **SHARD_KEY_SELECTOR,
        },
    )


def test_overwrite_payload():
    check_access(
        "overwrite_payload",
        rest_request={"points": [1], "payload": {"my_key": "value"}, "shard_key": SHARD_KEY},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "points_selector": {"points": {"ids": [{"num": 1}]}},
            "payload": {"my_key": {"string_value": "value"}},
            **SHARD_KEY_SELECTOR,
        },
    )


def test_delete_payload():
    check_access(
        "delete_payload",
        rest_request={"points": [1], "keys": ["my_key"], "shard_key": SHARD_KEY},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "points_selector": {"points": {"ids": [{"num": 1}]}},
            "keys": ["my_key"],
            **SHARD_KEY_SELECTOR,
        },
    )


def test_clear_payload():
    check_access(
        "clear_payload",
        rest_request={"points": [1], "shard_key": SHARD_KEY},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "points": {"points": {"ids": [{"num": 1}]}},
            **SHARD_KEY_SELECTOR,
        },
    )


def test_scroll_points():
    check_access(
        "scroll_points",
        rest_request={"limit": 10},
        path_params={"collection_name": COLL_NAME},
        grpc_request={"collection_name": COLL_NAME, "limit": 10},
    )


def test_search_points():
    check_access(
        "search_points",
        rest_request={"vector": [1, 2, 3, 4], "limit": 10},
        path_params={"collection_name": COLL_NAME},
        grpc_request={"collection_name": COLL_NAME, "vector": [1, 2, 3, 4], "limit": 10},
    )


def test_search_points_batch():
    query = {"vector": [1, 2, 3, 4], "limit": 10}
    check_access(
        "search_points_batch",
        rest_request={"searches": [query]},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "search_points": [{"collection_name": COLL_NAME, **query}],
        },
    )


def test_search_point_groups():
    query = {"vector": [1, 2, 3, 4], "limit": 10, "group_by": FIELD_NAME, "group_size": 3}
    check_access(
        "search_point_groups",
        rest_request=query,
        path_params={"collection_name": COLL_NAME},
        grpc_request={"collection_name": COLL_NAME, **query},
    )


def test_recommend_points():
    check_access(
        "recommend_points",
        rest_request={"positive": [1], "limit": 10},
        path_params={"collection_name": COLL_NAME},
        grpc_request={"collection_name": COLL_NAME, "positive": [{"num": 1}], "limit": 10},
    )


def test_recommend_points_batch():
    check_access(
        "recommend_points_batch",
        rest_request={"searches": [{"positive": [1], "limit": 10}]},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "recommend_points": [
                {
                    "collection_name": COLL_NAME,
                    "positive": [{"num": 1}],
                    "limit": 10,
                }
            ],
        },
    )


def test_recommend_point_groups():
    check_access(
        "recommend_point_groups",
        rest_request={"positive": [1], "limit": 10, "group_by": FIELD_NAME, "group_size": 3},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "positive": [{"num": 1}],
            "limit": 10,
            "group_by": FIELD_NAME,
            "group_size": 3,
        },
    )


def test_discover_points():
    check_access(
        "discover_points",
        rest_request={"target": 1, "limit": 10},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "target": {"single": {"id": {"num": 1}}},
            "limit": 10,
        },
    )


def test_discover_points_batch():
    check_access(
        "discover_points_batch",
        rest_request={"searches": [{"target": 1, "limit": 10}]},
        path_params={"collection_name": COLL_NAME},
        grpc_request={
            "collection_name": COLL_NAME,
            "discover_points": [
                {
                    "collection_name": COLL_NAME,
                    "target": {"single": {"id": {"num": 1}}},
                    "limit": 10,
                }
            ],
        },
    )


def test_count_points():
    check_access(
        "count_points",
        rest_request={},
        path_params={"collection_name": COLL_NAME},
        grpc_request={"collection_name": COLL_NAME},
    )


def test_root():
    check_access("root")


def test_readyz():
    check_access("readyz")


def test_healthz():
    check_access("healthz")


def test_livez():
    check_access("livez")


def test_telemetry():
    check_access("telemetry")


def test_metrics():
    check_access("metrics")


def test_post_locks():
    check_access("post_locks", rest_request={"write": False})


def test_get_locks():
    check_access("get_locks")
