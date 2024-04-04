from inspect import isfunction
import json
import pathlib
import random
import string
import time
from typing import List, Optional, Tuple

import grpc
import grpc_requests
import pytest
import requests
from consensus_tests import fixtures
from grpc_interceptor import ClientCallDetails, ClientInterceptor

from .utils import encode_jwt, make_peer_folder, start_first_peer, wait_for


def random_str():
    return "".join(random.choices(string.ascii_lowercase, k=10))


SECRET = "my_top_secret_key"

API_KEY_HEADERS = {"Api-Key": SECRET}
API_KEY_METADATA = [("api-key", SECRET)]

COLL_NAME = "primary_test_collection"
DELETABLE_COLL_NAMES = [random_str() for _ in range(10)]
DELETABLE_ALIASES = [random_str() for _ in range(10)]
RENAMABLE_ALIASES = [random_str() for _ in range(10)]
DELETABLE_FIELD_INDEX = [random_str() for _ in range(10)]
MOVABLE_SHARD_IDS = [i + 2 for i in range(10)]
SHARD_ID = 1
SNAPSHOT_NAME = "test_snapshot"
POINT_ID = 0
FIELD_NAME = "test_field"
PEER_ID = 0
DELETABLE_SHARD_KEYS = [random_str() for _ in range(10)]
MOVABLE_SHARD_KEYS = [random_str() for _ in range(10)]
SHARD_KEY = "existing_shard_key"


class Access:
    def __init__(self, r, rw, m):
        self.read = r
        self.read_write = rw
        self.manage = m


class AccessStub:
    def __init__(
        self, read, read_write, manage, rest_req=None, grpc_req=None, collection_name=COLL_NAME
    ):
        self.access = Access(read, read_write, manage)
        self.rest_req = rest_req
        self.grpc_req = grpc_req
        self.collection_name = collection_name


def default_shard_key_config():
    return {"shard_key": random_str()}


def default_shard_key_config_grpc():
    return {
        "collection_name": COLL_NAME,
        "request": {"shard_key": {"keyword": random_str()}},
    }


def custom_shard_key_config():
    return {"shard_key": random_str(), "replication_factor": 3}


def custom_shard_key_config_grpc():
    return {
        "collection_name": COLL_NAME,
        "request": {"shard_key": {"keyword": random_str()}, "replication_factor": 3},
    }


def create_collection_req():
    return {
        "collection_name": random_str(),
    }


deletable_shard_key = iter(DELETABLE_SHARD_KEYS)


def delete_shard_key_req():
    return {"shard_key": next(deletable_shard_key)}


def delete_shard_key_req_grpc():
    return {
        "collection_name": COLL_NAME,
        "request": {"shard_key": {"keyword": next(deletable_shard_key)}},
    }


deletable_coll_names = iter(DELETABLE_COLL_NAMES)


def delete_collection_name():
    return next(deletable_coll_names)


def delete_collection_req_grpc():
    return {
        "collection_name": next(deletable_coll_names),
    }


def create_alias_req():
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


def create_alias_req_grpc():
    return create_alias_req()


renamable_aliases = iter(RENAMABLE_ALIASES)


def rename_alias_req():
    return {
        "actions": [
            {
                "rename_alias": {
                    "old_alias_name": next(renamable_aliases),
                    "new_alias_name": random_str(),
                }
            }
        ]
    }


def rename_alias_req_grpc():
    return rename_alias_req()


deletable_aliases = iter(DELETABLE_ALIASES)


def delete_alias_req():
    return {"actions": [{"delete_alias": {"alias_name": next(deletable_aliases)}}]}


def delete_alias_req_grpc():
    return delete_alias_req()


deletable_field_index = iter(DELETABLE_FIELD_INDEX)


def delete_index_req():
    return {"field_name": next(deletable_field_index)}


def delete_index_req_grpc():
    return {
        "collection_name": COLL_NAME,
        "field_name": next(deletable_field_index),
    }


### TABLE_OF_ACCESS ACTIONS ###

default_create_shard_key = AccessStub(
    False,
    True,
    True,
    default_shard_key_config,
    default_shard_key_config_grpc,
)
custom_create_shard_key = AccessStub(
    False, False, True, custom_shard_key_config, custom_shard_key_config_grpc
)
delete_shard_key = AccessStub(False, True, True, delete_shard_key_req, delete_shard_key_req_grpc)
list_collections = AccessStub(True, True, True)
get_collection = AccessStub(True, True, True, None, {"collection_name": COLL_NAME})
create_collection = AccessStub(
    False, False, True, {}, create_collection_req, collection_name=random_str
)
update_collection_params = AccessStub(False, False, True, {}, {"collection_name": COLL_NAME})
delete_collection = AccessStub(
    False, False, True, None, delete_collection_req_grpc, collection_name=delete_collection_name
)
create_alias = AccessStub(
    False,
    False,
    True,
    create_alias_req,
    create_alias_req_grpc,
)
rename_alias = AccessStub(
    False,
    False,
    True,
    rename_alias_req,
    rename_alias_req_grpc,
)
delete_alias = AccessStub(
    False,
    False,
    True,
    delete_alias_req,
    delete_alias_req_grpc,
)
create_index = AccessStub(
    False,
    True,
    True,
    {"field_name": FIELD_NAME, "field_schema": "keyword"},
    {"collection_name": COLL_NAME, "field_name": FIELD_NAME, "field_type": 0},
)
collection_exists = AccessStub(True, True, True, None, {"collection_name": COLL_NAME})
delete_index = AccessStub(
    False,
    True,
    True,
    delete_index_req,
    delete_index_req_grpc,
)
get_collection_cluster_info = AccessStub(
    True, True, True, None, {"collection_name": COLL_NAME}
)  # TODO: are these the expected permissions?
move_shard_operation = AccessStub(
    False,
    False,
    True,
    {
        "move_shard": {
            "shard_id": SHARD_ID,
            "from_peer_id": PEER_ID,
            "to_peer_id": PEER_ID + 1,
        }
    },
    {
        "collection_name": COLL_NAME,
        "move_shard": {
            "shard_id": SHARD_ID,
            "from_peer_id": PEER_ID,
            "to_peer_id": PEER_ID + 1,
        },
    },
)
replicate_shard_operation = AccessStub(
    False,
    False,
    True,
    {
        "replicate_shard": {
            "shard_id": SHARD_ID,
            "from_peer_id": PEER_ID,
            "to_peer_id": PEER_ID,
        }
    },
)
abort_shard_transfer_operation = AccessStub(
    False,
    False,
    True,
    {
        "abort_transfer": {
            "shard_id": SHARD_ID,
            "from_peer_id": PEER_ID,
            "to_peer_id": PEER_ID,
        }
    },
)
drop_shard_replica_operation = AccessStub(
    False,
    False,
    True,
    {
        "drop_replica": {
            "shard_id": SHARD_ID,
            "peer_id": PEER_ID,
        }
    },
)
default_create_shard_key_operation = AccessStub(
    False,
    True,
    True,
    {
        "create_sharding_key": default_shard_key_config,
    },
)
custom_create_shard_key_operation = AccessStub(
    False,
    False,
    True,
    {
        "create_sharding_key": custom_shard_key_config,
    },
)
drop_shard_key_operation = AccessStub(
    False,
    True,
    True,
    {
        "drop_sharding_key": {
            "shard_key": "a",
        }
    },
)
restart_transfer_operation = AccessStub(
    False,
    False,
    True,
    {
        "restart_transfer": {
            "shard_id": SHARD_ID,
            "from_peer_id": PEER_ID,
            "to_peer_id": PEER_ID,
            "method": "stream_records",
        }
    },
)
list_collection_aliases = AccessStub(True, True, True, None, {"collection_name": COLL_NAME})
list_aliases = AccessStub(False, False, True)


TABLE_OF_ACCESS = {
    # Collections
    "PUT /collections/{collection_name}/shards": [
        default_create_shard_key,
        custom_create_shard_key,
    ],
    "POST /collections/{collection_name}/shards/delete": [delete_shard_key],
    "GET /collections": [list_collections],
    "GET /collections/{collection_name}": [get_collection],
    "PUT /collections/{collection_name}": [create_collection],
    "PATCH /collections/{collection_name}": [update_collection_params],
    "DELETE /collections/{collection_name}": [delete_collection],
    "POST /collections/aliases": [create_alias, rename_alias, delete_alias],
    "PUT /collections/{collection_name}/index": [create_index],
    "GET /collections/{collection_name}/exists": [collection_exists],
    "DELETE /collections/{collection_name}/index/{field_name}": [delete_index],
    "GET /collections/{collection_name}/cluster": [get_collection_cluster_info],
    "POST /collections/{collection_name}/cluster": [ # TODO: prepare second peer for these tests
        # move_shard_operation,
        # replicate_shard_operation,
        # abort_shard_transfer_operation,
        # drop_shard_replica_operation,
        # restart_transfer_operation,
        # default_create_shard_key_operation,
        # custom_create_shard_key_operation,
        # drop_shard_key_operation
    ],
    "GET /collections/{collection_name}/aliases": [list_collection_aliases],
    # "GET /aliases": [list_aliases],
    # "POST /collections/{collection_name}/snapshots/upload": [False, False, True],
    # "PUT /collections/{collection_name}/snapshots/recover": [False, False, True],
    # "GET /collections/{collection_name}/snapshots": [False, True, True, None],
    # "POST /collections/{collection_name}/snapshots": [False, True, True],
    # "DELETE /collections/{collection_name}/snapshots/{snapshot_name}": [False, True, True],  #
    # "GET /collections/{collection_name}/snapshots/{snapshot_name}": [True, True, True, None],  #
    # "POST /collections/{collection_name}/shards/{shard_id}/snapshots/upload": [
    #     False,
    #     False,
    #     True,
    # ],  #
    # "PUT /collections/{collection_name}/shards/{shard_id}/snapshots/recover": [
    #     False,
    #     False,
    #     True,
    # ],  #
    # "GET /collections/{collection_name}/shards/{shard_id}/snapshots": [False, True, True, None],  #
    # "POST /collections/{collection_name}/shards/{shard_id}/snapshots": [False, True, True],  #
    # "DELETE /collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}": [
    #     False,
    #     True,
    #     True,
    # ],  #
    # "GET /collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}": [
    #     False,
    #     True,
    #     True,
    #     None,
    # ],  #
    # # Points
    # "GET /collections/{collection_name}/points/{id}": [True, True, True, None],
    # "POST /collections/{collection_name}/points": [True, True, True],
    # "PUT /collections/{collection_name}/points": [False, True, True],
    # "POST /collections/{collection_name}/points/delete": [False, True, True],
    # "PUT /collections/{collection_name}/points/vectors": [False, True, True],
    # "POST /collections/{collection_name}/points/vectors/delete": [False, True, True],
    # "POST /collections/{collection_name}/points/payload": [False, True, True],
    # "PUT /collections/{collection_name}/points/payload": [False, True, True],
    # "POST /collections/{collection_name}/points/payload/delete": [False, True, True],
    # "POST /collections/{collection_name}/points/payload/clear": [False, True, True],
    # "POST /collections/{collection_name}/points/batch": [False, True, True],
    # "POST /collections/{collection_name}/points/scroll": [True, True, True],
    # "POST /collections/{collection_name}/points/search": [True, True, True],
    # "POST /collections/{collection_name}/points/search/batch": [True, True, True],
    # "POST /collections/{collection_name}/points/search/groups": [True, True, True],
    # "POST /collections/{collection_name}/points/recommend": [True, True, True],
    # "POST /collections/{collection_name}/points/recommend/batch": [True, True, True],
    # "POST /collections/{collection_name}/points/recommend/groups": [True, True, True],
    # "POST /collections/{collection_name}/points/discover": [True, True, True],
    # "POST /collections/{collection_name}/points/discover/batch": [True, True, True],
    # "POST /collections/{collection_name}/points/count": [True, True, True],
    # # Cluster
    # "GET /cluster": [True, True, True, None],
    # "POST /cluster/recover": [False, False, True],
    # "DELETE /cluster/peer/{peer_id}": [False, False, True],
    # # Snapshots
    # "GET /snapshots": [False, False, True, None],
    # "POST /snapshots": [False, False, True],
    # "DELETE /snapshots/{snapshot_name}": [False, False, True],
    # "GET /snapshots/{snapshot_name}": [False, False, True],
    # # Service
    # "GET /": [True, True, True, None],
    # "GET /readyz": [True, True, True, None],
    # "GET /healthz": [True, True, True, None],
    # "GET /livez": [True, True, True, None],
    # "GET /telemetry": [False, False, True, None],
    # "GET /metrics": [False, False, True, None],
    # "POST /locks": [False, False, True],
    # "GET /locks": [True, True, True, None],
}

GRPC_TO_REST_MAPPING = {
    "/qdrant.Collections/Get": "GET /collections/{collection_name}",
    "/qdrant.Collections/List": "GET /collections",
    "/qdrant.Collections/Create": "PUT /collections/{collection_name}",
    "/qdrant.Collections/Update": "PATCH /collections/{collection_name}",
    "/qdrant.Collections/Delete": "DELETE /collections/{collection_name}",
    "/qdrant.Collections/UpdateAliases": "POST /collections/aliases",
    "/qdrant.Collections/ListCollectionAliases": "GET /collections/{collection_name}/aliases",
    # "/qdrant.Collections/ListAliases": "GET /aliases",
    "/qdrant.Collections/CollectionClusterInfo": "GET /collections/{collection_name}/cluster",
    "/qdrant.Collections/CollectionExists": "GET /collections/{collection_name}/exists",
    "/qdrant.Collections/UpdateCollectionClusterSetup": "POST /collections/{collection_name}/cluster",
    "/qdrant.Collections/CreateShardKey": "PUT /collections/{collection_name}/shards",
    "/qdrant.Collections/DeleteShardKey": "POST /collections/{collection_name}/shards/delete",
    # "/qdrant.Points/Upsert": "PUT /collections/{collection_name}/points",
    # "/qdrant.Points/Delete": "POST /collections/{collection_name}/points/delete",
    # "/qdrant.Points/Get": "POST /collections/{collection_name}/points",
    # "/qdrant.Points/UpdateVectors": "PUT /collections/{collection_name}/points/vectors",
    # "/qdrant.Points/DeleteVectors": "POST /collections/{collection_name}/points/vectors/delete",
    # "/qdrant.Points/SetPayload": "POST /collections/{collection_name}/points/payload",
    # "/qdrant.Points/OverwritePayload": "PUT /collections/{collection_name}/points/payload",
    # "/qdrant.Points/DeletePayload": "POST /collections/{collection_name}/points/payload/delete",
    # "/qdrant.Points/ClearPayload": "POST /collections/{collection_name}/points/payload/clear",
    "/qdrant.Points/CreateFieldIndex": "PUT /collections/{collection_name}/index",
    "/qdrant.Points/DeleteFieldIndex": "DELETE /collections/{collection_name}/index/{field_name}",
    # "/qdrant.Points/Search": "POST /collections/{collection_name}/points/search",
    # "/qdrant.Points/SearchBatch": "POST /collections/{collection_name}/points/search/batch",
    # "/qdrant.Points/SearchGroups": "POST /collections/{collection_name}/points/search/groups",
    # "/qdrant.Points/Scroll": "POST /collections/{collection_name}/points/scroll",
    # "/qdrant.Points/Recommend": "POST /collections/{collection_name}/points/recommend",
    # "/qdrant.Points/RecommendBatch": "POST /collections/{collection_name}/points/recommend/batch",
    # "/qdrant.Points/RecommendGroups": "POST /collections/{collection_name}/points/recommend/groups",
    # "/qdrant.Points/Discover": "POST /collections/{collection_name}/points/discover",
    # "/qdrant.Points/DiscoverBatch": "POST /collections/{collection_name}/points/discover/batch",
    # "/qdrant.Points/Count": "POST /collections/{collection_name}/points/count",
    # "/qdrant.Points/UpdateBatch": "POST /collections/{collection_name}/points/batch",
    # "/qdrant.ShardSnapshots/Create": "POST /collections/{collection_name}/shards/{shard_id}/snapshots",
    # "/qdrant.ShardSnapshots/List": "GET /collections/{collection_name}/shards/{shard_id}/snapshots",
    # "/qdrant.ShardSnapshots/Delete": "DELETE /collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}",
    # "/qdrant.ShardSnapshots/Recover": "PUT /collections/{collection_name}/shards/{shard_id}/snapshots/recover",
    # "/qdrant.Snapshots/Create": "POST /collections/{collection_name}/snapshots",
    # "/qdrant.Snapshots/List": "GET /collections/{collection_name}/snapshots",
    # "/qdrant.Snapshots/Delete": "DELETE /collections/{collection_name}/snapshots/{snapshot_name}",
    # "/qdrant.Snapshots/CreateFull": "POST /snapshots",
    # "/qdrant.Snapshots/ListFull": "GET /snapshots",
    # "/qdrant.Snapshots/DeleteFull": "DELETE /snapshots/{snapshot_name}",
    # "/qdrant.Qdrant/HealthCheck": "GET /healthz",
    # "/grpc.health.v1.Health/Check": "GET /healthz",
}


def test_grpc_to_rest_mapping():
    for rest_endpoint in GRPC_TO_REST_MAPPING.values():
        assert (
            rest_endpoint in TABLE_OF_ACCESS
        ), f"REST endpoint `{rest_endpoint}` not found in TABLE_OF_ACCESS"


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

    fixtures.create_collection(
        rest_uri,
        collection=COLL_NAME,
        sharding_method="custom",
        headers=API_KEY_HEADERS,
    )

    for coll_name in DELETABLE_COLL_NAMES:
        fixtures.create_collection(
            rest_uri,
            collection=coll_name,
            headers=API_KEY_HEADERS,
        )

    for shard_key in [SHARD_KEY, * DELETABLE_SHARD_KEYS, *MOVABLE_SHARD_KEYS]:
        requests.put(
            f"{rest_uri}/collections/{COLL_NAME}/shards",
            json={"shard_key": shard_key},
            headers=API_KEY_HEADERS,
        ).raise_for_status()

    for alias in [*DELETABLE_ALIASES, *RENAMABLE_ALIASES]:
        requests.post(
            f"{rest_uri}/collections/aliases",
            json={
                "actions": [
                    {
                        "create_alias": {
                            "collection_name": COLL_NAME,
                            "alias_name": alias,
                        }
                    }
                ]
            },
            headers=API_KEY_HEADERS,
        ).raise_for_status()

    for field_name in DELETABLE_FIELD_INDEX:
        requests.put(
            f"{rest_uri}/collections/{COLL_NAME}/index",
            json={"field_name": field_name, "field_schema": "keyword"},
            headers=API_KEY_HEADERS,
        ).raise_for_status()

    fixtures.upsert_random_points(
        rest_uri, 100, COLL_NAME, shard_key=SHARD_KEY, headers=API_KEY_HEADERS
    )

    # TODO: create fixtures for payload index, snapshots, shards, shard_key, alias,

    yield rest_uri, grpc_uri

    fixtures.drop_collection(rest_uri, COLL_NAME, headers=API_KEY_HEADERS)


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
    create_validation_collection(rest_uri, secondary_collection)

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

    fixtures.drop_collection(rest_uri, secondary_collection, headers=API_KEY_HEADERS)
    fixtures.drop_collection(rest_uri, secondary_collection, headers=API_KEY_HEADERS)


def test_access(uris: Tuple[str, str]):
    rest_uri, grpc_uri = uris

    # Global read access token
    token_read = encode_jwt({"access": "r"}, SECRET)

    # Collection read access token
    token_coll_r = encode_jwt({"access": [{"collections": [COLL_NAME], "access": "r"}]}, SECRET)

    # Collection read-write access token
    token_coll_rw = encode_jwt({"access": [{"collections": [COLL_NAME], "access": "rw"}]}, SECRET)

    # Global manage access token
    token_manage = encode_jwt({"access": "m"}, SECRET)

    # Check REST endpoints
    for endpoint, stubs in TABLE_OF_ACCESS.items():
        for stub in stubs:

            assert isinstance(stub, AccessStub)

            coll_name = (
                stub.collection_name()
                if isfunction(stub.collection_name)
                else stub.collection_name
            )

            uri = rest_uri
            method, path = endpoint.split(" ")
            path = path.format(
                collection_name=coll_name,
                shard_id=SHARD_ID,
                snapshot_name=SNAPSHOT_NAME,
                id=POINT_ID,
                field_name=FIELD_NAME,
                peer_id=PEER_ID,
            )
            allowed_for = stub.access
            body = stub.rest_req

            check_rest_access(uri, method, path, body, allowed_for.read, token_read)
            check_rest_access(uri, method, path, body, allowed_for.read, token_coll_r)
            check_rest_access(uri, method, path, body, allowed_for.read_write, token_coll_rw)
            check_rest_access(uri, method, path, body, allowed_for.manage, token_manage)

    # Check GRPC endpoints
    grpc_read = grpc_requests.Client(
        grpc_uri, interceptors=[MetadataInterceptor([("authorization", f"Bearer {token_read}")])]
    )

    grpc_coll_r = grpc_requests.Client(
        grpc_uri, interceptors=[MetadataInterceptor([("authorization", f"Bearer {token_coll_r}")])]
    )

    grpc_coll_rw = grpc_requests.Client(
        grpc_uri,
        interceptors=[MetadataInterceptor([("authorization", f"Bearer {token_coll_rw}")])],
    )

    grpc_manage = grpc_requests.Client(
        grpc_uri, interceptors=[MetadataInterceptor([("authorization", f"Bearer {token_manage}")])]
    )
    for grpc_endpoint, rest_endpoint in GRPC_TO_REST_MAPPING.items():
        stubs = TABLE_OF_ACCESS[rest_endpoint]

        for stub in stubs:
            assert isinstance(stub, AccessStub)

            service = grpc_endpoint.split("/")[1]
            method = grpc_endpoint.split("/")[2]

            allowed_for = stub.access
            request = stub.grpc_req

            check_grpc_access(grpc_read, service, method, request, allowed_for.read)
            check_grpc_access(grpc_coll_r, service, method, request, allowed_for.read)
            check_grpc_access(grpc_coll_rw, service, method, request, allowed_for.read_write)
            check_grpc_access(grpc_manage, service, method, request, allowed_for.manage)


def check_rest_access(
    uri: str, method: str, path: str, body: Optional[dict], should_succeed: bool, token: str
):
    if isfunction(body):
        body = body()

    res = requests.request(
        method, f"{uri}{path}", headers={"authorization": f"Bearer {token}"}, json=body
    )

    if should_succeed:
        assert res.ok, f"{method} {path} failed with {res.status_code}: {res.text}"
    else:
        assert res.status_code in [
            401,
            403,
        ], f"{method} {path} failed with {res.status_code}: {res.text}"


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
            pytest.fail(f"{service}/{method} failed with {e.code()}: {e.details()}")
        else:
            assert e.code() == grpc.StatusCode.PERMISSION_DENIED
