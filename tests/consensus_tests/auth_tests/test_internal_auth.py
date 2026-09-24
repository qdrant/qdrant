"""The internal (p2p) gRPC API must only accept the read-write API keys.

A read-only key or a JWT authenticates on the public API, but the `Raft`
service does not check `Access` per request. If those credentials passed the
internal auth layer, anyone holding a read-only key could attach a node to
consensus. These tests call the `Raft` service on the p2p port directly.
"""

import grpc
import pytest
import requests
from consensus_tests.utils import kill_all_processes, start_cluster
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory, text_format

from .utils import ALT_SECRET, API_KEY_HEADERS, READ_ONLY_API_KEY, SECRET, encode_jwt

# Hand-rolled projection of lib/api/src/grpc/proto/raft_service.proto,
# the internal server does not expose gRPC reflection.
_schema = text_format.Parse('''
name: "raft_auth_test.proto"
package: "raft_auth_test"
syntax: "proto3"
message_type {
  name: "PeerId"
  field { name: "id" number: 1 label: LABEL_OPTIONAL type: TYPE_UINT64 }
}
message_type {
  name: "Uri"
  field { name: "uri" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
}
message_type {
  name: "AddPeerToKnownMessage"
  field { name: "uri" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING proto3_optional: true oneof_index: 0 }
  field { name: "port" number: 2 label: LABEL_OPTIONAL type: TYPE_UINT32 proto3_optional: true oneof_index: 1 }
  field { name: "id" number: 3 label: LABEL_OPTIONAL type: TYPE_UINT64 }
  oneof_decl { name: "_uri" }
  oneof_decl { name: "_port" }
}
message_type {
  name: "Peer"
  field { name: "uri" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
  field { name: "id" number: 2 label: LABEL_OPTIONAL type: TYPE_UINT64 }
}
message_type {
  name: "AllPeers"
  field { name: "all_peers" number: 1 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".raft_auth_test.Peer" }
  field { name: "first_peer_id" number: 2 label: LABEL_OPTIONAL type: TYPE_UINT64 }
}
''', descriptor_pb2.FileDescriptorProto())
_descriptor = descriptor_pool.DescriptorPool().Add(_schema)
PeerId = message_factory.GetMessageClass(_descriptor.message_types_by_name["PeerId"])
Uri = message_factory.GetMessageClass(_descriptor.message_types_by_name["Uri"])
AddPeerToKnownMessage = message_factory.GetMessageClass(
    _descriptor.message_types_by_name["AddPeerToKnownMessage"]
)
AllPeers = message_factory.GetMessageClass(_descriptor.message_types_by_name["AllPeers"])

ROGUE_PEER_ID = 4242


@pytest.fixture(scope="module")
def internal_auth_cluster(tmp_path_factory: pytest.TempPathFactory):
    tmp_path = tmp_path_factory.mktemp("internal_auth_cluster")

    # Ports come from the per-worker slice rather than the fixed auth-test
    # window: that window only has room for a single peer.
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path,
        num_peers=2,
        extra_env={
            "QDRANT__SERVICE__API_KEY": SECRET,
            "QDRANT__SERVICE__ALT_API_KEY": ALT_SECRET,
            "QDRANT__SERVICE__READ_ONLY_API_KEY": READ_ONLY_API_KEY,
            "QDRANT__SERVICE__JWT_RBAC": "true",
            "QDRANT__SERVICE__ENFORCE_INTERNAL_AUTH": "true",
        },
        headers=API_KEY_HEADERS,
    )

    try:
        yield peer_api_uris, peer_dirs, bootstrap_uri
    finally:
        kill_all_processes()


def _p2p_target(bootstrap_uri: str) -> str:
    return bootstrap_uri.removeprefix("http://")


def _raft_stub(target: str):
    channel = grpc.insecure_channel(target)
    return {
        "who_is": channel.unary_unary(
            "/qdrant.Raft/WhoIs",
            request_serializer=PeerId.SerializeToString,
            response_deserializer=Uri.FromString,
        ),
        "add_peer_to_known": channel.unary_unary(
            "/qdrant.Raft/AddPeerToKnown",
            request_serializer=AddPeerToKnownMessage.SerializeToString,
            response_deserializer=AllPeers.FromString,
        ),
    }


def _cluster_peer_ids(rest_uri: str) -> set:
    resp = requests.get(f"{rest_uri}/cluster", headers=API_KEY_HEADERS)
    resp.raise_for_status()
    return set(int(peer_id) for peer_id in resp.json()["result"]["peers"].keys())


def _assert_unauthenticated(call, request, metadata):
    with pytest.raises(grpc.RpcError) as exc:
        call(request, metadata=metadata, timeout=10)
    assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED, exc.value.details()


@pytest.mark.parametrize(
    "metadata",
    [
        pytest.param([], id="no-key"),
        pytest.param([("api-key", READ_ONLY_API_KEY)], id="read-only-key"),
        pytest.param([("authorization", f"Bearer {READ_ONLY_API_KEY}")], id="read-only-bearer"),
        pytest.param([("api-key", "wrong-key")], id="wrong-key"),
        pytest.param(
            [("authorization", f"Bearer {encode_jwt({'access': 'm'}, SECRET)}")],
            id="jwt-manage",
        ),
        pytest.param(
            [("authorization", f"Bearer {encode_jwt({'access': 'r'}, SECRET)}")],
            id="jwt-read",
        ),
    ],
)
def test_internal_api_rejects_non_read_write_credentials(internal_auth_cluster, metadata):
    peer_api_uris, _, bootstrap_uri = internal_auth_cluster
    stub = _raft_stub(_p2p_target(bootstrap_uri))
    peers_before = _cluster_peer_ids(peer_api_uris[0])

    _assert_unauthenticated(stub["who_is"], PeerId(id=next(iter(peers_before))), metadata)
    _assert_unauthenticated(
        stub["add_peer_to_known"],
        AddPeerToKnownMessage(id=ROGUE_PEER_ID, uri="http://127.0.0.1:1"),
        metadata,
    )

    assert _cluster_peer_ids(peer_api_uris[0]) == peers_before
    assert ROGUE_PEER_ID not in peers_before


@pytest.mark.parametrize(
    "metadata",
    [
        pytest.param([("api-key", SECRET)], id="api-key"),
        pytest.param([("api-key", ALT_SECRET)], id="alt-api-key"),
        pytest.param([("authorization", f"Bearer {SECRET}")], id="api-key-bearer"),
    ],
)
def test_internal_api_accepts_read_write_keys(internal_auth_cluster, metadata):
    peer_api_uris, _, bootstrap_uri = internal_auth_cluster
    stub = _raft_stub(_p2p_target(bootstrap_uri))
    peer_id = next(iter(_cluster_peer_ids(peer_api_uris[0])))

    response = stub["who_is"](PeerId(id=peer_id), metadata=metadata, timeout=10)
    assert response.uri


def test_cluster_forms_with_internal_auth_enforced(internal_auth_cluster):
    peer_api_uris, _, _ = internal_auth_cluster

    # Both peers joined consensus through the enforced internal API using the
    # forwarded read-write key.
    assert len(_cluster_peer_ids(peer_api_uris[0])) == 2
