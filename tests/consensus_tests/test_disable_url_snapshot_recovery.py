import pathlib

import grpc
import pytest
import requests
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory, text_format

from .fixtures import create_collection, upsert_random_points
from .utils import *

COLLECTION_NAME = "test_collection"

# Hand-rolled projection of lib/api/src/grpc/proto/shard_snapshots_service.proto,
# the internal server does not expose gRPC reflection.
_schema = text_format.Parse('''
name: "shard_snapshots_url_test.proto"
package: "shard_snapshots_url_test"
syntax: "proto3"
message_type {
  name: "ShardSnapshotLocation"
  field { name: "url" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
}
message_type {
  name: "RecoverShardSnapshotRequest"
  field { name: "collection_name" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
  field { name: "shard_id" number: 2 label: LABEL_OPTIONAL type: TYPE_UINT32 }
  field {
    name: "snapshot_location" number: 3 label: LABEL_OPTIONAL type: TYPE_MESSAGE
    type_name: ".shard_snapshots_url_test.ShardSnapshotLocation"
  }
  field { name: "snapshot_priority" number: 4 label: LABEL_OPTIONAL type: TYPE_INT32 }
  field {
    name: "from_peer_id" number: 7 label: LABEL_OPTIONAL type: TYPE_UINT64
    proto3_optional: true oneof_index: 0
  }
  oneof_decl { name: "_from_peer_id" }
}
message_type {
  name: "RecoverSnapshotResponse"
  field { name: "time" number: 1 label: LABEL_OPTIONAL type: TYPE_DOUBLE }
}
''', descriptor_pb2.FileDescriptorProto())
_descriptor = descriptor_pool.DescriptorPool().Add(_schema)
ShardSnapshotLocation = message_factory.GetMessageClass(
    _descriptor.message_types_by_name["ShardSnapshotLocation"]
)
RecoverShardSnapshotRequest = message_factory.GetMessageClass(
    _descriptor.message_types_by_name["RecoverShardSnapshotRequest"]
)
RecoverSnapshotResponse = message_factory.GetMessageClass(
    _descriptor.message_types_by_name["RecoverSnapshotResponse"]
)

# `ShardSnapshotPriority` values
PRIORITY_SNAPSHOT = 1
PRIORITY_SHARD_TRANSFER = 3

UNKNOWN_PEER_ID = 4242

# Substring of the error the service returns when URL-based snapshot recovery
# is disabled through the `service.enable_snapshot_url_recovery=false` config.
BLOCKED_ERROR_MESSAGE = "Snapshot recovery from remote URLs is disabled"


def assert_url_recovery_blocked(response: requests.Response):
    assert response.status_code == 403, (
        f"Expected 403 Forbidden, got {response.status_code}: {response.text}"
    )
    error = response.json()["status"]["error"]
    assert BLOCKED_ERROR_MESSAGE in error, (
        f"Expected error message to contain {BLOCKED_ERROR_MESSAGE!r}, got: {error!r}"
    )


# Assert that disabling URL-based snapshot recovery through the
# `QDRANT__SERVICE__ENABLE_SNAPSHOT_URL_RECOVERY=false` environment variable is
# actually respected by every snapshot recovery endpoint that accepts a remote
# URL: the service must reject `http`/`https` locations with a `403 Forbidden`
# response before attempting any download.
def test_disable_url_snapshot_recovery(tmp_path: pathlib.Path):
    assert_project_root()

    peer_urls, _, _ = start_cluster(
        tmp_path,
        1,
        extra_env={"QDRANT__SERVICE__ENABLE_SNAPSHOT_URL_RECOVERY": "false"},
    )
    peer_url = peer_urls[0]

    create_collection(peer_url, collection=COLLECTION_NAME)

    # Arbitrary remote URLs; the service must reject them up front rather than
    # attempting any network request (the `invalid` TLD never resolves).
    remote_urls = [
        "http://example.invalid/snapshot.tar",
        "https://example.invalid/snapshot.tar",
    ]

    shard_id = 0

    # Full collection snapshot recovery
    for location in remote_urls:
        resp = requests.put(
            f"{peer_url}/collections/{COLLECTION_NAME}/snapshots/recover",
            json={"location": location},
        )
        assert_url_recovery_blocked(resp)

    # Shard snapshot recovery
    for location in remote_urls:
        resp = requests.put(
            f"{peer_url}/collections/{COLLECTION_NAME}/shards/{shard_id}/snapshots/recover",
            json={"location": location},
        )
        assert_url_recovery_blocked(resp)

    # Partial shard snapshot recovery from a peer URL
    for peer_recover_url in remote_urls:
        resp = requests.post(
            f"{peer_url}/collections/{COLLECTION_NAME}/shards/{shard_id}/snapshot/partial/recover_from",
            json={"peer_url": peer_recover_url},
        )
        assert_url_recovery_blocked(resp)


def _local_shard(peer_url: str, shard_id: int):
    info = get_collection_cluster_info(peer_url, COLLECTION_NAME)
    return next((s for s in info["local_shards"] if s["shard_id"] == shard_id), None)


def _has_active_shard_with_points(peer_url: str, shard_id: int, points_count: int) -> bool:
    shard = _local_shard(peer_url, shard_id)
    return (
        shard is not None
        and shard["state"] == "Active"
        and shard["points_count"] == points_count
    )


# The internal gRPC API must respect the switch as well. The only remote URL it
# may still download from is the REST API of the source peer in a snapshot shard
# transfer, so that snapshot transfers keep working.
def test_disable_url_snapshot_recovery_internal_grpc(tmp_path: pathlib.Path):
    assert_project_root()

    peer_urls, _, _ = start_cluster(
        tmp_path,
        2,
        extra_env={"QDRANT__SERVICE__ENABLE_SNAPSHOT_URL_RECOVERY": "false"},
    )

    create_collection(peer_urls[0], collection=COLLECTION_NAME)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_urls
    )
    upsert_random_points(peer_urls[0], 100, collection_name=COLLECTION_NAME)

    shard_id = 0
    source, target = sorted(
        range(len(peer_urls)), key=lambda i: _local_shard(peer_urls[i], shard_id) is None
    )
    source_url, target_url = peer_urls[source], peer_urls[target]
    source_peer_id = get_cluster_info(source_url)["peer_id"]
    target_peer_id = get_cluster_info(target_url)["peer_id"]

    channel = grpc.insecure_channel(f"127.0.0.1:{processes[target].p2p_port}")
    recover = channel.unary_unary(
        "/qdrant.ShardSnapshots/Recover",
        request_serializer=RecoverShardSnapshotRequest.SerializeToString,
        response_deserializer=RecoverSnapshotResponse.FromString,
    )

    source_snapshot_url = (
        f"{source_url}/collections/{COLLECTION_NAME}/shards/{shard_id}/snapshot"
    )
    blocked = [
        # Not a peer
        ("http://example.invalid/snapshot.tar", PRIORITY_SNAPSHOT, None),
        ("http://example.invalid/snapshot.tar", PRIORITY_SHARD_TRANSFER, source_peer_id),
        ("http://example.invalid/snapshot.tar", PRIORITY_SHARD_TRANSFER, None),
        # A peer, but not a shard transfer
        (source_snapshot_url, PRIORITY_SNAPSHOT, None),
        # A shard transfer, but from an unknown peer
        (source_snapshot_url, PRIORITY_SHARD_TRANSFER, UNKNOWN_PEER_ID),
    ]
    for url, priority, from_peer_id in blocked:
        request = RecoverShardSnapshotRequest(
            collection_name=COLLECTION_NAME,
            shard_id=shard_id,
            snapshot_location=ShardSnapshotLocation(url=url),
            snapshot_priority=priority,
        )
        if from_peer_id is not None:
            request.from_peer_id = from_peer_id

        with pytest.raises(grpc.RpcError) as error:
            recover(request, timeout=10)
        assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED, (
            f"{url} with priority {priority} from peer {from_peer_id}: "
            f"{error.value.code()} {error.value.details()}"
        )
        assert BLOCKED_ERROR_MESSAGE in error.value.details()

    # A snapshot shard transfer recovers from the REST API of the source peer
    replicate_shard(
        source_url, COLLECTION_NAME, shard_id, source_peer_id, target_peer_id, method="snapshot"
    )
    wait_for_collection_shard_transfers_count(source_url, COLLECTION_NAME, 0)
    wait_for(_has_active_shard_with_points, target_url, shard_id, 100)
