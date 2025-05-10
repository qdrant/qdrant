import pathlib
import requests
from typing import Any

from .assertions import *
from .fixtures import *
from .utils import *

COLLECTION = "test_collection"
SHARD = 0

@pytest.mark.parametrize(
    "bootstrap_points, recover_read, upsert_points",
    [
        (0,   True,  0),
        (100, True,  0),
        (0,   True,  5),
        (100, True,  5),
        (0,   False, 0),
        (100, False, 0),
    ]
)
def test_partial_snapshot(tmp_path: pathlib.Path, bootstrap_points: int, recover_read: bool, upsert_points: int):
    assert_project_root()

    write_peer, read_peer = bootstrap_peers(tmp_path, bootstrap_points, recover_read)

    if upsert_points > 0:
        upsert(write_peer, upsert_points, offset = bootstrap_points)

    recover_partial_snapshot_from(read_peer, write_peer)
    assert_consistency(read_peer, write_peer)


def bootstrap_peers(tmp: pathlib.Path, bootstrap_points = 0, recover_read = False):
    write_peer = bootstrap_write_peer(tmp, bootstrap_points)
    read_peer = bootstrap_read_peer(tmp, write_peer if recover_read else None)
    return write_peer, read_peer

def bootstrap_write_peer(tmp: pathlib.Path, bootstrap_points = 0):
    write_peer = bootstrap_peer(tmp / "write", 6331)
    bootstrap_collection(write_peer, bootstrap_points)
    return write_peer

def bootstrap_read_peer(tmp: pathlib.Path, recover_from_url: str | None = None):
    read_peer = bootstrap_peer(tmp / "read", 63331)

    if recover_from_url is None:
        bootstrap_collection(read_peer)
    else:
        recover_collection(read_peer, recover_from_url)

    return read_peer

def bootstrap_peer(path: pathlib.Path, port: int):
    path.mkdir()

    config = {
        "QDRANT__LOG_LEVEL": "debug,collection::common::file_utils=trace",
        "QDRANT__FEATURE_FLAGS__USE_MUTABLE_ID_TRACKER_WITHOUT_ROCKSDB": "true",
    }

    uris, _, _ = start_cluster(path, 1, port_seed = port, extra_env = config)

    return uris[0]

def bootstrap_collection(peer_url, bootstrap_points = 0):
    create_collection(peer_url, shard_number = 1, replication_factor = 1, indexing_threshold = 1000000, sparse_vectors = False)
    wait_collection_exists_and_active_on_all_peers(COLLECTION, [peer_url])

    if bootstrap_points > 0:
        upsert(peer_url, bootstrap_points)

def recover_collection(peer_url: str, recover_from_url: str):
    snapshot_url = create_collection_snapshot(recover_from_url)
    recover_collection_snapshot(peer_url, snapshot_url)

    assert_point_consistency(peer_url, recover_from_url)

def create_collection_snapshot(peer_url: str):
    resp = requests.post(f"{peer_url}/collections/{COLLECTION}/snapshots")
    assert_http_ok(resp)

    snapshot_name = resp.json()["result"]["name"]
    snapshot_url = f"{peer_url}/collections/{COLLECTION}/snapshots/{snapshot_name}"
    return snapshot_url

def recover_collection_snapshot(peer_url: str, snapshot_url: str):
    resp = requests.put(
        f"{peer_url}/collections/{COLLECTION}/snapshots/recover",
        json = { "location": snapshot_url },
    )
    assert_http_ok(resp)

    return resp.json()["result"]

def recover_partial_snapshot_from(peer_url: str, recover_peer_url: str):
    resp = requests.post(
        f"{peer_url}/collections/{COLLECTION}/shards/{SHARD}/snapshot/partial/recover_from",
        json = { "peer_url": recover_peer_url },
    )
    assert_http_ok(resp)

    return resp.json()["result"]

def get_snapshot_manifest(peer_url: str):
    resp = requests.get(f"{peer_url}/collections/{COLLECTION}/shards/{SHARD}/snapshot/partial/manifest")
    assert_http_ok(resp)

    return resp.json()["result"]


def assert_consistency(write_peer: str, read_peer: str):
    assert_files_consistency(write_peer, read_peer)
    assert_point_consistency(write_peer, read_peer)

def assert_files_consistency(write_peer: str, read_peer: str):
    assert discard_file_versions(get_snapshot_manifest(write_peer)) == discard_file_versions(get_snapshot_manifest(read_peer))

def discard_file_versions(snapshot_manifest: Any):
    for _, segment_manifest in snapshot_manifest.items():
        segment_manifest['files'] = set(segment_manifest.pop('file_versions').keys())

    return snapshot_manifest

def assert_point_consistency(write_peer: str, read_peer: str):
    assert scroll_points(write_peer) == scroll_points(read_peer)

def scroll_points(peer_url: str):
    resp = requests.post(f"{peer_url}/collections/{COLLECTION}/points/scroll", json = {
        "limit": 1000000,
        "with_vectors": True,
        "with_payload": True,
    })
    assert_http_ok(resp)

    points = resp.json()["result"]["points"]
    points = { point["id"]: point for point in points }

    return points


def upsert(peer_url: str, points: int, offset = 0):
    upsert_random_points(peer_url, points, offset = offset, batch_size = 10, with_sparse_vector = False)
