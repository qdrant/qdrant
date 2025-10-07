import concurrent.futures
import pathlib
import requests
from typing import Any

from .assertions import *
from .fixtures import *
from .utils import *

COLLECTION = "test_collection"

@pytest.mark.parametrize(
    # bootstrap_points - upsert points when creating collection on write peer
    #                    (useful when used with `recove_read = True`)
    #
    # recover_read     - bootstrap read peer by recovering collection snapshot from write peer
    #                    (ensures segment IDs and points on both peers are *exactly* the same)
    #
    # append_points    - append *new* points to write peer
    #                    (this should modify appendable segments on write peer)
    #
    # update_points    - update (if > 0) or delete (if < 0) *existing* points on write peer
    #                    (this should modify indexed segments on write peer)

    "bootstrap_points, recover_read, append_points, update_points",
    [
        # Test "full" recovery:
        #
        # - new collection is created on read peer (`recover_read = False`)
        #   - collection exists on both peers, but it's empty on read peer and segment IDs are different
        # - partial snapshot recovered on read peer
        #   - because segments are different between peers, partial snapshot should act as regular shard snapshot
        #   - e.g., include every segment and every file from write peer, and replace all segments on read peer
        # - `append_points`/`update_points` are meaningless in this test case
        (0,   False, 0,  0),
        (100, False, 0,  0),

        # Test "empty" recovery:
        #
        # - read peer bootstrapped from write peer (`recover_read = True`)
        #   - collection is *exactly* the same on both peers: same points and segment IDs
        # - no changes on write peer (`append_points = 0` and `update_points = 0`)
        # - partial snapshot recovered on read peer
        #   - because there were no changes on write peer, the snapshot should be "empty"
        #   - e.g., all segments should only include `segment_manifest.json`, but no data files,
        #     no segments should be removed or changed on read peer
        (0,   True,  0,  0),
        (100, True,  0,  0),

        # Test appendable segments recovery:
        #
        # - read peer bootstrapped from write peer (`recover_read = True`)
        #   - collection is *exactly* the same on both peers: same points and segment IDs
        # - new points appended on write peer (`append_points = 5`)
        # - partial snapshot recovered on read peer
        #   - only appendable segments should be included in partial snapshot, immutable segments
        #     should be empty (e.g., only contain `segment_manifest.json`, but no data files)
        (0,   True,  5,  0),
        (100, True,  5,  0),

        # Test immutable segments recovery:
        #
        # - read peer bootstrapped from write peer (`recover_read = True`)
        #   - collection is *exactly* the same on both peers: same points and segment IDs
        # - indexed points updated/deleted on write peer (`update_points != 0`)
        # - partial snapshot recovered on read peer
        #   - only immutable segments should be included in partial snapshot, appendable segments
        #     should be empty (e.g., only contain `segment_manifest.json`, but no data files)
        (100, True,  0,  5),
        (100, True,  0, -5),
    ]
)
def test_partial_snapshot(
    tmp_path: pathlib.Path,
    bootstrap_points: int,
    recover_read: bool,
    append_points: int,
    update_points: int,
):
    assert_project_root()

    write_peer, read_peer = bootstrap_peers(
        tmp_path,
        bootstrap_points = bootstrap_points,
        recover_read = recover_read,
    )

    if append_points > 0:
        upsert(write_peer, append_points, offset = bootstrap_points)

    if update_points > 0:
        upsert(write_peer, update_points)

    if update_points < 0:
        delete(write_peer, -update_points)

    recover_partial_snapshot_from(read_peer, write_peer)
    assert_consistency(read_peer, write_peer)

@pytest.mark.parametrize("wait", [True, False])
def test_partial_snapshot_recovery_lock(tmp_path: pathlib.Path, wait: bool):
    assert_project_root()

    write_peer, read_peer = bootstrap_peers(tmp_path, bootstrap_points = 100_000)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers = 3)
    futures = [executor.submit(try_recover_partial_snapshot_from, read_peer, write_peer, wait = wait) for _ in range(3)]
    responses = [future.result() for future in concurrent.futures.as_completed(futures)]

    # Single partial snapshot recovery request allowed at the same time
    assert any(response.status_code == 503 for response in responses), "Subsequent partial snapshot recovery requests have to be rejected during partial snapshot recovery"

def test_partial_snapshot_read_lock(tmp_path: pathlib.Path):
    assert_project_root()

    write_peer, read_peer = bootstrap_peers(tmp_path, bootstrap_points = 100_000)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers = 1)
    recover_future = executor.submit(recover_partial_snapshot_from, read_peer, write_peer)

    is_search_rejected = False
    while not recover_future.done():
        response = try_search_random(read_peer)

        # Shard is unavailable during partial snapshot recovery
        if response.status_code == 500:
            is_search_rejected = True
            break

    assert is_search_rejected, "Search requests have to be rejected during partial snapshot recovery"

def test_partial_snapshot_empty(tmp_path: pathlib.Path):
    assert_project_root()

    write_peer, read_peer = bootstrap_peers(tmp_path, bootstrap_points = 1000, recover_read= True)

    # Don't let asynchronous optimizations mess with our test
    wait_collection_green(write_peer, COLLECTION)

    # Collection snapshot doesn't affect partial snapshot recovery timestamp
    recovery_ts = get_telemetry_collections(read_peer)[0]['shards'][0]['partial_snapshot']['recovery_timestamp']
    assert recovery_ts == 0

    # Ensure that both replicas are in sync (because of collection snapshot) and new partial snapshots are empty
    resp = create_partial_snapshot(write_peer, shard = 0, manifest = get_snapshot_manifest(read_peer))
    assert resp.status_code == 304

    recovered = recover_partial_snapshot_from(read_peer, write_peer, shard = 0)
    assert not recovered

    # recovery ts should be updated despite empty partial snapshot
    recovery_ts = get_telemetry_collections(read_peer)[0]['shards'][0]['partial_snapshot']['recovery_timestamp']
    assert recovery_ts > 0

def test_partial_snapshot_update_payload_index_schema(tmp_path: pathlib.Path):
    assert_project_root()

    write_peer, read_peer = bootstrap_peers(tmp_path, shards = 2, bootstrap_points = 1_000)

    # Enable strict mode on read peer
    update_collection_parameters(read_peer, {
        "strict_mode_config": {
            "enabled": True,
            "unindexed_filtering_retrieve": False,
        }
    })

    # Crete field index on write peer
    create_field_index(write_peer, "city", "keyword")

    # Recover shard 0 on read peer
    # Expect filtered query to fail, because field index does not exist on shard 1 yet
    recover_partial_snapshot_from(read_peer, write_peer, shard = 0)
    resp = try_query_by_field(read_peer, "city", "London")
    assert resp.status_code == 400

    # Recover shard 1 on read peer
    # Expect filtered query to succeed, because field index exists on both shards
    recover_partial_snapshot_from(read_peer, write_peer, shard = 1)
    resp = try_query_by_field(read_peer, "city", "London")
    assert_http_ok(resp)

def test_partial_snapshot_recreate_payload_field_index(tmp_path: pathlib.Path):
    assert_project_root()

    write_peer, read_peer = bootstrap_peers(tmp_path, bootstrap_points = 1_000)

    # Enable strict mode on read peer
    update_collection_parameters(read_peer, {
        "strict_mode_config": {
            "enabled": True,
            "unindexed_filtering_retrieve": False,
        }
    })

    # Create field index *with range filters disabled* on write peer
    create_field_index(write_peer, "magic", {
        "type": "integer",
        "range": False,
    })

    # Recover read peer
    # Expect filtered query to fail, because range index does not exist yet
    recover_partial_snapshot_from(read_peer, write_peer)
    resp = try_query_by_range(read_peer, "magic", 42, 69)
    assert resp.status_code == 400

    # Recreate field index *with range filters enabled* on write peer
    create_field_index(write_peer, "magic", "integer")

    # Recover read peer
    # Expect filtered query to succeed, because range index exists now
    recover_partial_snapshot_from(read_peer, write_peer)
    resp = try_query_by_range(read_peer, "magic", 42, 69)
    assert_http_ok(resp)


def bootstrap_peers(tmp: pathlib.Path, shards = 1, bootstrap_points = 0, recover_read = False):
    write_peer = bootstrap_write_peer(tmp, shards, bootstrap_points)
    read_peer = bootstrap_read_peer(tmp, shards, write_peer if recover_read else None)
    return write_peer, read_peer

def bootstrap_write_peer(tmp: pathlib.Path, shards = 1, bootstrap_points = 0):
    write_peer = bootstrap_peer(tmp / "write", 6331, "write_")
    bootstrap_collection(write_peer, shards, bootstrap_points)
    return write_peer

def bootstrap_read_peer(tmp: pathlib.Path, shards = 1, recover_from_url: str | None = None):
    read_peer = bootstrap_peer(tmp / "read", 63331, "read_")

    if recover_from_url is None:
        bootstrap_collection(read_peer, shards)
    else:
        recover_collection(read_peer, recover_from_url)

    return read_peer

def bootstrap_peer(path: pathlib.Path, port: int, log_file_prefix = ""):
    path.mkdir(exist_ok = True)

    config = {
        "QDRANT__LOG_LEVEL": "debug,collection::common::file_utils=trace",
        "QDRANT__FEATURE_FLAGS__USE_MUTABLE_ID_TRACKER_WITHOUT_ROCKSDB": "true",
    }

    uris, _, _ = start_cluster(path, 1, port_seed = port, extra_env = config, log_file_prefix = log_file_prefix)

    return uris[0]

def bootstrap_collection(peer_url, shards = 1, bootstrap_points = 0):
    create_collection(
        peer_url,
        shard_number = shards,
        replication_factor = 1,
        default_segment_number = 1,
        indexing_threshold = 1,
        sparse_vectors = False,
    )

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

def recover_partial_snapshot_from(peer_url: str, recover_peer_url: str, shard = 0, wait = True):
    resp = try_recover_partial_snapshot_from(peer_url, recover_peer_url, shard, wait)
    assert_http_ok(resp)

    return resp.json()["result"]

def try_recover_partial_snapshot_from(peer_url: str, recover_peer_url: str, shard = 0, wait = True):
    resp = requests.post(
        f"{peer_url}/collections/{COLLECTION}/shards/{shard}/snapshot/partial/recover_from?wait={'true' if wait else 'false'}",
        json = { "peer_url": recover_peer_url },
    )

    return resp

def create_partial_snapshot(peer_url: str, shard = 0, manifest = Any):
    resp = requests.post(
        f"{peer_url}/collections/{COLLECTION}/shards/{shard}/snapshot/partial/create",
        json = manifest,
    )

    return resp

def get_snapshot_manifest(peer_url: str, shard = 0):
    resp = requests.get(f"{peer_url}/collections/{COLLECTION}/shards/{shard}/snapshot/partial/manifest")
    assert_http_ok(resp)

    return resp.json()["result"]

def update_collection_parameters(peer_url: str, parameters: Any):
    resp = requests.patch(f"{peer_url}/collections/{COLLECTION}", json = parameters)
    assert_http_ok(resp)

def create_field_index(peer_url: str, key: str, schema: Any):
    resp = requests.put(f"{peer_url}/collections/{COLLECTION}/index?wait=true", json = {
        "field_name": key,
        "field_schema": schema,
    })
    assert_http_ok(resp)


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

def delete(peer_url: str, until_id: int, from_id = 0):
    resp = requests.post(f"{peer_url}/collections/{COLLECTION}/points/delete?wait=true", json = {
        "points": list(range(from_id, until_id)),
    })
    assert_http_ok(resp)

def try_search_random(peer_url: str):
    resp = requests.post(f"{peer_url}/collections/{COLLECTION}/points/search", json = {
        "vector": random_dense_vector(),
        "limit": 10,
        "with_vectors": True,
        "with_payload": True,
    })

    return resp

def try_query_by_field(peer_url: str, key: str, value: Any):
    resp = requests.post(f"{peer_url}/collections/{COLLECTION}/points/query", json = {
        "filter": {
            "must": {
                "key": key,
                "match": {
                    "value": value,
                }
            }
        }
    })

    return resp

def try_query_by_range(peer_url: str, key: str, min: int, max: int):
    resp = requests.post(f"{peer_url}/collections/{COLLECTION}/points/query", json = {
        "filter": {
            "must": {
                "key": key,
                "range": {
                    "gte": min,
                    "lte": max,
                }
            }
        }
    })

    return resp
