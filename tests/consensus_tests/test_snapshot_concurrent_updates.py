import multiprocessing

import pathlib
import random
from time import sleep

from .fixtures import create_collection, update_points_payload, upsert_random_points
from .utils import *

COLLECTION_NAME = "test_collection"
NUM_POINTS = 20000


def update_points_in_loop(peer_url, collection_name, from_id=0, to_id=1000):
    while True:
        # Randomly choose a point ID to update
        offset = random.randint(from_id, to_id - 1)
        update_points_payload(peer_url, points=[offset], collection_name=collection_name)

def run_update_points_in_background(peer_url, collection_name, from_id=0, to_id=1000):
    p = multiprocessing.Process(target=update_points_in_loop,
                                args=(peer_url, collection_name, from_id, to_id))
    p.start()
    return p


def create_snapshot(peer_api_uri):
    r = requests.post(f"{peer_api_uri}/collections/{COLLECTION_NAME}/snapshots")
    assert_http_ok(r)
    return r.json()["result"]["name"]


def download_snapshot_to_local(peer_api_uri, collection_name, snapshot_name, download_path: pathlib.Path):
    r = requests.get(f"{peer_api_uri}/collections/{collection_name}/snapshots/{snapshot_name}",
                     stream=True)

    snapshot_file_path = download_path / f"{snapshot_name}"
    with open(snapshot_file_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

    return snapshot_file_path


def recover_snapshot(peer_api_uri, path: str):
    # Upload snapshot using multipart/form-data
    # Equivalent to
    # curl -X POST $url -H 'Content-Type:multipart/form-data' -F 'snapshot=@/path/to/snapshot-2022-10-10.snapshot'

    with open(path, "rb") as f:
        files = {'snapshot': f}
        r = requests.post(f"{peer_api_uri}/collections/{COLLECTION_NAME}/snapshots/upload?priority=snapshot",
                         files=files)
    assert_http_ok(r)
    return r.json()["result"]


def test_snapshot_concurrent_updates(tmp_path: pathlib.Path):
    """
    # This test cheks that making a snapshot while concurrent updates won't lose any points.

    Plan:

    1. Create a single node cluster, create collection with N points.
    2. Start updates in background (only overwrite existing points).
    3. While updates are ongoing, create a snapshot.
    4. After snapshot is created, kill updates.
    5. Recover collection from snapshot.
    6. Verify that points count is correct (N points).
    """

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 1, 20000)

    create_collection(peer_api_uris[0], shard_number=1, replication_factor=1)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )

    # Insert some initial number of points
    upsert_random_points(peer_api_uris[0], NUM_POINTS)
    update_process = run_update_points_in_background(peer_api_uris[0], COLLECTION_NAME, 0, NUM_POINTS)
    update_process2 = run_update_points_in_background(peer_api_uris[0], COLLECTION_NAME, 0, NUM_POINTS)

    sleep(1)  # Give some time for updates to start

    snapshot_name = create_snapshot(peer_api_uris[0])

    update_process.kill()
    update_process2.kill()

    # Download snapshot
    snapshot_path = download_snapshot_to_local(peer_api_uris[0], COLLECTION_NAME, snapshot_name, tmp_path)

    # Recover from snapshot
    recover_snapshot(peer_api_uris[0], snapshot_path)

    # Verify points count
    r = requests.post(f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/count", json={"exact": True})
    assert_http_ok(r)
    result = r.json()["result"]
    count = result["count"]
    assert count == NUM_POINTS, f"Expected {NUM_POINTS} points, got {count}"
