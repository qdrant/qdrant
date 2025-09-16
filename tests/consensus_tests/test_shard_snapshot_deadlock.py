from concurrent.futures import ThreadPoolExecutor
import pathlib
import requests
import time

from .assertions import *
from .fixtures import *
from .utils import *


def loop_telemetry(peer_url):
    while True:
        requests.get(f"{peer_url}/telemetry", params = { "details_level": "10" })

def loop_version(peer_url):
    while True:
        requests.get(f"{peer_url}/", timeout = 0.5)

def test_shard_snapshot_deadlock(tmp_path: pathlib.Path):
    assert_project_root()

    # Start peer with a single Actix worker
    peer_urls, _, _ = start_cluster(tmp_path, 1, extra_env = { "QDRANT__SERVICE__MAX_WORKERS": "1" })
    peer_url = peer_urls[0]

    # Create collection
    create_collection(peer_url)

    # Initialize collection with a few points
    upsert_random_points(peer_url, 10_000)

    print("ready to take snapshot")

    # Request streaming shard snapshot
    snapshot = requests.get(f"{peer_url}/collections/test_collection/shards/0/snapshot", stream = True)

    # Check HTTP status
    snapshot.raise_for_status()

    # Read only the first response chunk, but don't read response to completion. This ensures
    # that snapshot processing started, but makes request "hang" while holding segment read-lock.
    snapshot_stream = snapshot.iter_content(None)
    snapshot_chunk = next(snapshot_stream)

    # Run background executor to send blocking requests without blocking the test
    executor = ThreadPoolExecutor(max_workers = 3)

    # Get telemetry, to block on segment read-lock, which would block Actix worker
    telemetry = executor.submit(loop_telemetry, peer_url)

    # Upsert a point, to block on segment write-lock
    upsert = executor.submit(upsert_random_points, peer_url, 10_000, batch_size=1)
    
    # Get version, to block on segment read-lock, which would block Actix worker
    version = executor.submit(loop_version, peer_url)

    # Let executor cook for a bit to get some interleaving
    time.sleep(0.5)

    # Try to query Qdrant version info, which would block and timeout if Actix worker is blocked
    try:
        resp = requests.get(f"{peer_url}/", timeout = 20)
        assert_http_ok(resp)
    except requests.exceptions.Timeout:
        print(f"Request timed out against {peer_url}")
        # uncomment `sleep` and remove `raise` to investigate the deadlock with gdb
        #time.sleep(100_000) 
        raise
