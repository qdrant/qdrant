"""
A streamed shard snapshot must not wait forever for a concurrent one to finish.

Snapshots of one shard serialize on the segment holder's upgradable read lock,
taken by `proxy_all_segments_and_apply` and held for the whole snapshot:

    let segments_lock = segments.upgradable_read();

`parking_lot` admits one upgradable reader at a time, and a *streamed* snapshot
lives for as long as its consumer keeps reading. So a second stream used to park
there indefinitely -- after actix had already committed a `200 OK`, leaving the
consumer with a response that never produced a byte. Measured before the fix:
30s, zero bytes, while the first stream was happily streaming 500+ KB.

That is what a retried shard transfer hits when the previous attempt's stream was
not torn down: its download looks hung rather than failing.

Streamed snapshots now bound that wait and fail with an explanation. Snapshots
written to a local file have no waiting consumer and still queue.

    pytest tests/consensus_tests/test_concurrent_shard_snapshot_streams.py -s -v
"""

import pathlib
import threading
import time

import requests

from .assertions import assert_http_ok
from .fixtures import create_collection, upsert_random_points
from .utils import *

COLLECTION_NAME = "test_collection"

# Big enough that the first snapshot outlives the whole observation window.
NUM_POINTS = 50_000

# First download reads this much per step, then sleeps -- slow enough to keep the
# sender parked mid-snapshot, holding the lock.
THROTTLE_CHUNK = 4096
THROTTLE_SLEEP = 0.2

# Let the first stream get well inside `proxy_all_segments_and_apply`.
LEAD_SECS = 3

# `STREAMED_SNAPSHOT_SEGMENT_LOCK_TIMEOUT` in local_shard/snapshot.rs.
SERVER_LOCK_TIMEOUT_SECS = 30

# Generous, so that finishing early proves the *server* gave up rather than the
# client timing out.
CLIENT_TIMEOUT_SECS = 90

LOCK_TIMEOUT_LOG = "waiting for exclusive access to the segment holder"


class ThrottledDownload(threading.Thread):
    """Hold a snapshot download open, reading slowly, until asked to stop."""

    def __init__(self, url: str):
        super().__init__(daemon=True)
        self.url = url
        self.bytes_read = 0
        self._stop = threading.Event()

    def run(self):
        try:
            with requests.get(self.url, stream=True, timeout=CLIENT_TIMEOUT_SECS) as resp:
                for chunk in resp.iter_content(THROTTLE_CHUNK):
                    self.bytes_read += len(chunk)
                    if self._stop.is_set():
                        return
                    time.sleep(THROTTLE_SLEEP)
        except requests.exceptions.RequestException:
            pass

    def stop(self):
        self._stop.set()


def test_streamed_snapshot_gives_up_waiting_for_a_concurrent_one(tmp_path: pathlib.Path):
    assert_project_root()

    peer_dirs = make_peer_folders(tmp_path, 1)
    peer_uri, _ = start_first_peer(peer_dirs[0], "peer_0_0.log")
    wait_peer_added(peer_uri)

    create_collection(peer_uri, shard_number=1, replication_factor=1)
    wait_collection_exists_and_active_on_all_peers(COLLECTION_NAME, [peer_uri])
    upsert_random_points(peer_uri, NUM_POINTS, batch_size=1000)

    info = get_collection_cluster_info(peer_uri, COLLECTION_NAME)
    shard_id = info["local_shards"][0]["shard_id"]
    url = f"{peer_uri}/collections/{COLLECTION_NAME}/shards/{shard_id}/snapshot"

    first = ThrottledDownload(url)
    first.start()

    time.sleep(LEAD_SECS)
    assert first.is_alive(), "throttled download ended too early, raise NUM_POINTS"
    assert first.bytes_read > 0, "first stream never produced any bytes"

    started = time.monotonic()
    received = 0
    try:
        with requests.get(url, stream=True, timeout=CLIENT_TIMEOUT_SECS) as resp:
            assert_http_ok(resp)
            for chunk in resp.iter_content(THROTTLE_CHUNK):
                received += len(chunk)
    except requests.exceptions.RequestException as err:
        print(f"\nsecond stream ended with: {type(err).__name__}: {err}")
    elapsed = time.monotonic() - started

    print(
        f"\nfirst stream read {first.bytes_read} bytes; "
        f"second stream ended after {elapsed:.1f}s with {received} bytes"
    )

    first.stop()

    # The second stream must give up on its own, not hang until the client's
    # timeout. Allow slack over the server's 30s bound for snapshot setup.
    assert elapsed < CLIENT_TIMEOUT_SECS - 15, (
        f"second stream did not give up on its own: it ran {elapsed:.1f}s, which "
        f"means it waited for the client timeout rather than the server's "
        f"{SERVER_LOCK_TIMEOUT_SECS}s lock bound"
    )

    # And it must say why, rather than dying silently.
    log = (pathlib.Path(init_pytest_log_folder()) / "peer_0_0.log").read_text()
    assert LOCK_TIMEOUT_LOG in log, (
        "sender should log why the concurrent snapshot was abandoned"
    )
