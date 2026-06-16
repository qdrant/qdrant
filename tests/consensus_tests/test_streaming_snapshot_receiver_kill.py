"""
Observe how the *sender* of a streaming shard snapshot reacts when a throttled
receiver is killed mid-flight and a second receiver then starts.

Scenario (single node = the sender):
  1. Create a collection with enough data that the snapshot tar is many MB,
     so a slow reader cannot drain it and the sender ends up parked mid-write.
  2. Receiver #1 pulls `GET /collections/{c}/shards/0/snapshot` at a throttled
     speed (read a chunk, sleep). This fills the sender's 4 KB duplex + socket
     buffers and parks the snapshot writer **while it holds the SegmentHolder
     upgradable-read lock** (see `proxy_all_segments_and_apply`).
  3. Kill receiver #1 mid-flight (like a receiver peer dying).
  4. Start receiver #2 and watch what the sender does.

What we observe ("how the sender reacts"):
  - Does the node stay responsive (`/`, `/cluster`)?  -> hard assert
  - Does receiver #2 complete its download?           -> the key signal:
      it can only finish if the sender RELEASED the SegmentHolder lock that
      receiver #1 was pinning. If the sender is wedged, #2 streams 200 OK but
      then never receives bytes (its snapshot blocks on `upgradable_read`).
  - When/whether the sender logged "broken pipe" / "Failed to stream shard
    snapshot" for receiver #1.

IMPORTANT — localhost caveat:
  Killing a local process makes the kernel send a prompt RST, so the sender's
  actix usually notices the disconnect quickly and recovers. The 18h
  production hang needed the RST to NOT be delivered (half-open connection via
  a cloud LB / NAT). To reproduce *that*, route the download through a
  throttling TCP proxy and, on "kill", drop the receiver<->proxy side while
  keeping the proxy<->sender socket open. This test deliberately uses the
  simple process-kill so it documents the baseline behavior; the proxy variant
  is noted at the bottom.

Usage:
    pytest tests/consensus_tests/test_streaming_snapshot_receiver_kill.py -s -v
"""

import multiprocessing
import pathlib
import time

import requests

from .fixtures import create_collection, upsert_random_points
from .utils import *

COLLECTION = "test_collection"
NUM_POINTS = 10_000
SHARD_ID = 0

# Throttle: ~20 KB/s, far slower than the sender can produce -> backpressure.
THROTTLE_CHUNK = 4096
THROTTLE_SLEEP = 0.2

# How long to let backpressure build before killing receiver #1.
BACKPRESSURE_BUILD_SECS = 2

# How long we give the sender to recover (receiver #2 to finish) after the kill.
RECOVER_TIMEOUT_SECS = 120

RESPONSIVE_TIMEOUT = 10


def _snapshot_url(peer_url: str) -> str:
    return f"{peer_url}/collections/{COLLECTION}/shards/{SHARD_ID}/snapshot"


def throttled_receiver(peer_url: str, ready_path: str):
    """Receiver #1: pull the snapshot slowly, forever. Run as a separate OS
    process so the test can `.kill()` it and drop its socket like a dead peer."""
    try:
        r = requests.get(_snapshot_url(peer_url), stream=True, timeout=300)
        if r.status_code != 200:
            print(f"[recv1] unexpected status {r.status_code}")
            return
        # Signal that the stream actually started (sender is now producing/parked).
        pathlib.Path(ready_path).write_text("streaming")
        total = 0
        for chunk in r.iter_content(chunk_size=THROTTLE_CHUNK):
            total += len(chunk)
            time.sleep(THROTTLE_SLEEP)
    except Exception as e:  # noqa: BLE001 - diagnostic process
        print(f"[recv1] ended: {e}")


def full_receiver(peer_url: str, ready_path: str, done_path: str):
    """Receiver #2: download to completion as fast as possible and record the
    result. Completing proves the sender released the lock receiver #1 held."""
    try:
        start = time.time()
        r = requests.get(_snapshot_url(peer_url), stream=True, timeout=RECOVER_TIMEOUT_SECS)
        if r.status_code != 200:
            pathlib.Path(done_path).write_text(f"status={r.status_code}")
            return
        pathlib.Path(ready_path).write_text("streaming")
        total = 0
        for chunk in r.iter_content(chunk_size=1 << 20):
            total += len(chunk)
        elapsed = time.time() - start
        pathlib.Path(done_path).write_text(f"ok bytes={total} secs={elapsed:.1f}")
    except Exception as e:  # noqa: BLE001 - diagnostic process
        pathlib.Path(done_path).write_text(f"error={e}")


def _responsive(peer_url: str) -> bool:
    try:
        requests.get(f"{peer_url}/", timeout=RESPONSIVE_TIMEOUT)
        requests.get(f"{peer_url}/collections/{COLLECTION}/cluster", timeout=RESPONSIVE_TIMEOUT)
        return True
    except requests.exceptions.RequestException:
        return False


def _sender_log_text() -> str:
    log_path = pathlib.Path(init_pytest_log_folder()) / "peer_0_0.log"
    try:
        return log_path.read_text(errors="replace")
    except OSError:
        return ""


def _wait_for_file(path: pathlib.Path, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists():
            return True
        time.sleep(0.1)
    return False


def test_streaming_snapshot_receiver_kill(tmp_path: pathlib.Path):
    assert_project_root()

    peer_urls, _, _ = start_cluster(tmp_path, 1)
    peer_url = peer_urls[0]

    create_collection(peer_url, collection=COLLECTION)
    upsert_random_points(peer_url, NUM_POINTS, collection_name=COLLECTION)
    print(f"[test] inserted {NUM_POINTS} points")

    ready1 = tmp_path / "recv1_ready"
    ready2 = tmp_path / "recv2_ready"
    done2 = tmp_path / "recv2_done"

    # --- Receiver #1: throttled, killed mid-flight ---------------------------
    recv1 = multiprocessing.Process(
        target=throttled_receiver, args=(peer_url, str(ready1)), daemon=True
    )
    recv1.start()

    assert _wait_for_file(ready1, timeout=30), "receiver #1 never started streaming"
    print("[test] receiver #1 is streaming (sender now parked holding the lock)")

    # Let the sender fill its buffers and park mid-write.
    time.sleep(BACKPRESSURE_BUILD_SECS)
    assert _responsive(peer_url), "node already unresponsive before the kill"

    recv1.kill()
    recv1.join(timeout=10)
    kill_time = time.time()
    print("[test] receiver #1 KILLED mid-flight")

    # --- Receiver #2: the "another receiver" --------------------------------
    recv2 = multiprocessing.Process(
        target=full_receiver, args=(peer_url, str(ready2), str(done2)), daemon=True
    )
    recv2.start()

    # Observe the sender's reaction.
    recovered = _wait_for_file(done2, timeout=RECOVER_TIMEOUT_SECS)
    recv2.join(timeout=5)

    # The node must stay responsive the whole time (blast-radius containment).
    assert _responsive(peer_url), "node became unresponsive after receiver kill"

    log = _sender_log_text()
    noticed_broken_pipe = "broken pipe" in log or "Failed to stream shard snapshot" in log

    print("\n==================== SENDER REACTION ====================")
    print(f"  node responsive after kill : True")
    print(f"  sender logged disconnect   : {noticed_broken_pipe}")
    if recovered:
        print(f"  receiver #2 result         : {done2.read_text()}")
        print(f"  time kill -> #2 done       : {time.time() - kill_time:.1f}s")
    else:
        print(f"  receiver #2 result         : DID NOT FINISH within {RECOVER_TIMEOUT_SECS}s")
        print(f"  -> sender appears WEDGED: the lock from receiver #1 was not released")
    print("=========================================================\n")

    # Key assertion: a second receiver must be able to snapshot, which is only
    # possible if the sender released the SegmentHolder lock held by receiver #1.
    assert recovered, (
        f"sender did not recover within {RECOVER_TIMEOUT_SECS}s after receiver #1 "
        f"was killed: receiver #2 could not obtain the snapshot stream"
    )
    assert done2.read_text().startswith("ok"), f"receiver #2 failed: {done2.read_text()}"


# -----------------------------------------------------------------------------
# Reproducing the TRUE half-open hang (optional, manual):
#
# Replace the direct download URL with a tiny throttling TCP proxy in front of
# the sender's REST port:
#
#   downloader  ──>  proxy (throttles, you control)  ──>  sender:REST
#
# "Kill the receiver" then means: close the downloader<->proxy socket but KEEP
# the proxy<->sender socket open and stop forwarding. The sender never receives
# an RST, its write stays parked, the SegmentHolder lock is held indefinitely,
# and receiver #2 hangs -> `recovered` stays False. That is the production
# deadlock, and it is exactly what a sender-side inactivity write-timeout
# (TimeoutWriter) would bound.
# -----------------------------------------------------------------------------
