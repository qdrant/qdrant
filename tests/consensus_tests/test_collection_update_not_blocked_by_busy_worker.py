"""
Test that a collection update applied through consensus is not blocked by a busy
update worker.

A collection config update (PATCH /collections/{name}) triggers recreation of the
shard optimizers. Recreating optimizers stops the update worker, which first has to
finish whatever operation it is currently running. If that recreation runs
*synchronously* on the consensus apply thread, a long-running update operation stalls
the whole consensus loop and the collection update hangs - which can take down a
cluster. The recreation must instead happen in the background, so the update returns
right away.

Repro:
  1. Keep the shard's update worker busy for a long time with a staging `delay`
     operation.
  2. Send a collection config update through consensus.
  3. The update must return promptly, not wait for the delay to finish.

Requires the `staging` feature (for the `delay` debug operation); skipped otherwise.

Usage:
    pytest tests/consensus_tests/test_collection_update_not_blocked_by_busy_worker.py -s -v
"""

import os
import pathlib
import time

import requests

os.environ.setdefault(
    "PYTEST_CURRENT_TEST", "test_collection_update_not_blocked_by_busy_worker"
)

from .utils import *

COLLECTION = "update_not_blocked"
DIM = 4

# How long the update worker is kept busy by the staging delay operation.
DELAY_SEC = 20.0
# Time to let the worker actually pick up and start processing the delay before we send
# the collection update. The worker only blocks on an operation it has already started;
# a queued-but-not-yet-started delay would simply be dropped when the worker stops, so
# we must make sure it is genuinely in-flight to exercise the blocking path.
WORKER_BUSY_WAIT_SEC = 4.0
# Generous client-side timeout: with the fix the PATCH returns near-instantly, without
# it the request blocks for ~(DELAY_SEC - WORKER_BUSY_WAIT_SEC) (or errors out earlier).
PATCH_CLIENT_TIMEOUT_SEC = 30.0
# The collection update must return well before the delay would finish. With the fix it
# is sub-second; without it, it is ~16s. 10s cleanly separates the two.
MAX_PATCH_SEC = 10.0


def create_collection(peer_uri: str):
    r = requests.put(
        f"{peer_uri}/collections/{COLLECTION}",
        json={
            "vectors": {"size": DIM, "distance": "Cosine"},
            "shard_number": 1,
            "replication_factor": 1,
        },
    )
    assert_http_ok(r)


def submit_delay(peer_uri: str, duration_sec: float):
    """Submit a staging delay operation to keep the shard's update worker busy.

    Sent with wait=false so the call returns immediately while the worker processes the
    delay in the background.
    """
    r = requests.post(
        f"{peer_uri}/collections/{COLLECTION}/debug?wait=false",
        json={"delay": {"duration_sec": duration_sec}},
    )
    assert_http_ok(r)


def test_collection_update_not_blocked_by_busy_worker(tmp_path: pathlib.Path):
    assert_project_root()

    peer_uris, _, _ = start_cluster(tmp_path, 1)
    peer_uri = peer_uris[0]

    # The `delay` debug operation is only available with the staging feature.
    skip_if_no_feature(peer_uri, "staging")

    create_collection(peer_uri)
    wait_for(check_collection_green, peer_uri, COLLECTION, wait_for_timeout=30)

    # Keep the update worker busy for a long time, then let it actually start the delay.
    submit_delay(peer_uri, DELAY_SEC)
    time.sleep(WORKER_BUSY_WAIT_SEC)

    # Send a collection config update through consensus. Any optimizers_config change
    # triggers optimizer recreation, which must not block the consensus apply thread on
    # the busy worker.
    start = time.time()
    try:
        r = requests.patch(
            f"{peer_uri}/collections/{COLLECTION}",
            json={"optimizers_config": {"default_segment_number": 2}},
            timeout=PATCH_CLIENT_TIMEOUT_SEC,
        )
    except requests.exceptions.Timeout:
        elapsed = time.time() - start
        raise AssertionError(
            f"Collection update did not return within {PATCH_CLIENT_TIMEOUT_SEC}s "
            f"(waited {elapsed:.1f}s) - consensus apply was blocked by the busy update worker"
        )
    elapsed = time.time() - start

    assert_http_ok(r)
    assert elapsed < MAX_PATCH_SEC, (
        f"Collection update took {elapsed:.1f}s (>= {MAX_PATCH_SEC}s) while the update worker "
        f"was busy for {DELAY_SEC}s - consensus apply was blocked instead of recreating "
        f"optimizers in the background"
    )
    print(
        f"Collection update returned in {elapsed:.2f}s while the update worker was busy "
        f"for {DELAY_SEC}s"
    )
