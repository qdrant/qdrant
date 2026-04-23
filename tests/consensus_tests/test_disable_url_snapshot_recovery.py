import pathlib

import requests

from .fixtures import create_collection
from .utils import *

COLLECTION_NAME = "test_collection"

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
