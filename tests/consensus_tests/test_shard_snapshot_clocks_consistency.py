import pathlib
from concurrent.futures import ThreadPoolExecutor
from time import sleep

import requests

from .assertions import *
from .fixtures import *
from .utils import *


def test_shard_snapshot_clocks_consistency(tmp_path: pathlib.Path):
    assert_project_root()

    # Start peer
    peer_urls, _, _ = start_cluster(tmp_path, 1, extra_env={ "QDRANT__STAGING__SNAPSHOT_SHARD_CLOCKS_DELAY": "5" })
    peer_url = peer_urls[0]

    # Bootstrap collection
    create_collection(peer_url)
    upsert_random_points(peer_url, 10_000, batch_size=100, with_sparse_vector=False)

    # Upsert points in the background
    upsert_random_points_bg = True

    def upsert_random_points_bg_task():
        while upsert_random_points_bg:
            upsert_random_points(peer_url, 100, with_sparse_vector=False, wait="false")

    executor = ThreadPoolExecutor()
    executor.submit(upsert_random_points_bg_task)

    # Create and download collection snapshot
    snapshot = requests.post(f"{peer_url}/collections/test_collection/snapshots").json()['result']['name']

    with requests.get(f"{peer_url}/collections/test_collection/snapshots/{snapshot}", stream=True) as snapshot:
        snapshot.raise_for_status()

        with open(tmp_path / "snapshot.tar", "wb") as file:
            for chunk in snapshot.iter_content(chunk_size=8192):
                file.write(chunk)

    # Stop background task
    upsert_random_points_bg = False
    executor.shutdown()

    # Recover shard snapshot
    with open(tmp_path / "snapshot.tar", "rb") as file:
        response = requests.post(f"{peer_url}/collections/test_collection/snapshots/upload", files={ "snapshot": file })
        response.raise_for_status()

    sleep(1)

    # TODO: Compare clock tags of most recent operations in WAL with recovery point
    recovery_point = requests.get(f"{peer_url}/collections/test_collection/shards/0/recovery_point").json()['result']
    wal = requests.get(f"{peer_url}/collections/test_collection/shards/0/wal?entries=5").json()['result']

    assert len(recovery_point) == 1
    clock_tag = recovery_point[0]

    assert len(wal) > 0
    operation = wal[0]

    assert clock_tag == operation['clock_tag']
