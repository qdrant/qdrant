"""End-to-end test for the strict-mode `max_disk_usage_percent` threshold.

The disk-usage gate samples the filesystem hosting Qdrant storage with a 5s
TTL cache, so the test has to be patient on both sides:

1. Boot Qdrant with `/qdrant/storage` backed by a small tmpfs so the gate is
   reachable with a modest amount of data.
2. Create a collection and turn strict mode on with a disk threshold well
   below the tmpfs capacity.
3. Upsert dense vectors (which actually hit disk via mmap segments) until
   the gate trips with the `"disk usage"` error.
4. Drop the collection — deletes alone don't free disk before segment
   rebuilds finish, but a collection drop reliably frees space and is the
   point of "give the operator a way out".
5. Wait for the cached usage reading to expire and confirm a fresh upsert
   succeeds against a new collection.
"""

import time
import uuid
from typing import Optional

import requests
from docker.types import Mount
from qdrant_client import models
from qdrant_client.http.exceptions import UnexpectedResponse

from e2e_tests.client_utils import ClientUtils
from e2e_tests.models import QdrantContainerConfig

# 200 MB tmpfs: needs to comfortably fit Qdrant's default WAL buffer
# (which is multiple tens of MB) plus the segment data we'll write, while
# being small enough that ~50% trips after a moderate amount of data.
TMPFS_BYTES = 200 * 1024 * 1024

# Shrink WAL so it doesn't dominate the small tmpfs (Qdrant's default WAL
# buffer alone would fill a sub-100 MB tmpfs at collection-creation time).
WAL_CAPACITY_MB = 1

DISK_PERCENT_THRESHOLD = 50

VECTOR_DIM = 128
BATCH_SIZE = 200
COLLECTION_NAME = "strict_mode_disk_test"
RECOVERY_COLLECTION_NAME = "strict_mode_disk_recovery"

MAX_INSERT_BATCHES = 400

RECOVERY_TIMEOUT_SECS = 60


def _set_strict_mode_via_rest(host: str, port: int, collection: str, config: dict) -> None:
    """PATCH strict mode directly through REST.

    Bypasses the Python client model because older releases may not yet know
    about the ``max_disk_usage_percent`` field.
    """
    response = requests.patch(
        f"http://{host}:{port}/collections/{collection}",
        json={"strict_mode_config": config},
        timeout=30,
    )
    response.raise_for_status()


def _random_dense_points(n: int) -> list[models.PointStruct]:
    import random

    points = []
    for _ in range(n):
        points.append(
            models.PointStruct(
                id=str(uuid.uuid4()),
                vector=[random.random() for _ in range(VECTOR_DIM)],
            )
        )
    return points


def _try_upsert(client: ClientUtils, collection: str, points: list[models.PointStruct]) -> Optional[str]:
    try:
        client.client.upsert(collection_name=collection, points=points, wait=True)
        return None
    except UnexpectedResponse as exc:
        return str(exc)
    except Exception as exc:
        return str(exc)


def _is_disk_rejection(err: str) -> bool:
    """Distinguish the disk-threshold rejection from unrelated errors.

    ``UnexpectedResponse`` truncates the raw body, so we match only on the
    stable prefix of the server message.
    """
    return "disk usage" in err.lower()


class TestStrictModeDisk:
    def test_disk_threshold_blocks_then_recovers_after_drop(
        self, qdrant_container_factory
    ):
        config = QdrantContainerConfig(
            name=f"qdrant-sm-disk-{uuid.uuid4().hex[:8]}",
            mounts=[
                Mount(
                    target="/qdrant/storage",
                    source=None,
                    type="tmpfs",
                    tmpfs_size=TMPFS_BYTES,
                )
            ],
            remove=False,
        )
        container_info = qdrant_container_factory(config)

        client = ClientUtils(host=container_info.host, port=container_info.http_port)
        assert client.wait_for_server(), "Server failed to start"

        client.client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=models.VectorParams(
                size=VECTOR_DIM, distance=models.Distance.COSINE
            ),
            wal_config=models.WalConfigDiff(wal_capacity_mb=WAL_CAPACITY_MB),
        )

        _set_strict_mode_via_rest(
            container_info.host,
            container_info.http_port,
            COLLECTION_NAME,
            {
                "enabled": True,
                "max_disk_usage_percent": DISK_PERCENT_THRESHOLD,
            },
        )

        inserted = 0
        rejection_msg: Optional[str] = None
        for batch_idx in range(MAX_INSERT_BATCHES):
            batch = _random_dense_points(BATCH_SIZE)
            err = _try_upsert(client, COLLECTION_NAME, batch)
            if err is None:
                inserted += len(batch)
                continue

            if _is_disk_rejection(err):
                rejection_msg = err
                print(
                    f"Strict mode tripped after {batch_idx} batches "
                    f"({inserted} points inserted). Error: {err}"
                )
                break

            raise AssertionError(
                f"Unexpected upsert error on batch {batch_idx}: {err}"
            )

        assert rejection_msg is not None, (
            f"Disk threshold never tripped after {MAX_INSERT_BATCHES} batches "
            f"({inserted} points). Lower DISK_PERCENT_THRESHOLD or raise data volume."
        )

        # Drop the collection to free disk. Deletes alone don't reclaim space
        # before segment rebuilds, so a drop is the deterministic way to
        # confirm the gate re-opens once space is freed.
        client.client.delete_collection(COLLECTION_NAME)
        print(f"Dropped {COLLECTION_NAME} to free disk.")

        # The reader is TTL-cached (5s). Give it room to refresh, then probe
        # with a fresh collection so we exercise the recovered-state path
        # without the rejected collection getting in the way.
        client.client.create_collection(
            collection_name=RECOVERY_COLLECTION_NAME,
            vectors_config=models.VectorParams(
                size=VECTOR_DIM, distance=models.Distance.COSINE
            ),
            wal_config=models.WalConfigDiff(wal_capacity_mb=WAL_CAPACITY_MB),
        )
        _set_strict_mode_via_rest(
            container_info.host,
            container_info.http_port,
            RECOVERY_COLLECTION_NAME,
            {
                "enabled": True,
                "max_disk_usage_percent": DISK_PERCENT_THRESHOLD,
            },
        )

        deadline = time.time() + RECOVERY_TIMEOUT_SECS
        attempt = 0
        last_err: Optional[str] = None
        while time.time() < deadline:
            attempt += 1
            err = _try_upsert(client, RECOVERY_COLLECTION_NAME, _random_dense_points(5))
            if err is None:
                print(f"Upsert recovered on attempt {attempt}.")
                return

            if not _is_disk_rejection(err):
                raise AssertionError(
                    f"Unexpected upsert error during recovery (attempt {attempt}): {err}"
                )

            last_err = err
            time.sleep(2)

        raise AssertionError(
            f"Upsert never recovered within {RECOVERY_TIMEOUT_SECS}s after "
            f"dropping the source collection. Last error: {last_err}"
        )
