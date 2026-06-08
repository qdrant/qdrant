"""End-to-end test for the strict-mode `max_resident_memory_percent` threshold.

Uses sparse vectors on purpose: dense vectors live in mmap'd storage and do
not show up in jemalloc RSS, so they would never trip a heap-based threshold.
The sparse inverted index is resident in RAM, and every new unique token id
we insert grows it, which is exactly what we need to exercise the check.

Scenario:
1. Launch Qdrant in a container with a tight memory limit.
2. Create a sparse-only collection and turn strict mode on with a memory
   threshold well below the container cap.
3. Upsert batches of sparse vectors with random indices drawn from a very
   large id space until an upsert is rejected with the memory strict-mode
   error (process RSS crossed the configured percentage). Inserts are paced so
   the 5s-cached memory reader trips the gate before the cap OOM-kills us.
4. Delete a significant chunk of points. Deletes must not be gated by the
   memory check — otherwise there would be no way to recover.
5. Retry inserts with a generous timeout. jemalloc may hold freed pages for
   a while and the process-wide memory reader has a 5s TTL cache, so the
   probe has to be patient; eventually a fresh upsert must succeed.
"""

import random
import time
import uuid
from typing import Optional

import requests
from qdrant_client import models
from qdrant_client.http.exceptions import UnexpectedResponse

from e2e_tests.client_utils import ClientUtils
from e2e_tests.models import QdrantContainerConfig

# Container memory cap. Tight enough that the threshold trips after a modest
# amount of data, while still leaving the base process room to run.
MEM_LIMIT = "400m"

# 70% of the cap — loose enough that idle startup doesn't already trip,
# tight enough that we reach it without exploding the container.
MEMORY_PERCENT_THRESHOLD = 70

# Name of the single sparse vector on the collection.
SPARSE_VECTOR_NAME = "sparse"

# Random indices are drawn from [0, SPARSE_ID_SPACE). A very large id space
# maximises the rate at which each new batch introduces previously-unseen
# tokens, which is what actually grows the inverted index on the heap.
SPARSE_ID_SPACE = 2_000_000

# Non-zeros per sparse vector. Larger = more heap pressure per point.
NNZ_PER_VECTOR = 512

# Small batches: the gate reads memory through a 5s cache, so back-to-back
# batches slip through stale; keep each from overshooting the cap.
BATCH_SIZE = 100

# The gate re-samples memory only every 5s; without pacing, inserts OOM the
# container before it trips. Slow growth so it trips cleanly near the threshold.
INSERT_PACING_SECS = 1.5

COLLECTION_NAME = "strict_mode_memory_test"

# Upper bound on how many batches we'll try before declaring "threshold never
# kicked in" — the container cap protects us, this just keeps a broken test
# from running forever.
MAX_INSERT_BATCHES = 400

# After deletes, how long we're willing to wait for memory to drop and for an
# insert to succeed again. Bounded mainly by the 5s TTL cache on the memory
# reader plus jemalloc's own lag in returning pages to the OS.
RECOVERY_TIMEOUT_SECS = 90


def _set_strict_mode_via_rest(host: str, port: int, collection: str, config: dict) -> None:
    """PATCH strict mode directly through REST.

    The Python client's `StrictModeConfig` model may not yet know about the
    `max_resident_memory_percent` field on older releases, so we bypass it.
    """
    response = requests.patch(
        f"http://{host}:{port}/collections/{collection}",
        json={"strict_mode_config": config},
        timeout=30,
    )
    response.raise_for_status()


def _random_sparse_points(n: int) -> list[models.PointStruct]:
    """Generate points whose sparse vector has NNZ_PER_VECTOR random unique,
    sorted indices pulled from SPARSE_ID_SPACE."""
    points = []
    for _ in range(n):
        indices = sorted(random.sample(range(SPARSE_ID_SPACE), NNZ_PER_VECTOR))
        values = [random.random() for _ in range(NNZ_PER_VECTOR)]
        points.append(
            models.PointStruct(
                id=str(uuid.uuid4()),
                vector={
                    SPARSE_VECTOR_NAME: models.SparseVector(
                        indices=indices, values=values
                    )
                },
            )
        )
    return points


def _try_upsert(client: ClientUtils, points: list[models.PointStruct]) -> Optional[str]:
    """Upsert a batch; return the error string if rejected, else None."""
    try:
        client.client.upsert(
            collection_name=COLLECTION_NAME,
            points=points,
            wait=True,
        )
        return None
    except UnexpectedResponse as exc:
        return str(exc)
    except Exception as exc:  # qdrant-client may wrap differently depending on version
        return str(exc)


def _is_memory_rejection(err: str) -> bool:
    """Distinguish the memory-threshold rejection from unrelated errors.

    Only check for the prefix of the server message — ``UnexpectedResponse``
    truncates the raw body, so the ``max_resident_memory_percent`` hint at the
    end may be cut off.
    """
    return "resident memory usage" in err.lower()


class TestStrictModeMemory:
    def test_memory_threshold_blocks_then_recovers_after_delete(
        self, qdrant_container_factory
    ):
        config = QdrantContainerConfig(
            name=f"qdrant-sm-memory-{uuid.uuid4().hex[:8]}",
            mem_limit=MEM_LIMIT,
        )
        container_info = qdrant_container_factory(config)

        client = ClientUtils(host=container_info.host, port=container_info.http_port)
        assert client.wait_for_server(), "Server failed to start"

        # 1. Create a sparse-only collection. We go through the raw client
        #    because ClientUtils.create_collection doesn't wire through
        #    sparse_vectors_config.
        client.client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config={},
            sparse_vectors_config={
                SPARSE_VECTOR_NAME: models.SparseVectorParams(),
            },
        )

        # 2. Enable strict mode with a resident-memory threshold.
        _set_strict_mode_via_rest(
            container_info.host,
            container_info.http_port,
            COLLECTION_NAME,
            {
                "enabled": True,
                "max_resident_memory_percent": MEMORY_PERCENT_THRESHOLD,
            },
        )

        # 3. Upload until the threshold trips.
        inserted_ids: list[str] = []
        rejection_msg: Optional[str] = None
        for batch_idx in range(MAX_INSERT_BATCHES):
            batch = _random_sparse_points(BATCH_SIZE)
            err = _try_upsert(client, batch)
            if err is None:
                inserted_ids.extend(p.id for p in batch)
                # Let the 5s-cached memory reader refresh so the gate can trip.
                time.sleep(INSERT_PACING_SECS)
                continue

            if _is_memory_rejection(err):
                rejection_msg = err
                print(
                    f"Strict mode tripped after {batch_idx} batches "
                    f"({len(inserted_ids)} points inserted). Error: {err}"
                )
                break

            # Any other error is a genuine failure.
            raise AssertionError(
                f"Unexpected upsert error on batch {batch_idx}: {err}"
            )

        assert rejection_msg is not None, (
            f"Memory threshold never tripped after {MAX_INSERT_BATCHES} batches "
            f"({len(inserted_ids)} points). Raise the data volume or lower "
            f"MEMORY_PERCENT_THRESHOLD."
        )

        # 4. Delete roughly half of the points. Deletes must pass through even
        #    while the memory check is rejecting upserts — that's the whole
        #    point of excluding them from `consumes_memory()`.
        delete_count = len(inserted_ids) // 2
        ids_to_delete = inserted_ids[:delete_count]
        client.client.delete(
            collection_name=COLLECTION_NAME,
            points_selector=models.PointIdsList(points=ids_to_delete),
            wait=True,
        )
        remaining_ids = inserted_ids[delete_count:]
        print(f"Deleted {delete_count} points, {len(remaining_ids)} remain.")

        # 5. Retry an upsert until it succeeds or we time out. Memory doesn't
        #    drop instantly (jemalloc page return + 5s reader cache), so we
        #    probe periodically with a small batch.
        deadline = time.time() + RECOVERY_TIMEOUT_SECS
        probe_batch = _random_sparse_points(10)
        attempt = 0
        last_err: Optional[str] = None
        while time.time() < deadline:
            attempt += 1
            err = _try_upsert(client, probe_batch)
            if err is None:
                print(f"Upsert recovered on attempt {attempt}.")
                return

            if not _is_memory_rejection(err):
                raise AssertionError(
                    f"Unexpected upsert error during recovery (attempt {attempt}): {err}"
                )

            last_err = err
            # Generate a fresh batch so retries don't collapse into an idempotent no-op.
            probe_batch = _random_sparse_points(10)
            time.sleep(3)

        raise AssertionError(
            f"Upsert never recovered within {RECOVERY_TIMEOUT_SECS}s after "
            f"deleting {delete_count} points. Last error: {last_err}"
        )
