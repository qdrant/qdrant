"""
Reproduction for https://github.com/qdrant/qdrant/issues/9373

Symptom reported in the issue:
  - After creating a keyword payload index with ``wait: true``, a filtered query
    returns only a small fraction of the matching points (e.g. 2 out of 25).
  - The index creation API returns HTTP 200 (success).
  - The total point count is still correct.
  - The bug is state-dependent: it only appears once the container has accumulated
    operational history (i.e. when the optimizer is no longer able to make
    deferred points "ready" promptly).

Root cause (hypothesis #2 from the issue, related to PR #8239 "deferred points"):
  When points are upserted into an unoptimized appendable segment beyond the
  ``indexing_threshold``, the excess points become *deferred* and are invisible to
  reads/index queries until the optimizer turns them into a non-appendable segment.

  ``CreateFieldIndex`` with ``wait: true`` is supposed to block until those deferred
  points are ready before building the index over them. But if the optimizer is
  stalled (the real-world "accumulated state" condition), the internal wait for
  deferred points times out, the timeout is swallowed, and the API still returns
  HTTP 200 with a *partially built* index. Filtered queries then silently return
  incomplete results, while count (which is computed differently) looks correct.

This test forces the deferred-points condition deterministically by disabling the
optimizer (``max_optimization_threads=0``, ``prevent_unoptimized=True``), which is
the same mechanism used by ``test_deferred_points.py``.
"""

import random
import time

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import drop_collection

COLLECTION_NAME = "test_index_deferred_points"
VECTOR_DIM = 256

# With 256d float32 vectors each point is ~1 KB of vector data, and the
# indexing_threshold is expressed in KB, so a threshold of 100 makes roughly the
# first ~100 points visible and the rest deferred.
INDEXING_THRESHOLD_KB = 100

TOTAL_POINTS = 2000
CATEGORY_KEY = "category_id"
CAT_A = "cat_a"
CAT_B = "cat_b"


def setup_module():
    drop_collection(COLLECTION_NAME)


def teardown_module():
    drop_collection(COLLECTION_NAME)


def create_collection_with_stalled_optimizer():
    """
    Create a collection whose optimizer is disabled, so points upserted beyond the
    indexing threshold stay deferred (invisible) indefinitely. This emulates the
    "container has accumulated state / optimizer is backed up" condition from the
    issue without having to run for hours.
    """
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': COLLECTION_NAME},
        body={
            "vectors": {
                "size": VECTOR_DIM,
                "distance": "Cosine",
            },
            "optimizers_config": {
                "indexing_threshold": INDEXING_THRESHOLD_KB,
                "prevent_unoptimized": True,
                "max_optimization_threads": 0,
            },
        },
    )
    assert response.ok


def make_points(start_id, count):
    random.seed(start_id)
    points = []
    for i in range(count):
        point_id = start_id + i
        points.append({
            "id": point_id,
            "vector": [random.random() for _ in range(VECTOR_DIM)],
            "payload": {CATEGORY_KEY: CAT_A if point_id % 2 == 0 else CAT_B},
        })
    return points


def upsert_points(points, wait=True):
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': COLLECTION_NAME},
        query_params={'wait': 'true' if wait else 'false'},
        body={"points": points},
    )
    assert response.ok


def count_points():
    response = request_with_validation(
        api='/collections/{collection_name}/points/count',
        method="POST",
        path_params={'collection_name': COLLECTION_NAME},
        body={"exact": True},
    )
    assert response.ok
    return response.json()['result']['count']


def create_keyword_index_wait_true():
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': COLLECTION_NAME},
        query_params={'wait': 'true'},
        body={
            "field_name": CATEGORY_KEY,
            "field_schema": "keyword",
        },
    )
    assert response.ok
    return response


def scroll_filtered(value):
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': COLLECTION_NAME},
        body={
            "with_vector": False,
            "filter": {"must": [{"key": CATEGORY_KEY, "match": {"value": value}}]},
            "limit": TOTAL_POINTS,
        },
    )
    assert response.ok
    return [p['id'] for p in response.json()['result']['points']]


def test_index_over_deferred_points_is_complete():
    """
    Reproduces issue #9373.

    Steps mirror the issue's MRE:
      1. Insert points split evenly across two keyword values.
      2. Create a keyword payload index with ``wait: true``.
      3. Filter by one keyword value.

    The twist that makes the bug reproducible (matching the issue's "accumulated
    state" precondition) is that the optimizer is disabled, so most of the inserted
    points are *deferred* at the moment the index is built.

    Expected (correct) behaviour: ``wait: true`` makes the index cover every point
    that carries the field, so the filter returns all matching points.

    Buggy behaviour (issue #9373): the internal wait for deferred points times out,
    is swallowed, the API returns HTTP 200, and the filter returns only the handful
    of points that happened to be visible -> severely incomplete results.
    """
    create_collection_with_stalled_optimizer()

    # Upsert without waiting so the points land in the unoptimized appendable
    # segment; everything past the indexing threshold becomes deferred.
    upsert_points(make_points(1, TOTAL_POINTS), wait=False)
    time.sleep(3)

    expected_cat_a = TOTAL_POINTS // 2  # ids 2, 4, ... -> 1000 points

    # Build the keyword index with wait:true. Per the issue this returns HTTP 200.
    index_response = create_keyword_index_wait_true()
    assert index_response.status_code == 200
    time.sleep(2)

    matched = scroll_filtered(CAT_A)

    # This is the core assertion for issue #9373. When the bug is present the index
    # only covers the few visible (non-deferred) points, so far fewer than
    # `expected_cat_a` are returned even though `wait: true` reported success.
    assert len(matched) == expected_cat_a, (
        f"issue #9373: keyword index over deferred points is incomplete. "
        f"Filter for {CATEGORY_KEY}={CAT_A!r} returned {len(matched)} points, "
        f"expected {expected_cat_a}. Index creation returned HTTP "
        f"{index_response.status_code} (success) despite the missing points."
    )
