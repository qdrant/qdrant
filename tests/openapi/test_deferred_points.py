import pytest
import random
import time

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import drop_collection

COLLECTION_NAME = "test_deferred_points"
NUM_POINTS = 10000
VECTOR_DIM = 4


@pytest.fixture(autouse=True)
def setup():
    drop_collection(COLLECTION_NAME)
    yield
    drop_collection(COLLECTION_NAME)


def create_collection():
    """Create a collection with prevent_unoptimized enabled, zero optimizer threads, and low indexing threshold."""
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
                "indexing_threshold": 10,
                "max_optimization_threads": 0,
                "prevent_unoptimized": True,
            },
        }
    )
    assert response.ok


def get_collection_info():
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': COLLECTION_NAME},
    )
    assert response.ok
    return response.json()['result']


def upsert_points(start_id, count, wait=False):
    """Upsert points with sequential IDs and random vectors."""
    random.seed(42)
    points = []
    for i in range(count):
        point_id = start_id + i
        vector = [random.random() for _ in range(VECTOR_DIM)]
        points.append({
            "id": point_id,
            "vector": vector,
            "payload": {"idx": point_id},
        })

    # Upsert in batches of 100 to avoid too large requests
    batch_size = 100
    for batch_start in range(0, len(points), batch_size):
        batch = points[batch_start:batch_start + batch_size]
        response = request_with_validation(
            api='/collections/{collection_name}/points',
            method="PUT",
            path_params={'collection_name': COLLECTION_NAME},
            query_params={'wait': 'true' if wait else 'false'},
            body={"points": batch},
        )
        assert response.ok


def search_points(limit=10):
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': COLLECTION_NAME},
        body={
            "vector": [0.5] * VECTOR_DIM,
            "limit": limit,
        }
    )
    assert response.ok
    return response.json()['result']


def scroll_all_points():
    """Scroll through all points and return the full list."""
    all_points = []
    offset = None
    while True:
        body = {"limit": 100, "with_vector": False}
        if offset is not None:
            body["offset"] = offset

        response = request_with_validation(
            api='/collections/{collection_name}/points/scroll',
            method="POST",
            path_params={'collection_name': COLLECTION_NAME},
            body=body,
        )
        assert response.ok
        result = response.json()['result']
        all_points.extend(result['points'])
        offset = result.get('next_page_offset')
        if offset is None:
            break
    return all_points


def retrieve_points(ids):
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="POST",
        path_params={'collection_name': COLLECTION_NAME},
        body={"ids": ids},
    )
    assert response.ok
    return response.json()['result']


def test_deferred_points():
    create_collection()

    # Upsert 10000 points with wait=false, creating deferred points
    upsert_points(start_id=1, count=NUM_POINTS, wait=False)

    # Small sleep to let the async operations be applied
    time.sleep(1)

    info = get_collection_info()
    points_count = info['points_count']

    # With prevent_unoptimized and low indexing threshold,
    # some points should be deferred and thus invisible
    assert points_count < NUM_POINTS, (
        f"Expected some deferred (invisible) points, but all {NUM_POINTS} are visible"
    )
    assert points_count > 0, "Expected at least some visible points"

    # Scroll should only return visible (non-deferred) points
    scrolled_points = scroll_all_points()
    assert len(scrolled_points) == points_count, (
        f"Scroll returned {len(scrolled_points)} points, expected {points_count}"
    )

    # Search should only return visible (non-deferred) points
    search_results = search_points(limit=NUM_POINTS)
    assert len(search_results) <= points_count, (
        f"Search returned {len(search_results)} points, but only {points_count} should be visible"
    )

    # Retrieve all point IDs - only non-deferred should be returned
    all_ids = list(range(1, NUM_POINTS + 1))
    retrieved = retrieve_points(all_ids)
    assert len(retrieved) == points_count, (
        f"Retrieve returned {len(retrieved)} points, expected {points_count}"
    )

    # Now enable optimizers and upsert a single point with wait=true
    # This should trigger optimization and make all deferred points visible
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': COLLECTION_NAME},
        body={
            "optimizers_config": {
                "max_optimization_threads": 1,
            },
        }
    )
    assert response.ok

    # Upsert a single point with wait=true to trigger and wait for optimization
    upsert_points(start_id=NUM_POINTS + 1, count=1, wait=True)

    total_points = NUM_POINTS + 1

    # After optimization, all points should be visible
    info = get_collection_info()
    assert info['points_count'] == total_points, (
        f"After optimization, expected {total_points} points, got {info['points_count']}"
    )

    # Scroll should return all points now
    scrolled_points = scroll_all_points()
    assert len(scrolled_points) == total_points, (
        f"After optimization, scroll returned {len(scrolled_points)}, expected {total_points}"
    )

    # Search should return results up to the limit
    search_results = search_points(limit=total_points)
    assert len(search_results) == total_points, (
        f"After optimization, search returned {len(search_results)}, expected {total_points}"
    )

    # Retrieve all point IDs - all should be returned now
    all_ids = list(range(1, total_points + 1))
    retrieved = retrieve_points(all_ids)
    assert len(retrieved) == total_points, (
        f"After optimization, retrieve returned {len(retrieved)}, expected {total_points}"
    )
