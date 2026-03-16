import random
import time

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import drop_collection

COLLECTION_NAME = "test_deferred_points"
VECTOR_DIM = 256
KEYWORDS = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa"]

# With 256d float32 vectors, each point ≈ 1 KB of vector data.
# indexing_threshold is in KB, so threshold of 100 ≈ 100 points before deferred kicks in.
INDEXING_THRESHOLD_KB = 100


def setup_module():
    drop_collection(COLLECTION_NAME)


def teardown_module():
    drop_collection(COLLECTION_NAME)


def create_collection(prevent_unoptimized=False, max_optimization_threads=None):
    optimizers_config = {
        "indexing_threshold": INDEXING_THRESHOLD_KB,
    }
    if prevent_unoptimized:
        optimizers_config["prevent_unoptimized"] = True
    if max_optimization_threads is not None:
        optimizers_config["max_optimization_threads"] = max_optimization_threads

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': COLLECTION_NAME},
        body={
            "vectors": {
                "size": VECTOR_DIM,
                "distance": "Cosine",
            },
            "optimizers_config": optimizers_config,
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


def get_point_count_excluding_deferred():
    """
    Manually calculate the amount of visible (non-deferred) points for now,
    since we don't provide it in CollectionInfo yet.
    """

    response = request_with_validation(
        api='/telemetry',
        method="GET",
        query_params={"details_level": 10}
    )
    assert response.ok
    collections = response.json()['result']['collections']['collections']

    num_points = 0
    had_collection = False
    for collection in collections:
        if collection['id'] != COLLECTION_NAME:
            continue

        had_collection = True

        for shard in collection['shards']:
            if 'local' not in shard:
                continue

            for segment in shard['local']['segments']:
                info = segment['info']
                num_points += int(info['num_points']) - int(info['num_deferred_points'])

    assert had_collection, "Collection not found in telemetry!"

    return num_points


def upsert_points_batch(points, wait=True):
    """Upsert a list of points in a single request."""
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': COLLECTION_NAME},
        query_params={'wait': 'true' if wait else 'false'},
        body={"points": points},
    )
    assert response.ok


def make_points(start_id, count, payload_fn=None):
    """Generate a list of points with sequential IDs and random vectors."""
    random.seed(start_id)
    points = []
    for i in range(count):
        point_id = start_id + i
        vector = [random.random() for _ in range(VECTOR_DIM)]
        if payload_fn:
            payload = payload_fn(point_id)
        else:
            payload = {
                "keyword": KEYWORDS[point_id % len(KEYWORDS)],
                "score": point_id * 0.1,
            }
        points.append({"id": point_id, "vector": vector, "payload": payload})
    return points


def upsert_points(start_id, count, wait=True, payload_fn=None):
    """Upsert points in a single batch."""
    points = make_points(start_id, count, payload_fn)
    upsert_points_batch(points, wait=wait)


def set_payload(point_ids, payload, wait=True):
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="POST",
        path_params={'collection_name': COLLECTION_NAME},
        query_params={'wait': 'true' if wait else 'false'},
        body={
            "payload": payload,
            "points": point_ids,
        }
    )
    assert response.ok


def scroll_all_points():
    all_points = []
    offset = None
    while True:
        body = {"limit": 100, "with_vector": False, "with_payload": True}
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
        body={"ids": ids, "with_payload": True},
    )
    assert response.ok
    return response.json()['result']


def search_points(limit=10):
    random.seed(0)
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': COLLECTION_NAME},
        body={
            "vector": [random.random() for _ in range(VECTOR_DIM)],
            "limit": limit,
        }
    )
    assert response.ok
    return response.json()['result']


def update_collection_config(config):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': COLLECTION_NAME},
        body=config,
    )
    assert response.ok


def wait_collection_green(timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        info = get_collection_info()
        if info['status'] == 'green':
            return
        time.sleep(0.5)
    raise Exception(f"Collection did not reach green status within {timeout}s")


def create_field_index(field_name, field_schema):
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': COLLECTION_NAME},
        query_params={'wait': 'true'},
        body={
            "field_name": field_name,
            "field_schema": field_schema,
        }
    )
    assert response.ok


def test_deferred_points():
    # Create collection with prevent_unoptimized and optimizers disabled.
    # deferred_internal_id is only set at segment creation time, so prevent_unoptimized
    # must be enabled before any appendable segments are created.
    # With 256d float32 vectors, each point ≈ 1 KB of vector data.
    # indexing_threshold=100 KB → deferred_internal_id ≈ 100 points.
    #
    # Note: internal offsets are assigned in AHashMap iteration order (not external ID order),
    # so we cannot predict which external IDs map to which internal offsets.
    # All assertions use counts rather than specific IDs.
    create_collection(prevent_unoptimized=True, max_optimization_threads=0)

    # Create payload indexes
    create_field_index("keyword", "keyword")
    create_field_index("score", "float")

    total_upserted = 2000

    # --- Phase 1: Upsert 2000 points into fresh appendable segment ---
    # With optimizers disabled, points stay in the unoptimized appendable segment.
    # ~100 points (those with internal offset < threshold) are visible, the rest are deferred.
    all_ids = list(range(1, total_upserted + 1))
    upsert_points_batch(make_points(1, total_upserted), wait=False)
    time.sleep(2)

    # Scroll should only return non-deferred points (~100 out of 2000)
    scrolled = scroll_all_points()
    visible_count = len(scrolled)
    assert visible_count > 0, "Some points should be scrollable (within threshold)"
    assert visible_count < total_upserted, (
        f"Not all points should be scrollable (most are deferred), got {visible_count}"
    )
    visible_ids = {p['id'] for p in scrolled}

    # Point count from collection info should match scrolled count
    # info = get_collection_info()
    point_count = get_point_count_excluding_deferred()
    assert point_count == visible_count, (
        f"points_count ({point_count}) should match scrolled count ({visible_count})"
    )

    # Retrieve all 2000 IDs: only non-deferred ones should be returned
    retrieved = retrieve_points(all_ids)
    assert len(retrieved) == visible_count, (
        f"Retrieve should return {visible_count} non-deferred points, got {len(retrieved)}"
    )
    retrieved_ids = {p['id'] for p in retrieved}
    assert retrieved_ids == visible_ids, "Retrieved IDs should match scrolled IDs"

    # Search should only find non-deferred points
    search_results = search_points(limit=total_upserted)
    search_ids = {r['id'] for r in search_results}
    deferred_ids = set(all_ids) - visible_ids
    assert len(search_ids & deferred_ids) == 0, (
        "Deferred points should not appear in search results"
    )

    # --- Phase 2: Set payload on a deferred point — should have no effect ---
    # Pick a point that we know is deferred
    deferred_point_id = next(iter(deferred_ids))
    set_payload([deferred_point_id], {"keyword": "deferred_modified"}, wait=False)
    time.sleep(1)
    retrieved = retrieve_points([deferred_point_id])
    assert len(retrieved) == 0, (
        "Deferred point should not be retrievable even after set_payload"
    )

    # --- Phase 3: Add more points that land in the deferred section ---
    new_points_start = total_upserted + 1
    new_points_count = 1000
    upsert_points_batch(make_points(new_points_start, new_points_count), wait=False)
    time.sleep(2)

    # These new points should also be deferred (added beyond the threshold)
    point_count = get_point_count_excluding_deferred()
    assert point_count == visible_count, (
        f"Expected {visible_count} visible points (new points deferred), got {point_count}"
    )

    new_point_ids = set(range(new_points_start, new_points_start + new_points_count))
    scrolled = scroll_all_points()
    scrolled_ids = {p['id'] for p in scrolled}
    assert len(scrolled_ids & new_point_ids) == 0, (
        "New deferred points should not be scrollable"
    )

    # --- Phase 4: Enable optimizers and wait for optimization ---
    update_collection_config({
        "optimizers_config": {
            "max_optimization_threads": "auto",
        },
    })

    # Trigger optimization with a small upsert
    trigger_id = new_points_start + new_points_count
    upsert_points(start_id=trigger_id, count=1, wait=True)
    wait_collection_green()

    # After optimization, ALL points should be visible
    expected_total = total_upserted + new_points_count + 1
    point_count = get_point_count_excluding_deferred()
    assert point_count == expected_total, (
        f"After optimization, expected {expected_total} points, got {point_count}"
    )

    # The deferred set_payload should now be visible
    retrieved = retrieve_points([deferred_point_id])
    assert len(retrieved) == 1
    assert retrieved[0]['payload']['keyword'] == 'deferred_modified', (
        f"After optimization, deferred set_payload should be visible, "
        f"got keyword={retrieved[0]['payload']['keyword']}"
    )

    # All new points should now be scrollable
    scrolled = scroll_all_points()
    scrolled_ids = {p['id'] for p in scrolled}
    assert new_point_ids.issubset(scrolled_ids), (
        "After optimization, all previously deferred points should be scrollable"
    )
