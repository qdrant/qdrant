import pathlib

from .fixtures import upsert_random_points, create_collection, random_dense_vector, random_sparse_vector
from .utils import *

from .test_custom_sharding import create_shard

N_PEERS = 1
N_SHARDS = 1
N_REPLICA = 1
COLLECTION_NAME = "test_collection"


def city_by_id(point_id):
    # City is a pure function of the point ID, so the set of points matching
    # a city filter is known exactly at any moment, no matter how points are
    # overwritten concurrently.
    return "London" if point_id % 2 == 0 else "Berlin"


def upsert_points_by_id(peer_url, ids, collection_name=COLLECTION_NAME, shard_key=None, batch_size=5):
    for batch_start in range(0, len(ids), batch_size):
        batch = ids[batch_start:batch_start + batch_size]
        r = requests.put(
            f"{peer_url}/collections/{collection_name}/points?wait=true",
            json={
                "points": [
                    {
                        "id": point_id,
                        "vector": {
                            "": random_dense_vector(),
                            "sparse-text": random_sparse_vector(),
                        },
                        "payload": {"city": city_by_id(point_id)},
                    }
                    for point_id in batch
                ],
                "shard_key": shard_key,
            },
        )
        assert_http_ok(r)


def scroll_all_ids(peer_url, collection_name, shard_key, filter=None):
    ids = set()
    offset = None
    while True:
        r = requests.post(
            f"{peer_url}/collections/{collection_name}/points/scroll",
            json={
                "limit": 10000,
                "offset": offset,
                "with_payload": False,
                "with_vector": False,
                "shard_key": shard_key,
                "filter": filter,
            },
        )
        assert_http_ok(r)
        result = r.json()["result"]
        ids.update(point["id"] for point in result["points"])
        offset = result["next_page_offset"]
        if offset is None:
            return ids


# Transfer points from one shard to another
#
# Simply does the most basic transfer: no concurrent updates during the
# transfer.
#
# Test that data on the both sides is consistent
def test_replicate_points_stream_transfer(tmp_path: pathlib.Path):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(
        peer_api_uris[0],
        sharding_method="custom",
        shard_number=N_SHARDS,
        replication_factor=N_REPLICA,
    )
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )

    create_shard(
        peer_api_uris[0], COLLECTION_NAME, shard_key="default", shard_number=N_SHARDS
    )
    create_shard(
        peer_api_uris[0], COLLECTION_NAME, shard_key="tenant", shard_number=N_SHARDS
    )

    # Insert some initial number of points
    upsert_random_points(peer_api_uris[0], 100, shard_key="default")

    filter = {"must": {"key": "city", "match": {"value": "London"}}}
    inverse_filter = {"must_not": {"key": "city", "match": {"value": "London"}}}

    expected_count = get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="default", exact=True, filter=filter)
    initial_count = get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="tenant", exact=True)
    assert expected_count > 0
    assert initial_count == 0 # no points in tenant shard key before transfer

    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster",
        json={
            "replicate_points": {
                "filter": filter,
                "from_shard_key": "default",
                "to_shard_key": "tenant",
            }
        },
    )
    assert_http_ok(r)

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    receiver_collection_cluster_info = get_collection_cluster_info(
        peer_api_uris[-1], COLLECTION_NAME
    )
    number_local_shards = len(receiver_collection_cluster_info["local_shards"])
    assert number_local_shards == 2

    # Point counts must be consistent across shard keys for given filter
    default_filtered_count = get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="default", exact=True, filter=filter)
    tenant_count = get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="tenant", exact=True)
    tenant_inverse_count = get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="tenant", exact=True, filter=inverse_filter)
    assert default_filtered_count == expected_count # original shard should remain unchanged
    assert tenant_count == expected_count # new shard should also have the points
    assert tenant_inverse_count == 0 # new shard should have only the points matching the filter

# Replicate points from one shard to another while applying updates in parallel
#
# Points are assigned a city deterministically by ID parity, so the exact set
# of points matching the city filter is known at all times and all assertions
# are exact set comparisons: no slack for concurrent filter membership changes
# is needed.
#
# The transfer streams the source shard in ascending point ID order. Initial
# points occupy IDs [100, 10100), leaving low IDs unoccupied: points inserted
# at IDs [0, 30) during the transfer are behind the stream cursor and can only
# reach the destination through live update forwarding, so this test fails if
# forwarding silently stops working. Overridden and appended points cover the
# update-to-streamed-point and insert-ahead-of-cursor paths respectively.
@pytest.mark.parametrize("override_points", [True, False])
def test_replicate_points_stream_transfer_updates(tmp_path: pathlib.Path, override_points: bool):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(
        peer_api_uris[0],
        sharding_method="custom",
        shard_number=N_SHARDS,
        replication_factor=N_REPLICA,
        indexing_threshold=None,
    )
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )

    create_shard(
        peer_api_uris[0], COLLECTION_NAME, shard_key="default", shard_number=N_SHARDS
    )
    create_shard(
        peer_api_uris[0], COLLECTION_NAME, shard_key="tenant", shard_number=N_SHARDS
    )

    # Insert initial points in a single request, like a regular bulk upload
    initial_ids = list(range(100, 10100))
    upsert_points_by_id(peer_api_uris[0], initial_ids, shard_key="default", batch_size=len(initial_ids))

    filter = {"must": {"key": "city", "match": {"value": "London"}}}

    assert get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="tenant", exact=True) == 0

    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster",
        json={
            "replicate_points": {
                "filter": filter,
                "from_shard_key": "default",
                "to_shard_key": "tenant",
            }
        },
    )
    assert_http_ok(r)

    # Concurrent updates, applied synchronously with wait=true while the
    # transfer streams the 10k initial points:
    # - overrides at the stream head: the stream passes IDs 100..109 within the
    #   first batch, so these updates reach the destination through forwarding
    # - inserts below the stream cursor: deliverable only through forwarding
    # - inserts above the stream cursor: delivered by the stream or forwarding
    override_ids = list(range(100, 110)) if override_points else []
    low_insert_ids = list(range(0, 30))
    high_insert_ids = list(range(10100, 10130))
    written_ids = override_ids + low_insert_ids + high_insert_ids
    upsert_points_by_id(peer_api_uris[0], written_ids, shard_key="default")

    # Precondition for the exact assertions below: every write above must have
    # gone through the transfer proxy. A handful of small requests cannot
    # realistically outlast streaming 10k points on the same node; if this
    # ever fires, the test did not exercise concurrent updates at all.
    assert get_shard_transfer_count(peer_api_uris[0], COLLECTION_NAME) > 0, \
        "transfer finished before the concurrent writes completed; increase the initial point count"

    # Wait for end of transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    receiver_collection_cluster_info = get_collection_cluster_info(
        peer_api_uris[-1], COLLECTION_NAME
    )
    number_local_shards = len(receiver_collection_cluster_info["local_shards"])
    assert number_local_shards == 2

    all_ids = set(initial_ids) | set(written_ids)
    expected_london_ids = {point_id for point_id in all_ids if city_by_id(point_id) == "London"}

    src_ids = scroll_all_ids(peer_api_uris[0], COLLECTION_NAME, shard_key="default")
    src_london_ids = scroll_all_ids(peer_api_uris[0], COLLECTION_NAME, shard_key="default", filter=filter)
    dest_ids = scroll_all_ids(peer_api_uris[0], COLLECTION_NAME, shard_key="tenant")

    # Source contains every written point, and its filtered view is exact
    assert src_ids == all_ids
    assert src_london_ids == expected_london_ids

    # Destination contains exactly the matching points and nothing else:
    # no point lost (streamed, forwarded override, forwarded low-ID insert),
    # no non-matching point leaked through the stream or forwarding
    assert dest_ids == expected_london_ids
