import pathlib
import multiprocessing
from time import sleep

from .fixtures import upsert_random_points, create_collection
from .utils import *

from .test_custom_sharding import create_shard

N_PEERS = 1
N_SHARDS = 1
N_REPLICA = 1
COLLECTION_NAME = "test_collection"


def update_points_in_loop(peer_url, collection_name, num_points=None, num_cities=None, shard_key=None, offset=0, throttle=False, duration=None):
    start = time.time()
    limit = 3
    counter = 0

    while True:
        if num_points is not None:
            if counter >= num_points:
                break
            if (num_points - counter) < limit:
                limit = num_points - counter

        upsert_random_points(peer_url, limit, collection_name, num_cities=num_cities, shard_key=shard_key, offset=offset)
        offset += limit
        counter += limit

        if throttle:
            sleep(0.1)
        if duration is not None and (time.time() - start) > duration:
            break


def run_update_points_in_background(peer_url, collection_name, num_points=None, num_cities=None, shard_key=None, init_offset=0, throttle=False, duration=None):
    p = multiprocessing.Process(target=update_points_in_loop, args=(peer_url, collection_name, num_points, num_cities, shard_key, init_offset, throttle, duration))
    p.start()
    return p


# Transfer points from one shard to another
#
# Simply does the most basic transfer: no concurrent updates during the
# transfer.
#
# Test that data on the both sides is consistent
def test_replicate_points_stream_transfer(tmp_path: pathlib.Path):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, 20000)

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

# Replicate points from one shard to another while applying throttled updates in parallel
#
# Updates are throttled to prevent sending updates faster than the queue proxy
# can handle. The transfer must therefore finish in 30 seconds without issues.
#
# Test that data on the both sides is consistent
@pytest.mark.parametrize("override_points", [True, False])
def test_replicate_points_stream_transfer_updates(tmp_path: pathlib.Path, override_points: bool):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, 20000)

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

    # Insert some initial number of points
    upsert_random_points(peer_api_uris[0], 10000, num_cities=2, shard_key="default")

    filter = {"must": {"key": "city", "match": {"value": "London"}}}
    inverse_filter = {"must_not": {"key": "city", "match": {"value": "London"}}}

    original_filtered_count = get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="default", exact=True, filter=filter)
    initial_dest_count = get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="tenant", exact=True)
    assert original_filtered_count > 0
    assert initial_dest_count == 0 # no points in tenant shard key before transfer

    # Start pushing new points to the cluster in parallel (update some old ones + insert new ones)
    num_points_to_override = 10 if override_points else 0
    upload_process_1 = run_update_points_in_background(peer_api_uris[0], COLLECTION_NAME, shard_key="default", init_offset=10000-num_points_to_override, num_points=250, num_cities=2, throttle=False)

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

    # Stop upserts before transfer finishes so we don't push extra points to "default" after transfer
    # TODO: Use "fallback" once PR is merged and kill after transfer is done
    sleep(1)
    upload_process_1.kill()

    # Wait for end of transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    receiver_collection_cluster_info = get_collection_cluster_info(
        peer_api_uris[-1], COLLECTION_NAME
    )
    number_local_shards = len(receiver_collection_cluster_info["local_shards"])
    assert number_local_shards == 2

    # Point counts must be consistent across shard keys for given filter
    src_filtered_count = get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="default", exact=True, filter=filter)
    dest_filtered_count = get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="tenant", exact=True, filter=filter)

    assert dest_filtered_count > original_filtered_count # more points than before due to upserts during transfer
    assert dest_filtered_count == src_filtered_count # new shard should also have the same points

    if override_points is False:
        dest_inverse_count = get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="tenant", exact=True, filter=inverse_filter)
        assert dest_inverse_count == 0 # new shard should have only the points matching the filter
