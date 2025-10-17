import pathlib
from time import sleep

from .fixtures import upsert_random_points, create_collection
from .utils import *

from .test_custom_sharding import create_shard
from .test_shard_stream_transfer import run_update_points_in_background

N_PEERS = 1
N_SHARDS = 1
N_REPLICA = 1
COLLECTION_NAME = "test_collection"


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

    # Transfer shard from one node to another
    filter = {"should": {"key": "city", "match": {"value": "London"}}}

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
    assert default_filtered_count == expected_count # original shard should remain unchanged
    assert tenant_count == expected_count # new shard should also have the points

# Replicate points from one node to another while applying throttled updates in parallel
#
# Updates are throttled to prevent sending updates faster than the queue proxy
# can handle. The transfer must therefore finish in 30 seconds without issues.
#
# Test that data on the both sides is consistent
@pytest.mark.parametrize("throttle_updates", [True])
def test_replicate_points_stream_transfer_updates(tmp_path: pathlib.Path, throttle_updates: bool):
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

    # Transfer shard from one node to another
    filter = {"should": {"key": "city", "match": {"value": "London"}}}

    original_filtered_count = get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="default", exact=True, filter=filter)
    initial_tenant_count = get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="tenant", exact=True)
    assert original_filtered_count > 0
    assert initial_tenant_count == 0 # no points in tenant shard key before transfer

    # Start pushing new points to the cluster in parallel
    upload_process_1 = run_update_points_in_background(peer_api_uris[0], COLLECTION_NAME, shard_key="default", init_offset=100, throttle=throttle_updates)

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

    # Wait for end of transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    upload_process_1.kill()
    sleep(1)


    receiver_collection_cluster_info = get_collection_cluster_info(
        peer_api_uris[-1], COLLECTION_NAME
    )
    number_local_shards = len(receiver_collection_cluster_info["local_shards"])
    assert number_local_shards == 2

    # Point counts must be consistent across shard keys for given filter
    src_filtered_count = get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="default", exact=True, filter=filter)
    dest_count = get_collection_point_count(peer_api_uris[0], COLLECTION_NAME, shard_key="tenant", exact=True)
    assert src_filtered_count >= original_filtered_count # original shard also got new points
    assert dest_count == src_filtered_count # new shard should also have the same points
