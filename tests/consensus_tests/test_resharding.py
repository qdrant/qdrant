import multiprocessing
import pathlib
import random
from time import sleep

from .fixtures import upsert_random_points, create_collection
from .utils import *

COLLECTION_NAME = "test_collection"


# Test resharding.
#
# On a static collection, this performs resharding a few times and asserts the
# shard and point counts are correct.
#
# More specifically this starts at 1 shard, reshards 3 times and ends up with 4
# shards.
def test_resharding(tmp_path: pathlib.Path):
    assert_project_root()

    num_points = 1000

    # Prevent optimizers messing with point counts
    env={
        "QDRANT__STORAGE__OPTIMIZERS__INDEXING_THRESHOLD_KB": "0",
    }

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 3, None, extra_env=env)
    first_peer_id = get_cluster_info(peer_api_uris[0])['peer_id']

    # Create collection, insert points
    create_collection(peer_api_uris[0], shard_number=1, replication_factor=3)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )
    upsert_random_points(peer_api_uris[0], num_points)

    sleep(1)

    # Assert node shard and point sum count
    for uri in peer_api_uris:
        assert check_collection_local_shards_count(uri, COLLECTION_NAME, 1)
        assert check_collection_local_shards_point_count(uri, COLLECTION_NAME, num_points)

    # Reshard 3 times in sequence
    for shard_count in range(2, 5):
        # Start resharding
        r = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
                "start_resharding": {
                    "peer_id": first_peer_id,
                    "shard_key": None,
                }
            })
        assert_http_ok(r)

        # Wait for resharding operation to start and stop
        wait_for_collection_resharding_operations_count(peer_api_uris[0], COLLECTION_NAME, 1)
        for uri in peer_api_uris:
            wait_for_collection_resharding_operations_count(uri, COLLECTION_NAME, 0)

        # Assert node shard and point sum count
        for uri in peer_api_uris:
            assert check_collection_local_shards_count(uri, COLLECTION_NAME, shard_count)
            assert check_collection_local_shards_point_count(uri, COLLECTION_NAME, num_points)


# Test resharding shard balancing.
#
# Sets up a 3 node cluster and a collection with 1 shard and 2 replicas.
# Performs resharding 7 times and asserts the shards replicas are evenly
# balanced across all nodes.
#
# In this case the replicas are balanced on the second and third node. The first
# node has all shards because we explicitly set it as shard target all the time.
def test_resharding_balance(tmp_path: pathlib.Path):
    assert_project_root()

    num_points = 100

    # Prevent optimizers messing with point counts
    env={
        "QDRANT__STORAGE__OPTIMIZERS__INDEXING_THRESHOLD_KB": "0",
    }

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 3, None, extra_env=env)
    first_peer_id = get_cluster_info(peer_api_uris[0])['peer_id']

    # Create collection, insert points
    create_collection(peer_api_uris[0], shard_number=1, replication_factor=2)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )
    upsert_random_points(peer_api_uris[0], num_points)

    sleep(1)

    # This test assumes we have a replica for every shard on the first node
    # If that is not the case, move the replica there now
    if get_collection_local_shards_count(peer_api_uris[0], COLLECTION_NAME) == 0:
        second_peer_id = get_cluster_info(peer_api_uris[1])['peer_id']
        r = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
                "move_shard": {
                    "shard_id": 0,
                    "from_peer_id": second_peer_id,
                    "to_peer_id": first_peer_id,
                    "method": "stream_records",
                }
            })
        assert_http_ok(r)
        wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)
        assert check_collection_local_shards_count(peer_api_uris[0], COLLECTION_NAME, 1)

    # Assert node point count
    for uri in peer_api_uris:
        assert get_collection_point_count(uri, COLLECTION_NAME, exact=True) == num_points

    # Reshard 5 times in sequence
    for _shard_count in range(5):
        # Start resharding
        r = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
                "start_resharding": {
                    "peer_id": first_peer_id,
                    "shard_key": None,
                }
            })
        assert_http_ok(r)

        # Wait for resharding operation to start and stop
        wait_for_collection_resharding_operations_count(peer_api_uris[0], COLLECTION_NAME, 1)
        for uri in peer_api_uris:
            wait_for_collection_resharding_operations_count(uri, COLLECTION_NAME, 0)

        # Point count across cluster must be stable
        for uri in peer_api_uris:
            assert get_collection_point_count(uri, COLLECTION_NAME, exact=False) == num_points

    # We must end up with:
    # - 6 shards on first node, it was the resharding target
    # - 3 shards on the other two nodes, 6 replicas balanced over 2 nodes
    assert check_collection_local_shards_count(peer_api_uris[0], COLLECTION_NAME, 6)
    for uri in peer_api_uris[1:]:
        assert check_collection_local_shards_count(uri, COLLECTION_NAME, 3)
