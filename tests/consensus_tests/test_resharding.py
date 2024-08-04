import multiprocessing
import pathlib
import random
from time import sleep

from .test_dummy_shard import assert_http_response

from .fixtures import upsert_random_points, create_collection, random_dense_vector
from .utils import *

COLLECTION_NAME = "test_collection"


# Test resharding.
#
# On a static collection, this performs resharding up and down a few times and
# asserts the shard and point counts are correct.
#
# More specifically this starts at 1 shard, reshards 3 times to 4 shards, and
# reshards 3 times back to 1 shard.
@pytest.mark.skip(reason="moving resharding driver to external service")
def test_resharding(tmp_path: pathlib.Path):
    assert_project_root()

    num_points = 1000

    # Prevent optimizers messing with point counts
    env={
        "QDRANT__STORAGE__OPTIMIZERS__INDEXING_THRESHOLD_KB": "0",
    }

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 3, None, extra_env=env)

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

    # We cannot reshard down now, because we only have one shard
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
            "start_resharding": {
                "direction": "down"
            }
        })
    assert r.status_code == 400
    assert r.json()["status"]["error"] == "Bad request: cannot remove shard 0 by resharding down, it is the last shard"

    # Reshard up 3 times in sequence
    for shard_count in range(2, 5):
        # Start resharding
        r = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
                "start_resharding": {
                    "direction": "up"
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

    sleep(1)

    # Match all points on all nodes exactly
    data = []
    for uri in peer_api_uris:
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/scroll", json={
                "limit": 999999999,
                "with_vectors": True,
                "with_payload": True,
            }
        )
        assert_http_ok(r)
        data.append(r.json()["result"])
    check_data_consistency(data)

    # Reshard down 3 times in sequence
    for shard_count in range(3, 0, -1):
        # Start resharding
        r = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
                "start_resharding": {
                    "direction": "down"
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

    sleep(1)

    # Match all points on all nodes exactly
    data = []
    for uri in peer_api_uris:
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/scroll", json={
                "limit": 999999999,
                "with_vectors": True,
                "with_payload": True,
            }
        )
        assert_http_ok(r)
        data.append(r.json()["result"])
    check_data_consistency(data)


# Test resharding shard balancing.
#
# Sets up a 3 node cluster and a collection with 1 shard and 2 replicas.
# Performs resharding 7 times and asserts the shards replicas are evenly
# balanced across all nodes.
#
# In this case the replicas are balanced on the second and third node. The first
# node has all shards because we explicitly set it as shard target all the time.
@pytest.mark.skip(reason="moving resharding driver to external service")
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
                    "direction": "up",
                    "peer_id": first_peer_id
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


# Test resharding with concurrent updates.
#
# This performs resharding a few times while sending point updates to all peers
# concurrently. At the end of the whole process it asserts the expected point
# count.
#
# The concurrent update tasks consist of:
# - 3 threads upserting new points on all peers
# - 1 threads updating existing points on the first peer
# - 2 threads deleting points on the first two peers
@pytest.mark.skip(reason="moving resharding driver to external service")
def test_resharding_concurrent_updates(tmp_path: pathlib.Path):
    assert_project_root()

    num_points = 1000
    num_inserts = 1000
    num_updates = 500
    num_deletes = 33

    # Prevent optimizers messing with point counts
    env={
        "QDRANT__STORAGE__OPTIMIZERS__INDEXING_THRESHOLD_KB": "0",
    }

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 3, None, extra_env=env)

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

    # During resharding, keep pushing updates into the collection
    update_tasks = [
        # Upsert new points on all peers
        run_in_background(upsert_points_throttled, peer_api_uris[0], COLLECTION_NAME, start=10000, end=10000 + num_inserts),
        run_in_background(upsert_points_throttled, peer_api_uris[1], COLLECTION_NAME, start=20000, end=20000 + num_inserts),
        run_in_background(upsert_points_throttled, peer_api_uris[2], COLLECTION_NAME, start=30000, end=30000 + num_inserts),
        # Update existing points on the first peer
        run_in_background(upsert_points_throttled, peer_api_uris[0], COLLECTION_NAME, start=0, end=num_updates),
        # Delete points on the first two peers, don't overlap with updates
        run_in_background(delete_points_throttled, peer_api_uris[0], COLLECTION_NAME, start=num_updates, end=num_updates + num_deletes),
        run_in_background(delete_points_throttled, peer_api_uris[1], COLLECTION_NAME, start=num_updates + num_deletes, end=num_updates + num_deletes * 2),
    ]

    # Reshard 3 times in sequence
    for shard_count in range(2, 5):
        # Start resharding
        r = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
                "start_resharding": {
                    "direction": "up"
                }
            })
        assert_http_ok(r)

        # Wait for resharding operation to start and stop
        wait_for_collection_resharding_operations_count(peer_api_uris[0], COLLECTION_NAME, 1)
        for uri in peer_api_uris:
            wait_for_collection_resharding_operations_count(uri, COLLECTION_NAME, 0, wait_for_timeout=120)

        # Assert node shard count
        for uri in peer_api_uris:
            assert check_collection_local_shards_count(uri, COLLECTION_NAME, shard_count)

    # Wait for background updates to finish
    for task in update_tasks:
        while task.is_alive():
            pass

    # Assert node shard and point sum count
    # Expects base points + 3x upserts - 2x deletes
    expected_points = num_points + num_inserts * 3 - num_deletes * 2
    for uri in peer_api_uris:
        assert check_collection_local_shards_point_count(uri, COLLECTION_NAME, expected_points)

    sleep(1)

    # Match all points on all nodes exactly
    # Note: due to concurrent updates on all peers this check may fail, but I've
    # not seen this yet. Once it does, we probably want to remove this.
    data = []
    for uri in peer_api_uris:
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/scroll", json={
                "limit": 999999999,
                "with_vectors": True,
                "with_payload": True,
            }
        )
        assert_http_ok(r)
        data.append(r.json()["result"])
    check_data_consistency(data)


# Test point count during resharding.
#
# On a static collection, this performs resharding a few times and asserts the
# exact point count remains stable on all peers during the whole process.
@pytest.mark.skip(reason="moving resharding driver to external service")
def test_resharding_stable_point_count(tmp_path: pathlib.Path):
    assert_project_root()

    num_points = 1000

    # Prevent optimizers messing with point counts
    env={
        "QDRANT__STORAGE__OPTIMIZERS__INDEXING_THRESHOLD_KB": "0",
    }

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 3, None, extra_env=env)

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
                    "direction": "up"
                }
            })
        assert_http_ok(r)

        # Wait for resharding to start
        wait_for_collection_resharding_operations_count(peer_api_uris[0], COLLECTION_NAME, 1)

        # Continuously assert point count on all peers, must be stable
        # Stop once all peers reported completed resharding
        while True:
            for uri in peer_api_uris:
                assert get_collection_point_count(uri, COLLECTION_NAME, exact=True) == num_points
                cardinality_count = get_collection_point_count(uri, COLLECTION_NAME, exact=False)
                assert cardinality_count >= num_points / 2 and cardinality_count < num_points * 2

            all_completed = True
            for uri in peer_api_uris:
                if not check_collection_resharding_operations_count(uri, COLLECTION_NAME, 0):
                    all_completed = False
                    break
            if all_completed:
                break

        # Assert node shard count
        for uri in peer_api_uris:
            assert check_collection_local_shards_count(uri, COLLECTION_NAME, shard_count)

    sleep(1)

    # Match all points on all nodes exactly
    data = []
    for uri in peer_api_uris:
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/scroll", json={
                "limit": 999999999,
                "with_vectors": True,
                "with_payload": True,
            }
        )
        assert_http_ok(r)
        data.append(r.json()["result"])
    check_data_consistency(data)


# Test point count during resharding and indexing.
#
# On a static collection, this performs resharding and indexing a few times and
# asserts the exact point count remains stable on all peers during the whole
# process.
@pytest.mark.skip(reason="moving resharding driver to external service")
def test_resharding_indexing_stable_point_count(tmp_path: pathlib.Path):
    assert_project_root()

    num_points = 1000

    # Configure optimizers to index right away with a low vector count
    env={
        "QDRANT__STORAGE__OPTIMIZERS__INDEXING_THRESHOLD_KB": "1",
    }

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 3, None, extra_env=env)

    # Create collection, insert points
    create_collection(peer_api_uris[0], shard_number=1, replication_factor=3)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )
    upsert_random_points(peer_api_uris[0], num_points)

    sleep(1)

    # Assert node shard and exact point count
    for uri in peer_api_uris:
        assert check_collection_local_shards_count(uri, COLLECTION_NAME, 1)
        assert get_collection_point_count(uri, COLLECTION_NAME, exact=True) == num_points

    # Reshard 3 times in sequence
    for shard_count in range(2, 5):
        # Start resharding
        r = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
                "start_resharding": {
                    "direction": "up"
                }
            })
        assert_http_ok(r)

        # Wait for resharding operation to start and stop
        wait_for_collection_resharding_operations_count(peer_api_uris[0], COLLECTION_NAME, 1)

        # Continuously assert exact point count on all peers, must be stable
        # Stop once all peers reported completed resharding
        while True:
            for uri in peer_api_uris:
                assert get_collection_point_count(uri, COLLECTION_NAME, exact=True) == num_points

            all_completed = True
            for uri in peer_api_uris:
                if not check_collection_resharding_operations_count(uri, COLLECTION_NAME, 0):
                    all_completed = False
                    break
            if all_completed:
                break

        # Assert node shard count
        for uri in peer_api_uris:
            assert check_collection_local_shards_count(uri, COLLECTION_NAME, shard_count)

    # Wait for optimizations to complete
    for uri in peer_api_uris:
        wait_collection_green(uri, COLLECTION_NAME)

    # Assert exact point count one more time
    for uri in peer_api_uris:
        assert get_collection_point_count(uri, COLLECTION_NAME, exact=True) == num_points

    # Match all points on all nodes exactly
    data = []
    for uri in peer_api_uris:
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/scroll", json={
                "limit": 999999999,
                "with_vectors": True,
                "with_payload": True,
            }
        )
        assert_http_ok(r)
        data.append(r.json()["result"])
    check_data_consistency(data)


# Test point scroll stability during resharding.
#
# On a static collection, this performs resharding a few times and asserts
# scrolling remains stable on all peers during the whole process.
@pytest.mark.skip(reason="moving resharding driver to external service")
def test_resharding_stable_scroll(tmp_path: pathlib.Path):
    assert_project_root()

    num_points = 1000
    scroll_limit = 25

    # Prevent optimizers messing with point counts
    env={
        "QDRANT__STORAGE__OPTIMIZERS__INDEXING_THRESHOLD_KB": "0",
    }

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 3, None, extra_env=env)

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

    # Match scroll sample of points on all nodes exactly
    data = []
    for uri in peer_api_uris:
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/scroll", json={
                "limit": scroll_limit,
                "with_vectors": True,
                "with_payload": False,
            }
        )
        assert_http_ok(r)
        data.append(r.json()["result"])
    check_data_consistency(data)

    # Reshard 3 times in sequence
    for shard_count in range(2, 5):
        # Start resharding
        r = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
                "start_resharding": {
                    "direction": "up"
                }
            })
        assert_http_ok(r)

        # Wait for resharding to start
        wait_for_collection_resharding_operations_count(peer_api_uris[0], COLLECTION_NAME, 1)

        # Continuously assert point scroll samples on all peers, must be stable
        # Stop once all peers reported completed resharding
        while True:
            # Match scroll sample of points on all nodes exactly
            data = []
            for uri in peer_api_uris:
                r = requests.post(
                    f"{uri}/collections/{COLLECTION_NAME}/points/scroll", json={
                        "limit": scroll_limit,
                        "with_vectors": True,
                        "with_payload": False,
                    }
                )
                assert_http_ok(r)
                data.append(r.json()["result"])
            check_data_consistency(data)

            all_completed = True
            for uri in peer_api_uris:
                if not check_collection_resharding_operations_count(uri, COLLECTION_NAME, 0):
                    all_completed = False
                    break
            if all_completed:
                break

        # Assert node shard count
        for uri in peer_api_uris:
            assert check_collection_local_shards_count(uri, COLLECTION_NAME, shard_count)


# Test point query stability during resharding.
#
# On a static collection, this performs resharding a few times and asserts
# query remains stable on all peers during the whole process.
@pytest.mark.skip(reason="moving resharding driver to external service")
def test_resharding_stable_query(tmp_path: pathlib.Path):
    assert_project_root()

    num_points = 1000
    query_limit = 10

    # Prevent optimizers messing with point counts
    env={
        "QDRANT__STORAGE__OPTIMIZERS__INDEXING_THRESHOLD_KB": "0",
    }

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 3, None, extra_env=env)

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

    # Match search sample of points on all nodes exactly
    data = []
    search_vector = random_dense_vector()
    for uri in peer_api_uris:
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/query", json={
                "vector": search_vector,
                "limit": query_limit,
            }
        )
        assert_http_ok(r)
        data.append(r.json()["result"])
    check_query_consistency(data)

    # Reshard 3 times in sequence
    for shard_count in range(2, 5):
        # Start resharding
        r = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
                "start_resharding": {
                    "direction": "up"
                }
            })
        assert_http_ok(r)

        # Wait for resharding to start
        wait_for_collection_resharding_operations_count(peer_api_uris[0], COLLECTION_NAME, 1)

        # Continuously assert point search samples on all peers, must be stable
        # Stop once all peers reported completed resharding
        while True:
            # Match search sample of points on all nodes exactly
            data = []
            search_vector = random_dense_vector()
            for uri in peer_api_uris:
                r = requests.post(
                    f"{uri}/collections/{COLLECTION_NAME}/points/query", json={
                        "vector": search_vector,
                        "limit": query_limit,
                    }
                )
                assert_http_ok(r)
                data.append(r.json()["result"])
            check_query_consistency(data)

            all_completed = True
            for uri in peer_api_uris:
                if not check_collection_resharding_operations_count(uri, COLLECTION_NAME, 0):
                    all_completed = False
                    break
            if all_completed:
                break

        # Assert node shard count
        for uri in peer_api_uris:
            assert check_collection_local_shards_count(uri, COLLECTION_NAME, shard_count)


# Test resharding resumption on restart at various stages.
#
# On a static collection, this performs resharding. It kills and restarts the
# driving peer at various stages. On restart, it should finish resharding as if
# nothing happened.
@pytest.mark.skip(reason="moving resharding driver to external service")
def test_resharding_resume_on_restart(tmp_path: pathlib.Path):
    assert_project_root()

    num_points = 2500

    # Stages at which we interrupt and restart resharding
    # We'd like to interrupt at other stages too, but they are too quick for this test to catch them
    interrupt_stages = ["migrate points", "replicate"]

    # Prevent optimizers messing with point counts
    env={
        "QDRANT__STORAGE__OPTIMIZERS__INDEXING_THRESHOLD_KB": "0",
    }

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3, None, extra_env=env)
    first_peer_process = processes.pop(0)
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

    # Start resharding
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
            "start_resharding": {
                "direction": "up",
                "peer_id": first_peer_id
            }
        })
    assert_http_ok(r)

    # Interrupt the resharding node once at each stage
    for stage in interrupt_stages:
        # Wait for resharding operation to start and migrate points
        wait_for_collection_resharding_operations_count(peer_api_uris[0], COLLECTION_NAME, 1)
        wait_for_collection_resharding_operation_stage(peer_api_uris[0], COLLECTION_NAME, stage)

        # Kill and restart first peer
        first_peer_process.kill()
        sleep(1)
        peer_api_uris[0] = start_peer(peer_dirs[0], "peer_0_restarted.log", bootstrap_uri, extra_env=env)
        first_peer_process = processes.pop()
        wait_for_peer_online(peer_api_uris[0], "/")

    # Wait for resharding operation to start and stop
    wait_for_collection_resharding_operations_count(peer_api_uris[0], COLLECTION_NAME, 1)
    for uri in peer_api_uris:
        wait_for_collection_resharding_operations_count(uri, COLLECTION_NAME, 0)

    # Assert node shard and point sum count
    for uri in peer_api_uris:
        assert check_collection_local_shards_count(uri, COLLECTION_NAME, 2)
        assert get_collection_point_count(uri, COLLECTION_NAME, exact=True) == num_points

    sleep(1)

    # Match all points on all nodes exactly
    data = []
    for uri in peer_api_uris:
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/scroll", json={
                "limit": 999999999,
                "with_vectors": True,
                "with_payload": True,
            }
        )
        assert_http_ok(r)
        data.append(r.json()["result"])
    check_data_consistency(data)


# Test that resharding can be aborted (before it reached `WriteHashRingCommitted` stage)
@pytest.mark.skip(reason="seems like a deadlock is sometimes possible during explicit (?) abort, so the test is disabled until deadlock is fixed, to reduce flakiness")
def test_resharding_abort(tmp_path: pathlib.Path):
    peer_api_uris, peer_ids = bootstrap_resharding(tmp_path)

    # Abort resharding
    resp = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
            "abort_resharding": {}
        }
    )

    assert_http_ok(resp)

    # Wait for resharding to abort
    wait_for_resharding_to_finish(peer_api_uris, 3)

# Test that resharding *can't* be aborted, once it reached `WriteHashRingCommitted` stage
@pytest.mark.skip(reason="flaky")
def test_resharding_try_abort_after_write_hash_ring_committed(tmp_path: pathlib.Path):
    peer_api_uris, peer_ids = bootstrap_resharding(tmp_path)

    # Wait for `propagate deletes` resharding stage
    wait_for_one_of_resharding_operation_stages(
        peer_api_uris[0],
        [
            'commit write hash ring',
            'propagate deletes',
            'finalize',
        ],
        wait_for_interval=0.125,
    )

    # Try to abort resharding
    resp = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
            "abort_resharding": {}
        }
    )

    assert resp.status_code == 400

    # Wait for resharding to finish successfully
    wait_for_resharding_to_finish(peer_api_uris, 4)

# Test that resharding is automatically aborted, when collection is deleted
@pytest.mark.skip(reason="flaky")
def test_resharding_abort_on_delete_collection(tmp_path: pathlib.Path):
    peer_api_uris, peer_ids = bootstrap_resharding(tmp_path)

    # Delete collection
    resp = requests.delete(f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}")
    assert_http_ok(resp)

    # TODO: Check... *something*? What? 🤔

# Test that resharding is automatically aborted, when custom shard key is deleted
@pytest.mark.skip(reason="flaky")
def test_resharding_abort_on_delete_shard_key(tmp_path: pathlib.Path):
    peer_api_uris, peer_ids = bootstrap_resharding(
        tmp_path,
        shard_keys=["custom_shard_key_1", "custom_shard_key_2"],
        resharding_shard_key="custom_shard_key_2",
    )

    # Delete shard key
    resp = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/shards/delete", json={
            "shard_key": "custom_shard_key_2",
        }
    )

    assert_http_ok(resp)

    # Wait for resharding to abort (!?)
    wait_for_resharding_to_finish(peer_api_uris, 3)

# Test that resharding is automatically aborted, when we force-remove resharding peer
@pytest.mark.skip(reason="flaky")
def test_resharding_abort_on_remove_peer(tmp_path: pathlib.Path):
    # Place resharding shard on the *last* peer for this test, so that the first peer would still
    # be available, after we remove *resharding* peer...
    peer_api_uris, peer_ids = bootstrap_resharding(tmp_path, replication_peer_idx=-1)

    # Remove peer
    resp = requests.delete(f"{peer_api_uris[0]}/cluster/peer/{peer_ids[-1]}?force=true")
    assert_http_ok(resp)

    # Wait for resharding to abort
    wait_for_resharding_to_finish(peer_api_uris[:-1], 3)

# Test that resharding is automatically restarted, when we force-remove a peer,
# that receives a *replica* of the new shard during replication
@pytest.mark.skip(reason="flaky")
def test_resharding_restart_on_remove_peer_during_replicate(tmp_path: pathlib.Path):
    peer_api_uris, peer_ids = bootstrap_resharding(tmp_path)

    # Wait for `stream_records` shard transfer (during `replicate` resharding stage)
    info = wait_for_resharding_shard_transfer_info(peer_api_uris[0], 'replicate', 'stream_records')

    # Select peer to remove
    peer_to_remove = info['to']

    # Remove peer
    resp = requests.delete(f"{peer_api_uris[0]}/cluster/peer/{info['to']}?force=true")
    assert_http_ok(resp)

    # Wait for resharding to restart
    wait_for_collection_resharding_operation_stage(peer_api_uris[0], COLLECTION_NAME, 'migrate points')

    # Select peers that weren't removed
    valid_peer_uris=[]
    for peer_idx in range(0, len(peer_api_uris)):
        if peer_ids[peer_idx] == peer_to_remove:
            continue

        valid_peer_uris.append(peer_api_uris[peer_idx])

    # Wait for resharding to finish successfully
    wait_for_resharding_to_finish(valid_peer_uris, 4)

# Test that new shard *can't* be removed during resharding (before it has been replicated at least once)
@pytest.mark.skip(reason="flaky")
def test_resharding_try_abort_on_remove_shard_before_replicate(tmp_path: pathlib.Path):
    peer_api_uris, peer_ids = bootstrap_resharding(tmp_path)

    # Try to remove new shard (before it has been replicated at least once)
    resp = requests.post(f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
        "drop_replica": {
            "peer_id": peer_ids[0],
            "shard_id": 3,
        }
    })

    assert resp.status_code == 400

# Test that resharding is automatically restarted, when we remove new shard during replication
@pytest.mark.skip(reason="removing transfer source shard is broken at the moment (in general, not just for resharding)")
def test_resharding_restart_on_remove_src_shard_during_replicate(tmp_path: pathlib.Path):
    resharding_restart_on_remove_shard_during_replicate(tmp_path, 'from')

# Test that resharding is automatically restarted, when we remove new shard during replication
@pytest.mark.skip(reason="resharding detects removing destination shard of replication shard transfer as successful transfer")
def test_resharding_restart_on_remove_dst_shard_during_replicate(tmp_path: pathlib.Path):
    resharding_restart_on_remove_shard_during_replicate(tmp_path, 'to')

def resharding_restart_on_remove_shard_during_replicate(tmp_path: pathlib.Path, shard_to_remove: str):
    peer_api_uris, peer_ids = bootstrap_resharding(tmp_path)

    # Wait for `stream_records` shard transfer (during `replicate` resharding stage)
    info = wait_for_resharding_shard_transfer_info(peer_api_uris[0], 'replicate', 'stream_records')

    # Remove replica of the new shard
    resp = requests.post(f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
        "drop_replica": {
            "peer_id": info[shard_to_remove],
            "shard_id": 3,
        }
    })

    assert_http_ok(resp)

    # Wait for resharding to restart and finish successfully
    wait_for_collection_resharding_operation_stage(peer_api_uris[0], COLLECTION_NAME, 'migrate points')
    wait_for_resharding_to_finish(peer_api_uris, 4)


def bootstrap_resharding(
    tmp_path: pathlib.Path,
    shard_keys: list[str] | str | None = None,
    shard_number: int = 3,
    replication_factor: int = 2,
    replication_peer_idx: int = 0,
    resharding_shard_key: str | None = None,
):
    peer_api_uris, peer_ids = bootstrap_cluster(tmp_path, shard_keys=shard_keys)

    # Start resharding
    resp = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
            "start_resharding": {
                "direction": "up",
                "peer_id": peer_ids[replication_peer_idx],
                "shard_key": resharding_shard_key,
            }
        })

    assert_http_ok(resp)

    # Wait for resharding to start
    wait_for_collection_resharding_operations_count(peer_api_uris[0], COLLECTION_NAME, 1)

    return (peer_api_uris, peer_ids)

def bootstrap_cluster(
    tmp_path: pathlib.Path,
    shard_keys: list[str] | str | None = None,
    shard_number: int = 3,
    replication_factor: int = 2,
) -> tuple[list[str], list[str]]:
    assert_project_root()

    num_points = 10000

    # Prevent optimizers messing with point counts
    env={
        "QDRANT__STORAGE__OPTIMIZERS__INDEXING_THRESHOLD_KB": "0",
    }

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 3, None, extra_env=env)

    peer_ids = []
    for peer_uri in peer_api_uris:
        peer_ids.append(get_cluster_info(peer_uri)['peer_id'])

    # Create collection
    create_collection(
        peer_api_uris[0],
        COLLECTION_NAME,
        shard_number,
        replication_factor,
        sharding_method='auto' if shard_keys is None else 'custom',
    )

    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )

    # Create custom shard keys (if required), and upload points to collection
    if type(shard_keys) is not list:
        shard_keys: list[str | None] = [shard_keys]

    for shard_key in shard_keys:
        # Create custom shard key (if required)
        if shard_key is not None:
            resp = requests.put(
                f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/shards", json={
                    "shard_key": shard_key,
                    "shards_number": shard_number,
                    "replication_factor": replication_factor,
                }
            )

            assert_http_ok(resp)

        # Upsert points to collection
        upsert_random_points(peer_api_uris[0], num_points, shard_key=shard_key)

    sleep(1)

    return (peer_api_uris, peer_ids)

def wait_for_one_of_resharding_operation_stages(peer_uri: str, expected_stages: list[str], **kwargs):
    def resharding_operation_stages():
        requests.post(f"{peer_uri}/collections/{COLLECTION_NAME}/points/scroll")

        info = get_collection_cluster_info(peer_uri, COLLECTION_NAME)

        if 'resharding_operations' not in info:
            return False

        for resharding in info['resharding_operations']:
            if not 'comment' in resharding:
                continue

            stage, *_ = resharding['comment'].split(':', maxsplit=1)

            if stage in expected_stages:
                return True

        return False

    wait_for(resharding_operation_stages, **kwargs)

def wait_for_resharding_shard_transfer_info(peer_uri: str, expected_stage: str | None, expected_method: str):
    if expected_stage is not None:
        wait_for_collection_resharding_operation_stage(peer_uri, COLLECTION_NAME, expected_stage)

    wait_for_collection_shard_transfer_method(peer_uri, COLLECTION_NAME, expected_method)

    info = get_collection_cluster_info(peer_uri, COLLECTION_NAME)
    return info['shard_transfers'][0]

def wait_for_resharding_to_finish(peer_uris: list[str], expected_shard_number: int):
    # Wait for resharding to finish
    for peer_uri in peer_uris:
        wait_for_collection_resharding_operations_count(
            peer_uri,
            COLLECTION_NAME,
            0,
            wait_for_timeout=60,
        )

    # Check number of shards in the collection
    for peer_uri in peer_uris:
        resp = get_collection_cluster_info(peer_uri, COLLECTION_NAME)
        assert resp['shard_count'] == expected_shard_number


def run_in_background(run, *args, **kwargs):
    p = multiprocessing.Process(target=run, args=args, kwargs=kwargs)
    p.start()
    return p


def upsert_points_throttled(peer_url, collection_name, start=0, end=None):
    batch_size = 2
    offset = start

    while True:
        count = min(end - offset, batch_size) if end is not None else batch_size
        if count <= 0:
            return

        upsert_random_points(peer_url, count, collection_name, offset=offset)
        offset += count

        sleep(random.uniform(0.01, 0.05))


def delete_points_throttled(peer_url, collection_name, start=0, end=None):
    batch_size = 2
    offset = start

    while True:
        count = min(end - offset, batch_size) if end is not None else batch_size
        if count <= 0:
            return

        r = requests.post(
            f"{peer_url}/collections/{collection_name}/points/delete?wait=true", json={
                "points": list(range(offset, offset + count)),
            }
        )
        assert_http_ok(r)
        offset += count

        sleep(random.uniform(0.04, 0.06))


def check_data_consistency(data):
    assert(len(data) > 1)

    for i in range(len(data) - 1):
        j = i + 1

        data_i = data[i]["points"]
        data_j = data[j]["points"]

        if data_i != data_j:
            ids_i = set(x.id for x in data_i)
            ids_j = set(x.id for x in data_j)

            diff = ids_i - ids_j

            if len(diff) < 100:
                print(f"Diff between {i} and {j}: {diff}")
            else:
                print(f"Diff len between {i} and {j}: {len(diff)}")

            assert False, "Data on all nodes should be consistent"


def check_query_consistency(data):
    assert(len(data) > 1)

    for i in range(len(data) - 1):
        j = i + 1

        data_i = data[i]["points"]
        data_j = data[j]["points"]

        for item in data_i:
            if "version" in item:
                del item["version"]
        for item in data_j:
            if "version" in item:
                del item["version"]

        if data_i != data_j:
            ids_i = set(x["id"] for x in data_i)
            ids_j = set(x["id"] for x in data_j)

            diff = ids_i - ids_j

            if len(diff) < 100:
                print(f"Diff between {i} and {j}: {diff}")
            else:
                print(f"Diff len between {i} and {j}: {len(diff)}")

            assert False, "Query results on all nodes should be consistent"
