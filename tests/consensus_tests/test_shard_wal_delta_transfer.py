import multiprocessing
import pathlib
import random
from time import sleep

from .fixtures import upsert_random_points, create_collection
from .utils import *

COLLECTION_NAME = "test_collection"


def update_points_in_loop(peer_url, collection_name, offset=0, throttle=False, duration=None):
    start = time.time()
    limit = 3

    while True:
        upsert_random_points(peer_url, limit, collection_name, offset=offset)
        offset += limit

        if throttle:
            sleep(random.uniform(0.4, 0.6))
        if duration is not None and (time.time() - start) > duration:
            break


def run_update_points_in_background(peer_url, collection_name, init_offset=0, throttle=False, duration=None):
    p = multiprocessing.Process(target=update_points_in_loop, args=(peer_url, collection_name, init_offset, throttle, duration))
    p.start()
    return p

def check_data_consistency(data):

    assert(len(data) > 1)

    for i in range(len(data) - 1):
        j = i + 1

        data_i = data[i]
        data_j = data[j]

        if data_i != data_j:
            ids_i = set(x.id for x in data_i)
            ids_j = set(x.id for x in data_j)

            diff = ids_i - ids_j

            if len(diff) < 100:
                print(f"Diff between {i} and {j}: {diff}")
            else:
                print(f"Diff len between {i} and {j}: {len(diff)}")

            assert False, "Data on all nodes should be consistent"


# Test a WAL delta transfer between two that have no difference.
#
# No WAL records will be transferred as the WAL should be empty. We cannot
# assert that property in this test however. It is tested both ways.
#
# Test that data on the both sides is consistent
def test_empty_shard_wal_delta_transfer(tmp_path: pathlib.Path):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 2, 20000)

    create_collection(peer_api_uris[0], shard_number=1, replication_factor=2)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris
    )

    # Insert some initial number of points
    upsert_random_points(peer_api_uris[0], 100)

    cluster_info_0 = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
    cluster_info_1 = get_collection_cluster_info(peer_api_uris[1], COLLECTION_NAME)

    shard_id = cluster_info_0['local_shards'][0]['shard_id']

    # Re-replicate shard from one node to another, should be no diff
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": cluster_info_0['peer_id'],
                "to_peer_id": cluster_info_1['peer_id'],
                "method": "wal_delta",
            }
        })
    assert_http_ok(r)

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    # Assume we got an empty WAL delta here, logging 'Resolved WAL delta that is
    # empty'. But we cannot assert that at this point.

    # Doing it the other way around should result in exactly the same
    r = requests.post(
        f"{peer_api_uris[1]}/collections/{COLLECTION_NAME}/cluster", json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": cluster_info_1['peer_id'],
                "to_peer_id": cluster_info_0['peer_id'],
                "method": "wal_delta",
            }
        })
    assert_http_ok(r)

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[1], COLLECTION_NAME, 0)

    # Assume we got an empty WAL delta here, logging 'Resolved WAL delta that is
    # empty'. But we cannot assert that at this point.

    cluster_info_0 = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
    cluster_info_1 = get_collection_cluster_info(peer_api_uris[1], COLLECTION_NAME)
    assert len(cluster_info_0['local_shards']) == 1
    assert len(cluster_info_1['local_shards']) == 1

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


# Test node recovery with a WAL delta transfer.
#
# The second node is killed while operations are ongoing. We later restart the
# node, and manually trigger rereplication to sync it up again.
#
# Test that data on the both sides is consistent
def test_shard_wal_delta_transfer_manual_recovery(tmp_path: pathlib.Path):
    assert_project_root()

    # Prevent automatic recovery on restarted node, so we can manually recover with a specific transfer method
    env={
        "QDRANT__STORAGE__PERFORMANCE__INCOMING_SHARD_TRANSFERS_LIMIT": "0",
        "QDRANT__STORAGE__PERFORMANCE__OUTGOING_SHARD_TRANSFERS_LIMIT": "0",
    }

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3, 20000, extra_env=env)

    create_collection(peer_api_uris[0], shard_number=1, replication_factor=3)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris
    )

    transfer_collection_cluster_info = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
    receiver_collection_cluster_info = get_collection_cluster_info(peer_api_uris[2], COLLECTION_NAME)

    from_peer_id = transfer_collection_cluster_info['peer_id']
    to_peer_id = receiver_collection_cluster_info['peer_id']

    # Start pushing points to the cluster
    upload_process_1 = run_update_points_in_background(peer_api_uris[0], COLLECTION_NAME, init_offset=100000, throttle=True)
    upload_process_2 = run_update_points_in_background(peer_api_uris[1], COLLECTION_NAME, init_offset=200000, throttle=True)
    upload_process_3 = run_update_points_in_background(peer_api_uris[2], COLLECTION_NAME, init_offset=300000, throttle=True)

    sleep(1)

    # Kill last peer
    upload_process_3.kill()
    processes.pop().kill()

    upsert_random_points(peer_api_uris[0], 100, batch_size=5)

    sleep(3)

    # Restart the peer
    peer_api_uris[-1] = start_peer(peer_dirs[-1], "peer_2_restarted.log", bootstrap_uri, extra_env=env)
    wait_for_peer_online(peer_api_uris[-1], "/")

    # Recover shard with WAL delta transfer
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
            "replicate_shard": {
                "shard_id": 0,
                "from_peer_id": from_peer_id,
                "to_peer_id": to_peer_id,
                "method": "wal_delta",
            }
        })
    assert_http_ok(r)

    # Assert WAL delta transfer progress, and wait for it to finish
    wait_for_collection_shard_transfer_progress(peer_api_uris[0], COLLECTION_NAME, None, 10)
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    # All nodes must have one shard
    for uri in peer_api_uris:
        cluster_info = get_collection_cluster_info(uri, COLLECTION_NAME)
        assert len(cluster_info['local_shards']) == 1

    upload_process_1.kill()
    upload_process_2.kill()
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


# Test node recovery on a chain of nodes with a WAL delta transfer.
#
# We have a cluster with 5 nodes, all are getting insertions.
# We kill the 4th and 5th node at different times.
# We restart them both. The 4th node is recovered first, then the 4th node
# recovers the 5th node, forming a chain.
# We manually trigger rereplication to sync it up again.
#
# Tests that data on all 5 nodes remains consistent.
def test_shard_wal_delta_transfer_manual_recovery_chain(tmp_path: pathlib.Path):
    assert_project_root()

    # Prevent automatic recovery on restarted node, so we can manually recover with a specific transfer method
    env={
        "QDRANT__STORAGE__PERFORMANCE__INCOMING_SHARD_TRANSFERS_LIMIT": "0",
        "QDRANT__STORAGE__PERFORMANCE__OUTGOING_SHARD_TRANSFERS_LIMIT": "0",
    }

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 5, 20000, extra_env=env)

    create_collection(peer_api_uris[0], shard_number=1, replication_factor=5)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris
    )

    peer_cluster_info = [get_collection_cluster_info(uri, COLLECTION_NAME) for uri in peer_api_uris]
    peer_ids = [cluster_info['peer_id'] for cluster_info in peer_cluster_info]

    # Start pushing points to the cluster
    upload_process_1 = run_update_points_in_background(peer_api_uris[0], COLLECTION_NAME, init_offset=100000, throttle=True)
    upload_process_2 = run_update_points_in_background(peer_api_uris[1], COLLECTION_NAME, init_offset=200000, throttle=True)
    upload_process_3 = run_update_points_in_background(peer_api_uris[2], COLLECTION_NAME, init_offset=300000, throttle=True)
    upload_process_4 = run_update_points_in_background(peer_api_uris[3], COLLECTION_NAME, init_offset=400000, throttle=True)
    upload_process_5 = run_update_points_in_background(peer_api_uris[4], COLLECTION_NAME, init_offset=500000, throttle=True)

    sleep(1)

    # Kill 5th peer
    upload_process_5.kill()
    processes.pop().kill()

    sleep(1)

    # Kill 4th peer
    upload_process_4.kill()
    processes.pop().kill()

    upsert_random_points(peer_api_uris[0], 100, batch_size=5)

    sleep(3)

    # Restart 3rd and 4th peer
    peer_api_uris[3] = start_peer(peer_dirs[3], "peer_3_restarted.log", bootstrap_uri, extra_env=env)
    peer_api_uris[4] = start_peer(peer_dirs[4], "peer_4_restarted.log", bootstrap_uri, extra_env=env)
    wait_for_peer_online(peer_api_uris[3], "/")
    wait_for_peer_online(peer_api_uris[4], "/")

    # Recover shard with WAL delta transfer
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
            "replicate_shard": {
                "shard_id": 0,
                "from_peer_id": peer_ids[0],
                "to_peer_id": peer_ids[3],
                "method": "wal_delta",
            }
        })
    assert_http_ok(r)

    # Assert WAL delta transfer progress, and wait for it to finish
    wait_for_collection_shard_transfer_progress(peer_api_uris[0], COLLECTION_NAME, None, 10)
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    # Start inserting into the fourth peer again
    upload_process_4 = run_update_points_in_background(peer_api_uris[3], COLLECTION_NAME, init_offset=600000, throttle=True)

    sleep(1)

    # Recover shard with WAL delta transfer
    r = requests.post(
        f"{peer_api_uris[3]}/collections/{COLLECTION_NAME}/cluster", json={
            "replicate_shard": {
                "shard_id": 0,
                "from_peer_id": peer_ids[3],
                "to_peer_id": peer_ids[4],
                "method": "wal_delta",
            }
        })
    assert_http_ok(r)

    # Assert WAL delta transfer progress, and wait for it to finish
    wait_for_collection_shard_transfer_progress(peer_api_uris[3], COLLECTION_NAME, None, 10)
    wait_for_collection_shard_transfers_count(peer_api_uris[3], COLLECTION_NAME, 0)

    upload_process_1.kill()
    upload_process_2.kill()
    upload_process_3.kill()
    upload_process_4.kill()

    sleep(1)

    # All nodes must have one shard
    for uri in peer_api_uris:
        cluster_info = get_collection_cluster_info(uri, COLLECTION_NAME)
        assert len(cluster_info['local_shards']) == 1

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


# Test aborting and retrying node recovery with a WAL delta transfer.
#
# First we measure the baseline recovery time by killing and recovering
# the last node. Then we kill the last node again and start recovery, but we
# abort recovery in the middle. We rereplicate again to fully recover.
#
# Test that data on the both sides is consistent even if replication was
# aborted.
def test_shard_wal_delta_transfer_abort_and_retry(tmp_path: pathlib.Path):
    assert_project_root()

    # Prevent automatic recovery on restarted node, so we can manually recover with a specific transfer method
    env={
        "QDRANT__STORAGE__PERFORMANCE__INCOMING_SHARD_TRANSFERS_LIMIT": "0",
        "QDRANT__STORAGE__PERFORMANCE__OUTGOING_SHARD_TRANSFERS_LIMIT": "0",
    }

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3, 20000, extra_env=env)

    create_collection(peer_api_uris[0], shard_number=1, replication_factor=3)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris
    )

    transfer_collection_cluster_info = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
    receiver_collection_cluster_info = get_collection_cluster_info(peer_api_uris[2], COLLECTION_NAME)

    from_peer_id = transfer_collection_cluster_info['peer_id']
    to_peer_id = receiver_collection_cluster_info['peer_id']

    # Start pushing points to the cluster
    upload_process_1 = run_update_points_in_background(peer_api_uris[0], COLLECTION_NAME, init_offset=100000, throttle=True)
    upload_process_2 = run_update_points_in_background(peer_api_uris[1], COLLECTION_NAME, init_offset=200000, throttle=True)
    upload_process_3 = run_update_points_in_background(peer_api_uris[2], COLLECTION_NAME, init_offset=300000, throttle=True)

    sleep(1)

    # Kill last peer
    upload_process_3.kill()
    processes.pop().kill()

    sleep(5)

    # Restart the peer
    peer_api_uris[-1] = start_peer(peer_dirs[-1], "peer_2_restarted.log", bootstrap_uri, extra_env=env)
    wait_for_peer_online(peer_api_uris[-1], "/")

    # Recover shard with WAL delta transfer
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
            "replicate_shard": {
                "shard_id": 0,
                "from_peer_id": from_peer_id,
                "to_peer_id": to_peer_id,
                "method": "wal_delta",
            }
        })
    assert_http_ok(r)

    # Wait for at least one record to be transferred
    wait_for_collection_shard_transfer_progress(peer_api_uris[0], COLLECTION_NAME, None, 1)

    # Then abort the transfer right away while it is still in progress
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
            "abort_transfer": {
                "shard_id": 0,
                "from_peer_id": from_peer_id,
                "to_peer_id": to_peer_id,
            }
        })
    assert_http_ok(r)

    sleep(1)

    # Confirm the shard is still dead
    receiver_collection_cluster_info = get_collection_cluster_info(peer_api_uris[2], COLLECTION_NAME)
    assert receiver_collection_cluster_info["local_shards"][0]["state"] == "Dead"

    # Retry recovery with WAL delta transfer
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
            "replicate_shard": {
                "shard_id": 0,
                "from_peer_id": from_peer_id,
                "to_peer_id": to_peer_id,
                "method": "wal_delta",
            }
        })
    assert_http_ok(r)

    # Assert WAL delta transfer progress, and wait for it to finish
    wait_for_collection_shard_transfer_progress(peer_api_uris[0], COLLECTION_NAME, None, 80)
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    # All nodes must have one shard
    for uri in peer_api_uris:
        cluster_info = get_collection_cluster_info(uri, COLLECTION_NAME)
        assert len(cluster_info['local_shards']) == 1

    upload_process_1.kill()
    upload_process_2.kill()
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


# Test the shard transfer fallback for WAL delta transfer.
#
# We replicate a shard with WAL delta transfer to a node that does not have any
# data for this shard yet. This is not supported in WAL delta transfer, so it
# should fall back to a different method.
#
# Test that data on the both sides is consistent
def test_shard_wal_delta_transfer_fallback(tmp_path: pathlib.Path):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 3, 20000)

    create_collection(peer_api_uris[0], shard_number=3, replication_factor=1)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris
    )

    # Insert some initial number of points
    upsert_random_points(peer_api_uris[0], 100)

    transfer_collection_cluster_info = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
    receiver_collection_cluster_info = get_collection_cluster_info(peer_api_uris[2], COLLECTION_NAME)

    from_peer_id = transfer_collection_cluster_info['peer_id']
    to_peer_id = receiver_collection_cluster_info['peer_id']

    shard_id = transfer_collection_cluster_info['local_shards'][0]['shard_id']

    # Transfer shard from one node to another

    # Move shard `shard_id` to peer `target_peer_id`
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": from_peer_id,
                "to_peer_id": to_peer_id,
                "method": "wal_delta",
            }
        })
    assert_http_ok(r)

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    # Assume that the WAL delta could not be resolved, preventing a diff
    # transfer. 'Failed to do shard diff transfer, falling back to default
    # method' is reported in the logs. But we cannot assert that at this point.
    # It falls back to streaming records.
    # TODO(1.9): assert 'streaming_records' transfer method in cluster state

    receiver_collection_cluster_info = get_collection_cluster_info(peer_api_uris[2], COLLECTION_NAME)
    number_local_shards = len(receiver_collection_cluster_info['local_shards'])
    assert number_local_shards == 2

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


# If the difference between the two nodes is too big, the WAL delta transfer
# cannot be used. In this case, we should fall back to a different method - the stream transfer.
def test_shard_fallback_on_big_diff(tmp_path: pathlib.Path):
    assert_project_root()

    # Prevent automatic recovery on restarted node, so we can manually recover with a specific transfer method
    env={
        "QDRANT__STORAGE__PERFORMANCE__INCOMING_SHARD_TRANSFERS_LIMIT": "0",
        "QDRANT__STORAGE__PERFORMANCE__OUTGOING_SHARD_TRANSFERS_LIMIT": "0",
        "QDRANT__STORAGE__WAL__WAL_CAPACITY_MB": "1",
    }

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3, 20000, extra_env=env)

    create_collection(peer_api_uris[0], shard_number=1, replication_factor=3)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris
    )

    transfer_collection_cluster_info = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
    receiver_collection_cluster_info = get_collection_cluster_info(peer_api_uris[2], COLLECTION_NAME)

    from_peer_id = transfer_collection_cluster_info['peer_id']
    to_peer_id = receiver_collection_cluster_info['peer_id']

    shard_id = 0

    upsert_random_points(peer_api_uris[0], 100)

    sleep(1)

    # Kill last peer
    processes.pop().kill()

    sleep(1)

    for i in range(10):
        upsert_random_points(peer_api_uris[0], 1000, offset=100000 + i * 1000)

    sleep(1)

    # Restart the peer
    peer_api_uris[-1] = start_peer(peer_dirs[-1], "peer_2_restarted.log", bootstrap_uri, extra_env=env)
    wait_for_peer_online(peer_api_uris[-1], "/")


    # Move shard `shard_id` to peer `target_peer_id`
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": from_peer_id,
                "to_peer_id": to_peer_id,
                "method": "wal_delta",
            }
        })
    assert_http_ok(r)

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    # Match all points on all nodes exactly
    data = []
    for uri in peer_api_uris:
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/scroll", json={
                "limit": 999999999,
                "with_vectors": False,
                "with_payload": False,
            }
        )
        assert_http_ok(r)
        data.append(r.json()["result"])

    check_data_consistency(data)
