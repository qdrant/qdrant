import multiprocessing
import pathlib
import random
from time import sleep

from .fixtures import upsert_random_points, create_collection
from .utils import *

N_PEERS = 3
N_SHARDS = 3
N_REPLICA = 1
COLLECTION_NAME = "test_collection"


def update_points_in_loop(peer_url, collection_name, offset=0, throttle=False, duration=None):
    start = time.time()
    limit = 3

    while True:
        upsert_random_points(peer_url, limit, collection_name, offset=offset)
        offset += limit

        if throttle:
            sleep(0.1)
        if duration is not None and (time.time() - start) > duration:
            break


def run_update_points_in_background(peer_url, collection_name, num_points=None, num_cities=None, shard_key=None, init_offset=0, throttle=False, duration=None):
    p = multiprocessing.Process(target=update_points_in_loop, args=(peer_url, collection_name, num_points, num_cities, shard_key, init_offset, throttle, duration))
    p.start()
    return p


def check_data_consistency(data):

    assert(len(data) > 1)

    for i in range(len(data) - 1):
        j = i + 1

        data_i = data[i]
        data_j = data[j]

        if data_i != data_j:
            ids_i = set(x["id"] for x in data_i["points"])
            ids_j = set(x["id"] for x in data_j["points"])

            diff = ids_i - ids_j

            if len(diff) < 100:
                print(f"Diff between {i} and {j}: {diff}")
            else:
                print(f"Diff len between {i} and {j}: {len(diff)}")

            assert False, "Data on all nodes should be consistent"

# Transfer shards from one node to another
#
# Simply does the most basic transfer: no concurrent updates during the
# transfer.
#
# Test that data on the both sides is consistent
def test_shard_stream_transfer(tmp_path: pathlib.Path):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, 20000)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA)
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
                "method": "stream_records",
            }
        })
    assert_http_ok(r)

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    receiver_collection_cluster_info = get_collection_cluster_info(peer_api_uris[2], COLLECTION_NAME)
    number_local_shards = len(receiver_collection_cluster_info['local_shards'])
    assert number_local_shards == 2

    # Point counts must be consistent across nodes
    counts = []
    for uri in peer_api_uris:
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/count", json={
                "exact": True
            }
        )
        assert_http_ok(r)
        counts.append(r.json()["result"]['count'])
    assert counts[0] == counts[1] == counts[2]


# Transfer shards from one node to another while applying throttled updates in parallel
#
# Updates are throttled to prevent sending updates faster than the queue proxy
# can handle. The transfer must therefore finish in 30 seconds without issues.
#
# Test that data on the both sides is consistent
def test_shard_stream_transfer_throttled_updates(tmp_path: pathlib.Path):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, 20000)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris
    )

    # Insert some initial number of points
    upsert_random_points(peer_api_uris[0], 10000)

    # Start pushing points to the cluster
    upload_process_1 = run_update_points_in_background(peer_api_uris[0], COLLECTION_NAME, init_offset=100, throttle=True)
    upload_process_2 = run_update_points_in_background(peer_api_uris[1], COLLECTION_NAME, init_offset=10000, throttle=True)
    upload_process_3 = run_update_points_in_background(peer_api_uris[2], COLLECTION_NAME, init_offset=20000, throttle=True)

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
                "method": "stream_records",
            }
        })
    assert_http_ok(r)

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    upload_process_1.kill()
    upload_process_2.kill()
    upload_process_3.kill()
    sleep(1)

    receiver_collection_cluster_info = get_collection_cluster_info(peer_api_uris[2], COLLECTION_NAME)
    number_local_shards = len(receiver_collection_cluster_info['local_shards'])
    assert number_local_shards == 2

    # Point counts must be consistent across nodes
    counts = []
    for uri in peer_api_uris:
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/count", json={
                "exact": True
            }
        )
        assert_http_ok(r)
        counts.append(r.json()["result"]['count'])
    assert counts[0] == counts[1] == counts[2]


# Transfer shards from one node to another while applying updates in parallel
#
# A fast burst of updates is sent in the first 5 seconds, the queue proxy will
# not be able to keep up with this. After that, updates are throttled. The
# transfer must still finish in 30 seconds without issues.
#
# Test that data on the both sides is consistent
def test_shard_stream_transfer_fast_burst(tmp_path: pathlib.Path):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, 20000)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris
    )

    # Insert some initial number of points
    upsert_random_points(peer_api_uris[0], 10000)

    # Start pushing points to the cluster
    upload_process_1 = run_update_points_in_background(peer_api_uris[0], COLLECTION_NAME, init_offset=100, duration=5)
    upload_process_2 = run_update_points_in_background(peer_api_uris[1], COLLECTION_NAME, init_offset=10000, duration=5)
    upload_process_3 = run_update_points_in_background(peer_api_uris[2], COLLECTION_NAME, init_offset=20000, throttle=True)

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
                "method": "stream_records",
            }
        })
    assert_http_ok(r)

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    upload_process_1.kill()
    upload_process_2.kill()
    upload_process_3.kill()
    sleep(1)

    receiver_collection_cluster_info = get_collection_cluster_info(peer_api_uris[2], COLLECTION_NAME)
    number_local_shards = len(receiver_collection_cluster_info['local_shards'])
    assert number_local_shards == 2

    # Point counts must be consistent across nodes
    counts = []
    for uri in peer_api_uris:
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/count", json={
                "exact": True
            }
        )
        assert_http_ok(r)
        counts.append(r.json()["result"]['count'])

    if counts[0] != counts[1] or counts[1] != counts[2]:
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


# Move one replica between nodes a few times, update pending point and ensure
# completion
#
# During the shard move we keep sending an operation that changes the very last
# point in the collection. More specifically, we invoke the set payload
# operation on the very last point.
#
# While the transfer is ongoing, the target replica might not have received the
# last point yet. The set payload operation must not mark the target replica as
# dead because it cannot find the point, as point may be transferred later as
# part of the transfer.
#
# If the test runs successfully, we see that the replica is properly moved. If the
# test does not run successfully, the point not found errors cascade into
# cancelling the shard transfers, and the replica will stay on the source node.
#
# Tests bug: <https://github.com/qdrant/qdrant/pull/5991>
#
# Updates are throttled to prevent overloading the cluster during the test.
def test_transfer_change_pending_point(tmp_path: pathlib.Path):
    assert_project_root()

    N_TRANSFERS = 5
    N_POINTS = 5000

    def set_payload_in_loop(peer_url, point_id):
        while True:
            r = requests.post(
                f"{peer_url}/collections/{COLLECTION_NAME}/points/payload?wait=true",
                json={
                    "points": [point_id],
                    "payload": {"n": random.random()},
                },
            )
            assert_http_ok(r)
            sleep(0.1)

    def run_set_payload_in_background(peer_url, point_id):
        p = multiprocessing.Process(target=set_payload_in_loop, args=(peer_url, point_id))
        p.start()
        return p

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, 20000)

    create_collection(peer_api_uris[0], shard_number=1, replication_factor=1)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris
    )

    # Insert some initial number of points
    upsert_random_points(peer_api_uris[0], N_POINTS)

    # Start pushing points to the cluster
    update_process_1 = run_update_points_in_background(peer_api_uris[0], COLLECTION_NAME, init_offset=100, throttle=True)
    update_process_2 = run_set_payload_in_background(peer_api_uris[1], point_id=N_POINTS - 1)
    update_process_3 = run_set_payload_in_background(peer_api_uris[2], point_id=N_POINTS - 1)

    # Move the shard a few times as it may not always trigger
    for _ in range(0, N_TRANSFERS):
        # Find what peer shard is currently on, select next peer as target
        from_index = None
        for index, uri in enumerate(peer_api_uris):
            collection_cluster_info = get_collection_cluster_info(uri, COLLECTION_NAME)
            if len(collection_cluster_info['local_shards']) == 1:
                from_index = index
                break
        assert from_index is not None, "No shard found"
        to_index = (from_index + 1) % len(peer_api_uris)

        # Grab peer IDs
        from_peer_uri = peer_api_uris[from_index]
        to_peer_uri = peer_api_uris[to_index]
        from_info = get_collection_cluster_info(from_peer_uri, COLLECTION_NAME)
        to_info = get_collection_cluster_info(to_peer_uri, COLLECTION_NAME)
        from_peer_id = from_info['peer_id']
        to_peer_id = to_info['peer_id']

        # Move shard from source to target peer
        r = requests.post(
            f"{from_peer_uri}/collections/{COLLECTION_NAME}/cluster", json={
                "move_shard": {
                    "shard_id": 0,
                    "from_peer_id": from_peer_id,
                    "to_peer_id": to_peer_id,
                    "method": "stream_records",
                }
            })
        assert_http_ok(r)

        # Wait both peers to have recognized end of transfer
        wait_for_collection_shard_transfers_count(from_peer_uri, COLLECTION_NAME, 0)
        wait_for_collection_shard_transfers_count(to_peer_uri, COLLECTION_NAME, 0)

        # Assert shard is actually moved
        # If the transfer succeeded, the replica is moved to the other node
        # If the transfer failed due to a missing point error, the replica is not moved
        from_info = get_collection_cluster_info(from_peer_uri, COLLECTION_NAME)
        to_info = get_collection_cluster_info(to_peer_uri, COLLECTION_NAME)
        assert len(from_info['local_shards']) == 0, "Shard must be moved off of source peer"
        assert len(to_info['local_shards']) == 1, "Shard must be moved onto target peer"

    # Cleanup
    update_process_1.kill()
    update_process_2.kill()
    update_process_3.kill()
