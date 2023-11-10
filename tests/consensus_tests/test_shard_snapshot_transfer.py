import multiprocessing
import pathlib
from time import sleep

from .fixtures import upsert_random_points, create_collection
from .utils import *

N_PEERS = 3
N_SHARDS = 3
N_REPLICA = 1
COLLECTION_NAME = "test_collection"


def update_points_in_loop(peer_url, collection_name, offset=0):
    limit = 3
    while True:
        upsert_random_points(peer_url, limit, collection_name, offset=offset)
        offset += limit

        # Prevent inserting points faster than the queue proxy can transfer
        sleep(0.01)


def run_update_points_in_background(peer_url, collection_name, init_offset=0):
    p = multiprocessing.Process(target=update_points_in_loop, args=(peer_url, collection_name, init_offset))
    p.start()
    return p


# Transfer shards from one node to another while applying updates in parallel
# Test that data on the both sides is consistent
def test_shard_snapshot_transfer(tmp_path: pathlib.Path):
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

    # Start pushing points to the cluster
    upload_process_1 = run_update_points_in_background(peer_api_uris[0], COLLECTION_NAME, init_offset=100)
    upload_process_2 = run_update_points_in_background(peer_api_uris[1], COLLECTION_NAME, init_offset=10000)
    upload_process_3 = run_update_points_in_background(peer_api_uris[2], COLLECTION_NAME, init_offset=20000)

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
                "method": "snapshot"
            }
        })
    assert_http_ok(r)

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    upload_process_1.kill()
    upload_process_2.kill()
    upload_process_3.kill()

    receiver_collection_cluster_info = get_collection_cluster_info(peer_api_uris[2], COLLECTION_NAME)

    number_local_shards = len(receiver_collection_cluster_info['local_shards'])

    assert number_local_shards == 2

    # Retrieve points count from both shards

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
