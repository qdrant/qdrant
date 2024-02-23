import multiprocessing
import pathlib
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


def run_update_points_in_background(peer_url, collection_name, init_offset=0, throttle=False, duration=None):
    p = multiprocessing.Process(target=update_points_in_loop, args=(peer_url, collection_name, init_offset, throttle, duration))
    p.start()
    return p


# Test a WAL delta transfer between two that have no difference.
#
# No WAL records will be transferred as the WAL should be empty. We cannot
# assert that property in this test however. It is tested both ways.
#
# Test that data on the both sides is consistent
def test_empty_shard_wal_delta_transfer(tmp_path: pathlib.Path):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 2, 20000)

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

    cluster_info_0 = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
    cluster_info_1 = get_collection_cluster_info(peer_api_uris[1], COLLECTION_NAME)
    assert len(cluster_info_0['local_shards']) == 1
    assert len(cluster_info_1['local_shards']) == 1

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
    assert counts[0] == counts[1]
