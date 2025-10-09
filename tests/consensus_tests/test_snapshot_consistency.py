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


# Test data consistency across nodes when creating snapshots.
#
# We test this because we proxy all segments while creating a snapshot, after
# which we unproxy all of them again propagating changes. While this is
# happening we keep upserting new data. Because this is error prone we need to
# assert data consistency.
#
# Test that data on the both sides is consistent
def test_shard_wal_delta_transfer_manual_recovery(tmp_path: pathlib.Path):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3, 20000)

    create_collection(peer_api_uris[0], shard_number=1, replication_factor=3)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris
    )

    # Insert points!
    upsert_random_points(peer_api_uris[0], 10000, batch_size=100)

    # Start pushing points to the cluster
    upload_process_1 = run_update_points_in_background(peer_api_uris[0], COLLECTION_NAME, init_offset=0, throttle=True)
    upload_process_2 = run_update_points_in_background(peer_api_uris[1], COLLECTION_NAME, init_offset=100000, throttle=True)
    upload_process_3 = run_update_points_in_background(peer_api_uris[2], COLLECTION_NAME, init_offset=200000, throttle=True)

    sleep(1)

    # Make 5 snapshots
    for i in range(0, 5):
        r = requests.post(f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/snapshots?wait=true")
        assert_http_ok(r)

    upload_process_1.kill()
    upload_process_2.kill()
    upload_process_3.kill()
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
        data.append(r.json()["result"]["points"])
    check_data_consistency(data)
