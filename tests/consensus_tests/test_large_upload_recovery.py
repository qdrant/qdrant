import multiprocessing
import pathlib
import uuid
from time import sleep

from .fixtures import upsert_random_points, create_collection
from .utils import *

N_PEERS = 1
N_SHARDS = 1
N_REPLICA = 1
COLLECTION_NAME = "test_collection_update_recover"


def test_large_update_and_restart(tmp_path: pathlib.Path):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, 20000)

    print("peer_dirs: ", peer_dirs)

    create_collection(
        peer_api_uris[0],
        collection=COLLECTION_NAME,
        shard_number=N_SHARDS,
        replication_factor=N_REPLICA,
        default_segment_number=32
    )
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris
    )

    number_points = 10_000

    # Insert some initial number of points
    upsert_random_points(peer_api_uris[0], number_points, collection_name=COLLECTION_NAME)

    p = processes.pop()
    p.kill()

    peer_api_uris.pop()

    peer_api_uris.append(
        start_peer(
            peer_dirs[-1],
            f"peer_0_{N_PEERS}_restart_again.log",
            bootstrap_uri,
            port=20000,
        )
    )

    wait_peer_added(peer_api_uris[0])

    for uri in peer_api_uris:
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/count", json={
                "exact": True
            }
        )
        assert_http_ok(r)
        assert r.json()["result"]['count'] == number_points
