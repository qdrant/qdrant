import pathlib
import shutil

import requests
from .fixtures import create_collection, upsert_random_points, random_vector, search
from .utils import *
from .assertions import assert_http_ok

COLLECTION_NAME = "test_collection"

def test_recover_from_snapshot_1(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0])
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)
    upsert_random_points(peer_api_uris[0], 100)

    # Kill last peer
    processes.pop().kill()

    # Delete collection files from disk
    collection_path = Path(peer_dirs[-1]) / "storage" / COLLECTION_NAME
    assert collection_path.exists()
    shutil.rmtree(collection_path)

    # Restart the peer
    peer_url = start_peer(peer_dirs[-1], f"peer_0_restarted.log", bootstrap_uri)

    # Recover Raft state
    recover_raft_state(peer_url)

    assert False

def recover_raft_state(peer_api_uri):
    r = requests.post(f"{peer_api_uri}/cluster/recover")
    assert_http_ok(r)
