import pathlib
import shutil

import requests
from .fixtures import create_collection, upsert_random_points, random_vector, search
from .utils import *
from .assertions import assert_http_ok

N_PEERS = 3
COLLECTION_NAME = "test_collection"


@pytest.mark.skip(reason="this test does not check anything useful, but simply documents current (i.e., broken) behavior")
def test_collection_recovery(tmp_path: pathlib.Path):
    assert_project_root()

    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_urls[0])
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_urls)
    upsert_random_points(peer_urls[0], 100)

    # Check that collection is not empty
    info = get_collection_cluster_info(peer_urls[-1], COLLECTION_NAME)

    for shard in info["local_shards"]:
        assert shard["points_count"] > 0

    # Kill last peer
    processes.pop().kill()

    # Delete collection files from disk
    collection_path = Path(peer_dirs[-1]) / "storage" / "collections" / COLLECTION_NAME
    assert collection_path.exists()
    shutil.rmtree(collection_path)

    # Restart the peer
    peer_url = start_peer(peer_dirs[-1], f"peer_0_restarted.log", bootstrap_url)

    # Wait until peer is up
    wait_for_peer_online(peer_url)

    # Recover Raft state
    recover_raft_state(peer_url)

    # Wait for the Raft state to be recovered
    wait_for(collection_exists, peer_url, COLLECTION_NAME)

    # Check, that remote shards are broken on recovered node 🥲
    info = get_collection_cluster_info(peer_url, COLLECTION_NAME)

    for remote_shard in info["remote_shards"]:
        assert remote_shard["state"] == "Initializing"

    # Recover Raft state once again
    recover_raft_state(peer_url)

    # Wait for the Raft state to be recovered
    wait_for(all_collection_shards_are_active, peer_url, COLLECTION_NAME)

    # Check, that the collection is empty on recovered node
    info = get_collection_cluster_info(peer_url, COLLECTION_NAME)

    for shard in info["local_shards"]:
        assert shard["points_count"] == 0


def get_collection_cluser_info(peer_url, collection_name):
    r = request.get(f"{peer_url}/collections/{collection_name}/cluster")
    return request_result(r)

def recover_raft_state(peer_url):
    r = requests.post(f"{peer_url}/cluster/recover")
    return request_result(r)

def request_result(resp):
    assert_http_ok(resp)
    return resp.json()["result"]

def collection_exists(peer_url, collection_name):
    try:
        # It is crusial to query the collection *cluster* info instead of collection info,
        # because collection info is permanently broken after the first recovery,
        # but collection cluster info works fine.
        get_collection_cluster_info(peer_url, collection_name)
    except:
        return False

    return True

def all_collection_shards_are_active(peer_url, collection_name):
    try:
        info = get_collection_cluster_info(peer_url, collection_name)
    except:
        return False

    remote_shards = info["remote_shards"]

    if len(remote_shards) == 0:
        return False

    return all(map(lambda shard: shard["state"] == "Active", remote_shards))
