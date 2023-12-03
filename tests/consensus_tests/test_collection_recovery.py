import pathlib
import shutil

from .fixtures import create_collection, upsert_random_points, random_dense_vector, search
from .utils import *
from .assertions import assert_http_ok

N_PEERS = 3
COLLECTION_NAME = "test_collection"


def test_collection_recovery(tmp_path: pathlib.Path):
    assert_project_root()

    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_urls[0], shard_number=2, replication_factor=2)
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

    # Wait for all shards to be active
    wait_for(all_collection_shards_are_active, peer_url, COLLECTION_NAME)

    # Check, that the collection is not empty on recovered node
    info = get_collection_cluster_info(peer_url, COLLECTION_NAME)

    for shard in info["local_shards"]:
        assert shard["points_count"] > 0


def recover_raft_state(peer_url):
    r = requests.post(f"{peer_url}/cluster/recover")
    return request_result(r)


def request_result(resp):
    assert_http_ok(resp)
    return resp.json()["result"]


def collection_exists(peer_url, collection_name):
    try:
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
    local_shards = info["local_shards"]

    if len(remote_shards) == 0:
        return False

    return all(map(lambda shard: shard["state"] == "Active", local_shards + remote_shards))
