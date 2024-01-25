import pathlib
import shutil

from .fixtures import create_collection, upsert_random_points, random_dense_vector, search
from .utils import *
from .assertions import assert_http_ok

N_PEERS = 3
COLLECTION_NAME = "test_collection"


# Check that a failed node automatically recovers with shard transfers.
# Make sure the number of transfers never goes above 1, which is the default
# incoming/outgoing limit.
def test_collection_recovery_below_limit(tmp_path: pathlib.Path):
    N_POINTS = 2500
    N_SHARDS = N_PEERS

    assert_project_root()

    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_urls[0], shard_number=N_SHARDS, replication_factor=N_SHARDS)
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_urls)
    upsert_random_points(peer_urls[0], 100)

    # Check that collection is not empty
    info = get_collection_cluster_info(peer_urls[-1], COLLECTION_NAME)

    for shard in info["local_shards"]:
        assert shard["points_count"] > 0

    # Kill last peer
    processes.pop().kill()

    # Insert some more points
    upsert_random_points(peer_urls[0], N_POINTS)

    # Restart the peer, wait until it is up
    peer_url = start_peer(peer_dirs[-1], f"peer_0_restarted.log", bootstrap_url)
    wait_for_peer_online(peer_url)

    # Wait until all shards are active, never allow more than 1 shard transfer
    wait_for(transfers_below_limit_or_done, peer_url, transfer_limit=1)

    # Check, that the collection is not empty on recovered node
    info = get_collection_cluster_info(peer_url, COLLECTION_NAME)

    # Assure we have all points
    count = sum([shard["points_count"] for shard in info["local_shards"]])
    assert count == N_POINTS


# Check that a failed node automatically recovers with shard transfers.
# Set a custom incoming/outgoing transfer limit of 3, and make sure we actually
# reach this limit with automatic transfers.
def test_collection_recovery_reach_limit(tmp_path: pathlib.Path):
    N_POINTS = 5000
    N_SHARDS = N_PEERS

    assert_project_root()

    # Use higher than default transfer limits
    env = {
        "QDRANT__STORAGE__PERFORMANCE__INCOMING_SHARD_TRANSFERS_LIMIT": "3",
        "QDRANT__STORAGE__PERFORMANCE__OUTGOING_SHARD_TRANSFERS_LIMIT": "2",
    }
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS, extra_env=env)

    create_collection(peer_urls[0], shard_number=N_SHARDS, replication_factor=N_SHARDS)
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_urls)
    upsert_random_points(peer_urls[0], 100)

    # Check that collection is not empty
    info = get_collection_cluster_info(peer_urls[-1], COLLECTION_NAME)

    for shard in info["local_shards"]:
        assert shard["points_count"] > 0

    # Kill last peer
    processes.pop().kill()

    # Insert some more points
    upsert_random_points(peer_urls[0], N_POINTS)

    # Restart the peer, wait until it is up
    peer_url = start_peer(peer_dirs[-1], f"peer_0_restarted.log", bootstrap_url, extra_env=env)
    wait_for_peer_online(peer_url)

    # We must see 3 transfers at one point with our customized limits
    wait_for(transfers_reached_threshold, peer_url, transfer_threshold=3, transfer_limit=3)

    # Wait until all shards are active, never allow more than 3 shard transfers
    wait_for(transfers_below_limit_or_done, peer_url, transfer_limit=3)

    # Check, that the collection is not empty on recovered node
    info = get_collection_cluster_info(peer_url, COLLECTION_NAME)

    # Assure we have all points
    count = sum([shard["points_count"] for shard in info["local_shards"]])
    assert count == N_POINTS


# Check that a failed node automatically recovers with shard transfers.
# Keep the default automatic shard transfer limit of 1, but make some user
# requests initiating additional shard transfers. Make sure that we surpass more
# than one ongoing shard transfer.
def test_collection_recovery_user_requests_above_limit(tmp_path: pathlib.Path):
    N_POINTS = 5000
    N_SHARDS = N_PEERS

    assert_project_root()

    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_urls[0], shard_number=N_SHARDS, replication_factor=N_SHARDS)
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_urls)
    upsert_random_points(peer_urls[0], 100)

    # Check that collection is not empty
    info = get_collection_cluster_info(peer_urls[-1], COLLECTION_NAME)

    for shard in info["local_shards"]:
        assert shard["points_count"] > 0

    # Kill last peer
    processes.pop().kill()
    killed_peer_dir = peer_dirs[-1]

    # Insert some more points
    upsert_random_points(peer_urls[0], N_POINTS)

    # Create an extra peer to allow moving some replicas
    peer_dirs.append(make_peer_folder(tmp_path, N_PEERS + 1))
    peer_urls.append(start_peer(peer_dirs[-1], f"peer_{N_PEERS + 1}.log", bootstrap_url))
    wait_for_peer_online(peer_urls[-1])

    # Restart the peer, wait unil it is up
    peer_url = start_peer(killed_peer_dir, f"peer_0_restarted.log", bootstrap_url)
    wait_for_peer_online(peer_url)

    # Replicate all shards to our new peer
    source_peer_id = get_cluster_info(peer_urls[0])["peer_id"]
    target_peer_id = get_cluster_info(peer_urls[-1])["peer_id"]
    for shard_id in range(N_SHARDS):
        r = requests.post(
            f"{peer_urls[0]}/collections/{COLLECTION_NAME}/cluster", json={
                "replicate_shard": {
                    "shard_id": shard_id,
                    "from_peer_id": source_peer_id,
                    "to_peer_id": target_peer_id,
                }
            })
        assert_http_ok(r)

    # We must see 4 transfers at one point with our customized limits
    wait_for(transfers_reached_threshold, peer_url, transfer_threshold=4, transfer_limit=4)

    # Wait until all shards are active, never allow more than 4 shard transfers
    wait_for(transfers_below_limit_or_done, peer_url, transfer_limit=4)

    # Wait until all shards are active on our new node we replicated to
    wait_for(all_collection_shards_are_active, peer_urls[-1], COLLECTION_NAME)

    # Check, that the collection is not empty on recovered node
    info = get_collection_cluster_info(peer_url, COLLECTION_NAME)

    # Assure we have all points
    count = sum([shard["points_count"] for shard in info["local_shards"]])
    assert count == N_POINTS


def transfers_reached_threshold(peer_url, transfer_threshold=1, transfer_limit = 1):
    # Ongoing transfers must be at threshold but not over the limit
    transfer_count = get_shard_transfer_count(peer_url, COLLECTION_NAME)
    if transfer_count > transfer_limit:
        raise Exception(f"Number of shard transfers in collection {COLLECTION_NAME} is above our limit ({transfer_count}/{transfer_limit})")

    return transfer_count >= transfer_threshold


def transfers_below_limit_or_done(peer_url, transfer_limit = 1):
    # Ongoing transfers must be below limit
    transfer_count = get_shard_transfer_count(peer_url, COLLECTION_NAME)
    if transfer_count > transfer_limit:
        raise Exception(f"Number of shard transfers in collection {COLLECTION_NAME} is above our limit ({transfer_count}/{transfer_limit})")

    # If all collection shards are active, we're good
    return transfer_count == 0 and all_collection_shards_are_active(peer_url, COLLECTION_NAME)


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
