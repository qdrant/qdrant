import pathlib
from time import sleep

import requests

from .assertions import assert_http_ok
from .fixtures import upsert_random_points, create_collection
from .utils import *

COLLECTION_NAME = "test_collection"
N_PEERS = 3
N_SHARDS = 3
REPLICATION_FACTOR = 1
NUM_POINTS = 1000
BATCH_SIZE = 100


def test_shard_transfer_and_resharding_bug(tmp_path: pathlib.Path):
    """
    Test that a shard move transfer and resharding can be triggered back-to-back
    on a 3-node cluster with 1M points, and that all peers end up with zero
    pending consensus operations.
    """
    assert_project_root()

    # Prevent optimizers from interfering during the test
    env = {
        "QDRANT__STORAGE__OPTIMIZERS__INDEXING_THRESHOLD_KB": "0",
        # Artificially make stream records transfer very slow
        "QDRANT_STAGING_SHARD_TRANSFER_DELAY_SEC": "10",
    }

    # Start a 3-node cluster
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, extra_env=env)

    # Staging feature must be enabled
    skip_if_no_feature(peer_api_uris[0], "staging")

    # Collect peer IDs
    peer_ids = [get_cluster_info(uri)["peer_id"] for uri in peer_api_uris]

    # Create collection with 3 shards and replication factor 2
    create_collection(
        peer_api_uris[0],
        COLLECTION_NAME,
        shard_number=N_SHARDS,
        replication_factor=REPLICATION_FACTOR,
    )
    wait_collection_exists_and_active_on_all_peers(COLLECTION_NAME, peer_api_uris)
    upsert_random_points(
        peer_api_uris[0],
        NUM_POINTS,
        collection_name=COLLECTION_NAME,
        batch_size=BATCH_SIZE,
        wait="true",
        with_sparse_vector=False,
    )

    # Get cluster info for the collection to find shard/peer details
    collection_cluster_info = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
    source_peer_id = collection_cluster_info["peer_id"]

    # Pick a local shard to move
    local_shard = collection_cluster_info["local_shards"][0]
    shard_id = local_shard["shard_id"]

    # Find a peer that does NOT already have this shard, to move it there
    peers_with_shard = {source_peer_id}
    for remote in collection_cluster_info["remote_shards"]:
        if remote["shard_id"] == shard_id:
            peers_with_shard.add(remote["peer_id"])

    target_peer_id = None
    for pid in peer_ids:
        if pid not in peers_with_shard:
            target_peer_id = pid
            break

    # # If all peers already have this shard (replication_factor covers all), pick a
    # # remote peer and move from source to that peer (move replaces the source replica)
    # if target_peer_id is None:
    #     for remote in collection_cluster_info["remote_shards"]:
    #         if remote["shard_id"] == shard_id:
    #             target_peer_id = remote["peer_id"]
    #             break

    assert target_peer_id is not None, "Could not find a target peer for shard move"

    # Trigger shard move transfer (non-blocking)
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster",
        params={
            "timeout": "3", # Operation is supposed to be accepted quickly
        },
        json={
            "move_shard": {
                "shard_id": shard_id,
                "from_peer_id": source_peer_id,
                "to_peer_id": target_peer_id,
                "method": "stream_records",
            }
        },
    )
    assert_http_ok(r)

    # Immediately trigger resharding (start_resharding action)
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster",
        params={
            "timeout": "3", # Operation is supposed to be accepted quickly
        },
        json={
            "start_resharding": {
                "direction": "up",
            }
        },
    )
    assert_http_ok(r)

    sleep(3)

    # TODO: # Wait for resharding state to appear in collection cluster info on any peer
    # TODO: resharding_visible = False
    # TODO: for uri in peer_api_uris:
    # TODO:     try:
    # TODO:         wait_for_collection_resharding_operations_count(uri, COLLECTION_NAME, 1)
    # TODO:         resharding_visible = True
    # TODO:         break
    # TODO:     except Exception:
    # TODO:         continue

    # TODO: assert resharding_visible, "Resharding operation never appeared on any peer"

    # Check that all peers have 0 pending consensus operations
    for i, uri in enumerate(peer_api_uris):
        cluster_info = get_cluster_info(uri)
        pending = cluster_info["raft_info"]["pending_operations"]
        assert pending == 0, (
            f"Peer {peer_ids[i]} at {uri} has {pending} pending operations, expected 0"
        )
