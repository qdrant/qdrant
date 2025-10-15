import pathlib

from .fixtures import upsert_random_points, create_collection
from .utils import *

from .test_custom_sharding import create_shard

N_PEERS = 1
N_SHARDS = 1
N_REPLICA = 1
COLLECTION_NAME = "test_collection"


# Transfer points from one shard to another
#
# Simply does the most basic transfer: no concurrent updates during the
# transfer.
#
# Test that data on the both sides is consistent
def test_replicate_points_stream_transfer(tmp_path: pathlib.Path):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, 20000)

    create_collection(
        peer_api_uris[0],
        sharding_method="custom",
        shard_number=N_SHARDS,
        replication_factor=N_REPLICA,
    )
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )

    create_shard(
        peer_api_uris[0], COLLECTION_NAME, shard_key="default", shard_number=N_SHARDS
    )
    create_shard(
        peer_api_uris[0], COLLECTION_NAME, shard_key="tenant", shard_number=N_SHARDS
    )

    # Insert some initial number of points
    upsert_random_points(peer_api_uris[0], 100, shard_key="default")

    # Transfer shard from one node to another
    filter = {}

    # Move points from from_shard_id to to_shard_id
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster",
        json={
            "replicate_points": {
                # "filter": filter,
                "from_shard_key": "default",
                "to_shard_key": "tenant",
            }
        },
    )
    assert_http_ok(r)

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    receiver_collection_cluster_info = get_collection_cluster_info(
        peer_api_uris[-1], COLLECTION_NAME
    )
    number_local_shards = len(receiver_collection_cluster_info["local_shards"])
    assert number_local_shards == 2

    # Point counts must be consistent across shard keys for given filter
    for shard_key in ["default", "tenant"]:
        r = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/count",
            json={"exact": True, "filter": filter, "shard_key": shard_key},
        )
        assert_http_ok(r)
        assert r.json()["result"]["count"] == 100
