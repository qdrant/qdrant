import concurrent.futures
import pathlib
import threading
from time import sleep

from .custom_sharding import create_collection_with_custom_sharding, create_shard, delete_shard
from .test_resharding import start_resharding, abort_resharding
from .fixtures import *
from .utils import *

N_PEERS = 3
N_SHARDS = 1
N_REPLICAS = 1

COLLECTION_NAME = "test_collection"


def test_fix_reshard_down_without_shard_key(tmp_path: pathlib.Path):
    """
    Test that we correctly set up the internal hash rings for custom sharding.

    Specifically, when auto sharding is used we need exactly one hash ring with
    a `None` key. When custom sharding is used, we use one hash ring for each
    shard key and do not have a `None` ring.

    Without it properly being implemented, people were able to break their
    cluster. On a cluster with custom sharding, triggering resharding down
    without a shard key would cause an infinite crash loop. Correctly setting up
    the internal hash rings prevents this due to a precondition check. Applying
    this fix (Qdrant 1.16.0+) on clusters that are already broken resolves the
    problem.

    See: <https://github.com/qdrant/qdrant/pull/7517>
    """

    assert_project_root()

    # Allow resharding
    env = {
        "QDRANT__CLUSTER__RESHARDING_ENABLED": "true",
    }

    # Bootstrap cluster
    peer_urls, _, _ = start_cluster(tmp_path, N_PEERS, extra_env=env)

    # Wait until all peers submit their metadata to consensus 🙄
    wait_for_peer_metadata(peer_urls[0])

    create_collection_with_custom_sharding(peer_urls[0], shard_number = N_SHARDS, replication_factor = N_PEERS)
    wait_collection_exists_and_active_on_all_peers(collection_name = COLLECTION_NAME, peer_api_uris = peer_urls)

    # Create a shard key
    create_shard(
        peer_urls[0],
        COLLECTION_NAME,
        shard_key = f"test",
        shard_number = 2,
        replication_factor = 2,
    )

    # Start resharding down without shard key (crashed before Qdrant 1.16.0)
    response = start_resharding(
        peer_urls[0],
        COLLECTION_NAME,
        "down",
        peer_id = None,
        shard_key = None,
    )
    assert not response.ok, response.text
    assert response.json()["status"]["error"] == ("Bad request: must specify shard key on collection with custom sharding")

    # Ensure consensus is still working
    for peer_url in peer_urls:
        info = get_cluster_info(peer_url)
        assert info["consensus_thread_status"]["consensus_thread_status"] == "working"

    # We expect resharding operation to have been ignored
    # We still have two shards and two replicas each
    info = get_collection_cluster_info(peer_urls[0], COLLECTION_NAME)
    assert "resharding_operations" not in info
    assert len(info["local_shards"]) + len(info["remote_shards"]) == 2 * 2


def wait_for_peer_metadata(peer_url: str):
    try:
        wait_for(check_peer_metadata, peer_url)
    except Exception as e:
        import json
        print(json.dumps(get_telemetry(peer_url), indent = 2))
        raise e


def check_peer_metadata(peer_url: str):
    telemetry = get_telemetry(peer_url)

    cluster = telemetry.get("cluster")

    metadata = cluster and cluster.get("peer_metadata")
    peers = cluster and cluster.get("peers")

    return metadata and peers and all(metadata.get(peer) for peer in peers)


def get_telemetry(peer_url: str):
    resp = requests.get(f"{peer_url}/telemetry?details_level=3")
    assert_http_ok(resp)

    return resp.json()["result"]
