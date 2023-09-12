import logging
import pathlib

from .fixtures import create_collection, upsert_random_points, count_counts
from .utils import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

N_PEERS = 3
N_SHARDS = 1
N_REPLICAS = 2
COLLECTION_NAME = "test_collection"


def test_listener_node(tmp_path: pathlib.Path):

    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path,
        N_PEERS,
        extra_env={
            "QDRANT__STORAGE__UPDATE_CONCURRENCY": "1",
        }
    )

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICAS)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris)

    upsert_random_points(peer_api_uris[0], 100)

    for uri in peer_api_uris:
        count = count_counts(uri, COLLECTION_NAME)
        assert count == 100
