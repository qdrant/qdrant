import multiprocessing
import pathlib
import random
from time import sleep

from .fixtures import upsert_random_points, create_collection, create_field_index
from .utils import *

COLLECTION_NAME = "test_collection"


# Test that the maximum segment size property is respected
#
# It sets a very low limit and inserts some points. Qdrant should dynamically
# create a few new appendable segments for all the data to fit.
#
# It also confirms payload indices are configured properly and all payload
# fields are indexed.
def test_max_segment_size(tmp_path: pathlib.Path):
    assert_project_root()

    # Set a low segment size limit and disable indexing
    env={
        "QDRANT__STORAGE__OPTIMIZERS__DEFAULT_SEGMENT_NUMBER": "2",
        "QDRANT__STORAGE__OPTIMIZERS__MAX_SEGMENT_SIZE_KB": "1",
        "QDRANT__STORAGE__OPTIMIZERS__INDEXING_THRESHOLD_KB": "0",
    }

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 1, 20000, extra_env=env)

    create_collection(peer_api_uris[0], shard_number=1, replication_factor=1)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris
    )
    create_field_index(peer_api_uris[0], COLLECTION_NAME, "city", "keyword")

    collection_cluster_info_before = get_collection_info(peer_api_uris[0], COLLECTION_NAME)

    assert collection_cluster_info_before["segments_count"] == 2
    assert collection_cluster_info_before["payload_schema"]["city"]["points"] == 0

    upsert_random_points(peer_api_uris[0], 20, batch_size=5)

    collection_cluster_info_after = get_collection_info(peer_api_uris[0], COLLECTION_NAME)

    # Number of segments must have grown due to limited segment size
    assert collection_cluster_info_before["segments_count"] < collection_cluster_info_after["segments_count"]

    # We must have indexed all points
    assert collection_cluster_info_after["payload_schema"]["city"]["points"] == 20
