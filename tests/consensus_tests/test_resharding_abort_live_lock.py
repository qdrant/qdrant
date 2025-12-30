import pathlib

from .fixtures import *
from .test_resharding import all_replicas, bootstrap_resharding, migrate_points
from .utils import *


def test_resharding_abourt_live_lock(tmp_path: pathlib.Path):
    # Bootstrap resharding cluster
    peer_uris, peer_ids = bootstrap_resharding(tmp_path, direction="down")

    # Trigger resharding transfers to last peer, to put replicas into `ReshardingScaleDown` state
    info = get_collection_cluster_info(peer_uris[-1], "test_collection")

    target_shard = max(all_replicas(info), key=lambda shard: shard["shard_id"])

    for local_shard in info["local_shards"]:
        if local_shard["shard_id"] == target_shard["shard_id"]:
            continue

        migrate_points(
            peer_uris[0],
            info["peer_id"],
            local_shard["shard_id"],
            target_shard["peer_id"],
            target_shard["shard_id"],
            "down",
        )

    # Kill last peer
    processes.pop().kill()

    # Upsert some points, to trigger resharding abort live-lock
    upsert_random_points(peer_uris[0], 20, timeout=20)
