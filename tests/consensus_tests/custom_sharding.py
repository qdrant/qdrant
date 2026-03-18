from .utils import *
from .fixtures import *

DEFAULT_COLLECTION_NAME = "test_collection"


def create_collection_with_custom_sharding(
        peer_url,
        collection=DEFAULT_COLLECTION_NAME,
        shard_number=1,
        replication_factor=1,
        write_consistency_factor=1,
        timeout=10
):
    create_collection(
        peer_url,
        collection=collection,
        shard_number=shard_number,
        replication_factor=replication_factor,
        write_consistency_factor=write_consistency_factor,
        timeout=timeout,
        sharding_method="custom",
    )


def create_shard(
        peer_url,
        collection,
        shard_key,
        shard_number=1,
        replication_factor=1,
        placement=None,
        initial_state=None,
        timeout=10
):
    r_batch = requests.put(
        f"{peer_url}/collections/{collection}/shards?timeout={timeout}", json={
            "shard_key": shard_key,
            "shards_number": shard_number,
            "replication_factor": replication_factor,
            "placement": placement,
            "initial_state": initial_state,
        })
    assert_http_ok(r_batch)


def delete_shard(
        peer_url,
        collection,
        shard_key,
        timeout=10
):
    r_batch = requests.post(
        f"{peer_url}/collections/{collection}/shards/delete?timeout={timeout}",
        json={
            "shard_key": shard_key,
        }
    )
    assert_http_ok(r_batch)
