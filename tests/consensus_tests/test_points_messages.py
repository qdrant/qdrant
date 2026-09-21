import pytest

from .points_messages import UpdateBatchInternal, is_upsert_batch_for


@pytest.mark.parametrize("routes, expected", [
    ([("test_collection", 0)], True),
    ([("test_collection", 0), ("test_collection", 0)], True),
    ([("other_collection", 0)], False),
    ([("test_collection", 1)], False),
    ([("test_collection", None)], False),
    ([("test_collection", 0), ("test_collection", 1)], False),
    ([], False),
])
def test_upsert_batch_matches_collection_and_explicit_shard(routes, expected):
    batch = UpdateBatchInternal()
    for collection_name, shard_id in routes:
        upsert = batch.operations.add().upsert
        upsert.upsert_points.collection_name = collection_name
        if shard_id is not None:
            upsert.shard_id = shard_id

    assert is_upsert_batch_for(batch.SerializeToString(), "test_collection", 0) is expected


def test_upsert_batch_rejects_unrecognized_operation():
    # An operation containing only unknown fields must not match shard 0.
    assert not is_upsert_batch_for(b"\x0a\x02\x1a\x00", "test_collection", 0)
