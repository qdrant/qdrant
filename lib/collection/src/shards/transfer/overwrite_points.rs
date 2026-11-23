use parking_lot::Mutex;
use segment::types::PointIdType;

use super::transfer_tasks_pool::TransferTaskProgress;
use crate::operations::types::CollectionResult;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ShardReplicaSet;

/// Number of points to overwrite in a single batch.
const OVERWRITE_BATCH_SIZE: usize = 100;

/// Overwrite the given points on the remote shard with our versions of them
///
/// Upserts the points we have, and deletes the ones we don't. The local shard must be a forward
/// proxy to the remote shard. Each batch holds a shared guard on the proxy update lock from before
/// reading until the remote accepted it, so the remote receives any later update to these points
/// after the batch.
///
/// The batches are sent without clock tags, and they must stay untagged. The remote keeps the
/// orphan operations that changed these points in its WAL, before these batches. A later WAL delta
/// transfer from the remote that includes them fails on the untagged records, instead of sending
/// the orphan operations to another replica.
///
/// # Cancel safety
///
/// This function is cancel safe.
///
/// If cancelled - only some of the points may be overwritten on the remote shard.
pub(super) async fn overwrite_points(
    replica_set: &ShardReplicaSet,
    remote_shard: &RemoteShard,
    point_ids: &[PointIdType],
    progress: &Mutex<TransferTaskProgress>,
) -> CollectionResult<()> {
    progress.lock().set(0, point_ids.len());

    let batch_count = point_ids.len().div_ceil(OVERWRITE_BATCH_SIZE);

    for (index, batch_point_ids) in point_ids.chunks(OVERWRITE_BATCH_SIZE).enumerate() {
        // The remote applies batches in order, so waiting on the last one is enough
        let wait = index + 1 == batch_count;

        let batch = replica_set
            .read_points_transfer_batch(batch_point_ids, wait)
            .await?;

        let result = batch.send(remote_shard).await?;

        progress.lock().add(result.count);
    }

    Ok(())
}
