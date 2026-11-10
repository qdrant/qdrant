//! Pipelined batch transfer of point records, shared by the stream records transfer methods.

use std::future;
use std::time::Duration;

use futures::{StreamExt as _, TryStreamExt as _, stream};
use parking_lot::Mutex;
use segment::types::PointIdType;
use tokio::sync::mpsc;

use super::transfer_tasks_pool::TransferTaskProgress;
use crate::hash_ring::HashRingRouter;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::forward_proxy_shard::{PreparedTransferBatch, TransferBatchResult};
use crate::shards::remote_shard::RemoteShard;
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::SharedShardHolder;

/// Number of points to transfer in a single batch.
const TRANSFER_BATCH_SIZE: usize = 100;

/// Number of batches to read ahead while earlier batches are being sent.
const TRANSFER_READ_AHEAD_BATCHES: usize = 2;

/// Number of batches to send to the remote shard in parallel.
const TRANSFER_PARALLEL_SENDS: usize = 4;

/// Transfer all point records of a proxified shard to the remote shard in batches.
///
/// Batches are read in order, and read ahead while earlier batches are still being sent. Up to
/// [`TRANSFER_PARALLEL_SENDS`] batches are sent to the remote shard in parallel. This keeps both
/// the local storage and the network busy, rather than waiting on one of them at a time.
///
/// Each batch holds a shared guard on the forward proxy update lock from before reading until the
/// remote accepted it. Updates to the shard wait for all those batches, so the remote never
/// receives an update before a batch that is older than it. Batches cover disjoint point ID
/// ranges, so the order in which the remote receives them does not matter.
///
/// The last batch makes the remote wait until it is fully applied. It is only sent after the
/// remote accepted all other batches, so waiting on it covers all of them.
///
/// # Cancel safety
///
/// This function is cancel safe.
pub(super) async fn transfer_batches(
    shard_holder: &SharedShardHolder,
    shard_id: ShardId,
    remote_shard: &RemoteShard,
    hashring: Option<&HashRingRouter>,
    merge_points: bool,
    progress: &Mutex<TransferTaskProgress>,
) -> CollectionResult<()> {
    #[cfg(feature = "staging")]
    let staging_delay = std::env::var("QDRANT_STAGING_SHARD_TRANSFER_DELAY_SEC")
        .ok()
        .map(|val| {
            Duration::from_secs_f64(
                val.parse::<f64>()
                    .expect("invalid QDRANT_STAGING_SHARD_TRANSFER_DELAY_SEC value"),
            )
        });

    let (batch_sender, mut batch_receiver) = mpsc::channel(TRANSFER_READ_AHEAD_BATCHES);

    // Read all batches in order, queue them for sending but return the last one
    //
    // Takes ownership of the channel sender, which closes the channel once all batches are read
    let reader = async move {
        let mut offset = None;

        loop {
            let batch = read_batch(shard_holder, shard_id, offset, hashring, merge_points).await?;

            #[cfg(feature = "staging")]
            if let Some(delay) = staging_delay {
                tokio::time::sleep(delay).await;
            }

            let Some(next_page_offset) = batch.next_page_offset else {
                return Ok(batch);
            };
            offset = Some(next_page_offset);

            batch_sender.send(batch).await.map_err(|_| {
                CollectionError::service_error(format!(
                    "Failed to queue batch for shard {shard_id} transfer, sender stopped"
                ))
            })?;
        }
    };

    let mut total_read = Duration::ZERO;
    let mut total_send = Duration::ZERO;
    let mut record_progress = |result: TransferBatchResult| {
        total_read += result.read_duration;
        total_send += result.send_duration;
        let mut progress = progress.lock();
        progress.add(result.count);
        progress.set_batch_durations(total_read, total_send);
    };

    // Send queued batches to the remote shard in parallel
    let sender = stream::poll_fn(|cx| batch_receiver.poll_recv(cx))
        .map(|batch| batch.send(remote_shard))
        .buffer_unordered(TRANSFER_PARALLEL_SENDS)
        .try_for_each(|result| {
            record_progress(result);
            future::ready(Ok(()))
        });

    let (last_batch, ()) = tokio::try_join!(reader, sender)?;

    // Send the last batch, the remote has accepted all other batches at this point
    let result = last_batch.send(remote_shard).await?;
    record_progress(result);

    Ok(())
}

/// Read the next transfer batch, starting at `offset`.
///
/// # Cancel safety
///
/// This function is cancel safe.
async fn read_batch(
    shard_holder: &SharedShardHolder,
    shard_id: ShardId,
    offset: Option<PointIdType>,
    hashring: Option<&HashRingRouter>,
    merge_points: bool,
) -> CollectionResult<PreparedTransferBatch> {
    let shard_holder = shard_holder.read().await;

    let Some(replica_set) = shard_holder.get_shard(shard_id) else {
        // Forward proxy gone?!
        // That would be a programming error.
        return Err(CollectionError::service_error(format!(
            "Shard {shard_id} is not found"
        )));
    };

    // The shard holder lock is dropped on return, it must not be held while sending. Sending can
    // take very long, especially the last batch, and would block all other operations on the
    // collection. The prepared batch keeps holding a shared guard on the forward proxy update
    // lock, which prevents concurrent updates until it is sent.
    replica_set
        .read_transfer_batch(offset, TRANSFER_BATCH_SIZE, hashring, merge_points)
        .await
}
