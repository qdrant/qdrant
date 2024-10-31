use std::sync::Arc;

use parking_lot::Mutex;
use tokio::task::block_in_place;

use super::driver::{PersistedState, Stage};
use super::tasks_pool::ReshardTaskProgress;
use super::ReshardKey;
use crate::operations::point_ops::{PointOperations, WriteOrdering};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::CollectionUpdateOperations;
use crate::shards::shard_holder::LockedShardHolder;
use crate::shards::transfer::ShardTransferConsensus;

/// Batch size for deleting migrated points in existing shards.
const DELETE_BATCH_SIZE: usize = 500;

/// Stage 5: propagate deletes
///
/// Check whether migrated points still need to be deleted in their old shards.
pub(super) fn is_completed(state: &PersistedState) -> bool {
    let state_read = state.read();
    state_read.all_peers_completed(Stage::S6_PropagateDeletes)
        && state_read.shards_to_delete().next().is_none()
}

/// Stage 5: commit new hashring
///
/// Do delete migrated points from their old shards.
// TODO(resharding): this is a naive implementation, delete by hashring filter directly!
pub(super) async fn drive(
    reshard_key: &ReshardKey,
    state: &PersistedState,
    progress: &Mutex<ReshardTaskProgress>,
    shard_holder: Arc<LockedShardHolder>,
    consensus: &dyn ShardTransferConsensus,
) -> CollectionResult<()> {
    let hashring = {
        let shard_holder = shard_holder.read().await;
        let shard_key = shard_holder
            .get_shard_id_to_key_mapping()
            .get(&reshard_key.shard_id)
            .cloned();
        shard_holder.rings.get(&shard_key).cloned().ok_or_else(|| {
            CollectionError::service_error(format!(
                "Cannot delete migrated points while resharding shard {}, failed to get shard hash ring",
                reshard_key.shard_id,
            ))
        })?
    };

    while let Some(source_shard_id) = block_in_place(|| state.read().shards_to_delete().next()) {
        let mut offset = None;

        loop {
            let shard_holder = shard_holder.read().await;

            let replica_set = shard_holder.get_shard(source_shard_id).ok_or_else(|| {
                CollectionError::service_error(format!(
                    "Shard {source_shard_id} not found in the shard holder for resharding",
                ))
            })?;

            // Take batch of points, if full, pop the last entry as next batch offset
            let mut points = replica_set
                .scroll_by(
                    offset,
                    DELETE_BATCH_SIZE + 1,
                    &false.into(),
                    &false.into(),
                    // TODO(resharding): directly apply hash ring filter here
                    None,
                    None,
                    false,
                    None,
                    None, // no timeout
                )
                .await?;

            offset = if points.len() > DELETE_BATCH_SIZE {
                points.pop().map(|point| point.id)
            } else {
                None
            };

            let ids = points
                .into_iter()
                .map(|point| point.id)
                .filter(|point_id| !hashring.is_in_shard(&point_id, source_shard_id))
                .collect();

            let operation =
                CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints { ids });

            // Wait on all updates here, not just the last batch
            // If we don't wait on all updates it somehow results in inconsistent deletes
            replica_set
                .update_with_consistency(operation, true, WriteOrdering::Weak, false)
                .await?;

            if offset.is_none() {
                break;
            }
        }

        state.write(|data| {
            data.deleted_shards.push(source_shard_id);
            data.update(progress, consensus);
        })?;
    }

    state.write(|data| {
        data.complete_for_all_peers(Stage::S6_PropagateDeletes);
        data.update(progress, consensus);
    })?;

    Ok(())
}
