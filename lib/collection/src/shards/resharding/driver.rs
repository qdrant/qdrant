use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;

use super::tasks_pool::ReshardTaskProgress;
use super::ReshardTask;
use crate::operations::types::CollectionResult;
use crate::shards::channel_service::ChannelService;
use crate::shards::shard_holder::LockedShardHolder;
use crate::shards::transfer::ShardTransferConsensus;
use crate::shards::CollectionId;

/// Drive the resharding on the target node based on the given configuration
///
/// Returns `true` if we should finalize resharding. Returns `false` if we should silently
/// drop it, because it is being restarted.
///
/// # Cancel safety
///
/// This function is cancel safe.
#[allow(clippy::too_many_arguments)]
pub async fn drive_resharding(
    _transfer_config: ReshardTask,
    _progress: Arc<Mutex<ReshardTaskProgress>>,
    _shard_holder: Arc<LockedShardHolder>,
    _consensus: &dyn ShardTransferConsensus,
    _collection_id: CollectionId,
    _collection_name: &str,
    _channel_service: ChannelService,
    _temp_dir: &Path,
) -> CollectionResult<bool> {
    // Stage 1: init
    if !check_init() {
        stage_init()?;
    }

    // Stage 2: init
    if !check_migrate_points() {
        stage_migrate_points()?;
    }

    // Stage 3: replicate to match replication factor
    if !check_replicate_replication_factor() {
        stage_replicate_replication_factor()?;
    }

    // ...

    todo!("implement driver steps here");

    Ok(true)
}

/// Stage 1: init
///
/// Check whether we need to initialize the resharding process.
fn check_init() -> bool {
    todo!()
}

/// Stage 1: init
///
/// Do initialize the resharding process.
fn stage_init() -> CollectionResult<()> {
    todo!()
}

/// Stage 2: migrate points
///
/// Check whether we need to migrate points into the new shard.
fn check_migrate_points() -> bool {
    todo!()
}

/// Stage 2: migrate points
///
/// Do migrate points into the new shard.
fn stage_migrate_points() -> CollectionResult<()> {
    todo!()
}

/// Stage 3: replicate to match replication factor
///
/// Check whether we need to replicate to match replication factor.
fn check_replicate_replication_factor() -> bool {
    todo!()
}

/// Stage 3: replicate to match replication factor
///
/// Do replicate replicate to match replication factor.
fn stage_replicate_replication_factor() -> CollectionResult<()> {
    todo!()
}
