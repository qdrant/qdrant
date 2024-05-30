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
    todo!("implement driver steps here");
    Ok(true)
}
