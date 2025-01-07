pub mod driver;
pub mod tasks_pool;

mod stage_commit_read_hashring;
mod stage_commit_write_hashring;
mod stage_finalize;
mod stage_init;
mod stage_migrate_points;
mod stage_propagate_deletes;
mod stage_replicate;

use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use driver::drive_resharding;
use parking_lot::Mutex;
use schemars::JsonSchema;
use segment::types::ShardKey;
use serde::{Deserialize, Serialize};
use tasks_pool::ReshardTaskProgress;
use tokio::sync::RwLock;
use tokio::time::sleep;
use uuid::Uuid;

use super::shard::{PeerId, ShardId};
use super::transfer::ShardTransferConsensus;
use crate::common::stoppable_task_async::{spawn_async_cancellable, CancellableAsyncTaskHandle};
use crate::config::CollectionConfigInternal;
use crate::operations::cluster_ops::ReshardingDirection;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::shards::channel_service::ChannelService;
use crate::shards::shard_holder::LockedShardHolder;
use crate::shards::CollectionId;

const RETRY_DELAY: Duration = Duration::from_secs(1);
pub(crate) const MAX_RETRY_COUNT: usize = 3;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReshardState {
    pub uuid: Uuid,
    pub peer_id: PeerId,
    pub shard_id: ShardId,
    pub shard_key: Option<ShardKey>,
    pub direction: ReshardingDirection,
    pub stage: ReshardStage,
}

impl ReshardState {
    pub fn new(
        uuid: Uuid,
        direction: ReshardingDirection,
        peer_id: PeerId,
        shard_id: ShardId,
        shard_key: Option<ShardKey>,
    ) -> Self {
        Self {
            uuid,
            direction,
            peer_id,
            shard_id,
            shard_key,
            stage: ReshardStage::MigratingPoints,
        }
    }

    pub fn matches(&self, key: &ReshardKey) -> bool {
        self.uuid == key.uuid
            && self.direction == key.direction
            && self.peer_id == key.peer_id
            && self.shard_id == key.shard_id
            && self.shard_key == key.shard_key
    }

    pub fn key(&self) -> ReshardKey {
        ReshardKey {
            uuid: self.uuid,
            direction: self.direction,
            peer_id: self.peer_id,
            shard_id: self.shard_id,
            shard_key: self.shard_key.clone(),
        }
    }
}

/// Reshard stages
///
/// # Warning
///
/// This enum is ordered!
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReshardStage {
    #[default]
    MigratingPoints,
    ReadHashRingCommitted,
    WriteHashRingCommitted,
}

/// Unique identifier of a resharding task
#[derive(Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize, JsonSchema)]
pub struct ReshardKey {
    #[schemars(skip)]
    pub uuid: Uuid,
    #[serde(default)]
    pub direction: ReshardingDirection,
    pub peer_id: PeerId,
    pub shard_id: ShardId,
    pub shard_key: Option<ShardKey>,
}

impl fmt::Display for ReshardKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{:?}", self.peer_id, self.shard_id, self.shard_key)
    }
}

// TODO(resharding): move this into driver module?
#[allow(clippy::too_many_arguments)]
pub fn spawn_resharding_task<T, F>(
    shards_holder: Arc<LockedShardHolder>,
    progress: Arc<Mutex<ReshardTaskProgress>>,
    reshard_key: ReshardKey,
    consensus: Box<dyn ShardTransferConsensus>,
    collection_id: CollectionId,
    collection_path: PathBuf,
    collection_config: Arc<RwLock<CollectionConfigInternal>>,
    shared_storage_config: Arc<SharedStorageConfig>,
    channel_service: ChannelService,
    can_resume: bool,
    on_finish: T,
    on_error: F,
) -> CancellableAsyncTaskHandle<bool>
where
    T: Future<Output = ()> + Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    spawn_async_cancellable(move |cancel| async move {
        let mut result = Err(cancel::Error::Cancelled);

        for attempt in 0..MAX_RETRY_COUNT {
            let future = async {
                if attempt > 0 {
                    sleep(RETRY_DELAY * attempt as u32).await;

                    log::warn!(
                        "Retrying resharding {collection_id}:{} (retry {attempt})",
                        reshard_key.shard_id,
                    );
                }

                drive_resharding(
                    reshard_key.clone(),
                    progress.clone(),
                    shards_holder.clone(),
                    consensus.as_ref(),
                    collection_id.clone(),
                    collection_path.clone(),
                    collection_config.clone(),
                    &shared_storage_config,
                    channel_service.clone(),
                    can_resume,
                )
                .await
            };

            result = cancel::future::cancel_on_token(cancel.clone(), future).await;

            let is_ok = matches!(result, Ok(Ok(true)));
            let is_cancelled = result.is_err() || matches!(result, Ok(Ok(false)));

            if let Ok(Err(err)) = &result {
                log::error!(
                    "Failed to reshard {collection_id}:{}: {err}",
                    reshard_key.shard_id,
                );
            }

            if is_ok || is_cancelled {
                break;
            }
        }

        match &result {
            Ok(Ok(true)) => on_finish.await,
            Ok(Ok(false)) => (), // do nothing, we should not finish the task
            Ok(Err(_)) => on_error.await,
            Err(_) => (), // do nothing, if task was cancelled
        }

        let is_ok_and_finish = matches!(result, Ok(Ok(true)));
        is_ok_and_finish
    })
}
