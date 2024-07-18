use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::Future;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::time::sleep;

use super::tasks_pool::ReshardTaskProgress;
use super::ReshardKey;
use crate::config::CollectionConfig;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::save_on_disk::SaveOnDisk;
use crate::shards::channel_service::ChannelService;
use crate::shards::resharding::{
    stage_commit_hashring, stage_finalize, stage_init, stage_migrate_points,
    stage_propagate_deletes, stage_replicate,
};
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::LockedShardHolder;
use crate::shards::transfer::{ShardTransfer, ShardTransferConsensus};
use crate::shards::CollectionId;

/// Interval for the sanity check while awaiting shard transfers.
const AWAIT_SHARD_TRANSFER_SANITY_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// If the shard transfer IO limit is reached, retry with this interval.
pub const SHARD_TRANSFER_IO_LIMIT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

pub(super) type PersistedState = SaveOnDisk<DriverState>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DriverState {
    key: ReshardKey,
    /// Stage each peer is currently in
    peers: HashMap<PeerId, Stage>,
    /// List of shard IDs that must be migrated into the new shard
    source_shard_ids: HashSet<ShardId>,
    /// List of shard IDs successfully migrated to the new shard
    pub migrated_shards: Vec<ShardId>,
    /// List of shard IDs in which we successfully deleted migrated points
    pub deleted_shards: Vec<ShardId>,
}

impl DriverState {
    pub fn new(key: ReshardKey, source_shard_ids: HashSet<ShardId>, peers: &[PeerId]) -> Self {
        Self {
            key,
            peers: HashMap::from_iter(peers.iter().map(|peer_id| (*peer_id, Stage::default()))),
            source_shard_ids,
            migrated_shards: vec![],
            deleted_shards: vec![],
        }
    }

    /// Update the resharding state, must be called periodically
    pub fn update(
        &mut self,
        progress: &Mutex<ReshardTaskProgress>,
        consensus: &dyn ShardTransferConsensus,
    ) {
        self.sync_peers(&consensus.peers());
        progress.lock().description.replace(self.describe());
    }

    /// Sync the peers we know about with this state.
    ///
    /// This will update this driver state to have exactly the peers given in the list. New peers
    /// are initialized with the default stage, now unknown peers are removed.
    pub fn sync_peers(&mut self, peers: &[PeerId]) {
        self.peers.retain(|peer_id, _| peers.contains(peer_id));
        for peer_id in peers {
            self.peers.entry(*peer_id).or_default();
        }
    }

    /// Check whether all peers have reached at least the given stage
    pub fn all_peers_completed(&self, stage: Stage) -> bool {
        self.peers.values().all(|peer_stage| peer_stage > &stage)
    }

    /// Bump the state of all peers to at least the given stage.
    pub fn complete_for_all_peers(&mut self, stage: Stage) {
        let next_stage = stage.next();
        self.peers
            .values_mut()
            .for_each(|peer_stage| *peer_stage = next_stage.max(*peer_stage));
    }

    /// List the shard IDs we still need to migrate.
    pub fn shards_to_migrate(&self) -> impl Iterator<Item = ShardId> + '_ {
        self.source_shards()
            .filter(|shard_id| !self.migrated_shards.contains(shard_id))
    }

    /// List the shard IDs in which we still need to propagate point deletions.
    pub fn shards_to_delete(&self) -> impl Iterator<Item = ShardId> + '_ {
        self.source_shards()
            .filter(|shard_id| !self.deleted_shards.contains(shard_id))
    }

    /// Get all the shard IDs which points are sourced from.
    pub fn source_shards(&self) -> impl Iterator<Item = ShardId> + '_ {
        self.source_shard_ids.iter().copied()
    }

    /// Describe the current stage and state in a human readable string.
    pub fn describe(&self) -> String {
        let Some(lowest_stage) = self.peers.values().min() else {
            return "unknown: no known peers".into();
        };

        match lowest_stage {
            Stage::S1_Init => "initialize".into(),
            Stage::S2_MigratePoints => format!(
                "migrate points: migrating points from shards {:?} to {}",
                self.shards_to_migrate().collect::<Vec<_>>(),
                self.key.shard_id,
            ),
            Stage::S3_Replicate => "replicate: replicate new shard to other peers".into(),
            Stage::S4_CommitHashring => "commit hash ring: switching reads and writes".into(),
            Stage::S5_PropagateDeletes => format!(
                "propagate deletes: deleting migrated points from shards {:?}",
                self.shards_to_delete().collect::<Vec<_>>(),
            ),
            Stage::S6_Finalize => "finalize".into(),
            Stage::Finished => "finished".into(),
        }
    }
}

/// State of each node while resharding
///
/// Defines the state each node has reached and completed.
///
/// Important: the states in this enum are ordered, from beginning to end!
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
#[serde(rename_all = "snake_case")]
#[allow(non_camel_case_types)]
pub(super) enum Stage {
    #[default]
    #[serde(rename = "init")]
    S1_Init,
    #[serde(rename = "migrate_points")]
    S2_MigratePoints,
    #[serde(rename = "replicate")]
    S3_Replicate,
    #[serde(rename = "commit_hash_ring")]
    S4_CommitHashring,
    #[serde(rename = "propagate_deletes")]
    S5_PropagateDeletes,
    #[serde(rename = "finalize")]
    S6_Finalize,
    #[serde(rename = "finished")]
    Finished,
}

impl Stage {
    pub fn next(self) -> Self {
        match self {
            Self::S1_Init => Self::S2_MigratePoints,
            Self::S2_MigratePoints => Self::S3_Replicate,
            Self::S3_Replicate => Self::S4_CommitHashring,
            Self::S4_CommitHashring => Self::S5_PropagateDeletes,
            Self::S5_PropagateDeletes => Self::S6_Finalize,
            Self::S6_Finalize => Self::Finished,
            Self::Finished => unreachable!(),
        }
    }
}

/// Drive the resharding on the target node based on the given configuration
///
/// Returns `true` if we should finalize resharding. Returns `false` if we should silently
/// drop it, because it is being restarted.
///
/// Sequence based on: <https://www.notion.so/qdrant/7b3c60d7843c4c7a945848f81dbdc1a1>
///
/// # Cancel safety
///
/// This function is cancel safe.
#[allow(clippy::too_many_arguments)]
pub async fn drive_resharding(
    reshard_key: ReshardKey,
    progress: Arc<Mutex<ReshardTaskProgress>>,
    shard_holder: Arc<LockedShardHolder>,
    consensus: &dyn ShardTransferConsensus,
    collection_id: CollectionId,
    collection_path: PathBuf,
    collection_config: Arc<RwLock<CollectionConfig>>,
    shared_storage_config: &SharedStorageConfig,
    channel_service: ChannelService,
    can_resume: bool,
) -> CollectionResult<bool> {
    let to_shard_id = reshard_key.shard_id;
    let hash_ring = shard_holder
        .read()
        .await
        .rings
        .get(&reshard_key.shard_key)
        .cloned()
        .unwrap();
    let resharding_state_path = resharding_state_path(&reshard_key, &collection_path);

    // Load or initialize resharding state
    let init_state = || {
        let mut shard_ids = hash_ring.unique_nodes();
        shard_ids.remove(&reshard_key.shard_id);

        DriverState::new(reshard_key.clone(), shard_ids, &consensus.peers())
    };
    let state: PersistedState = if can_resume {
        SaveOnDisk::load_or_init(&resharding_state_path, init_state)?
    } else {
        SaveOnDisk::new(&resharding_state_path, init_state())?
    };

    progress.lock().description.replace(state.read().describe());

    log::debug!(
        "Resharding {collection_id}:{to_shard_id} from shards {:?}",
        state.read().source_shards().collect::<Vec<_>>(),
    );

    // Stage 1: init
    if !stage_init::is_completed(&state) {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: init");
        stage_init::drive(&state, &progress, consensus)?;
    }

    // Stage 2: migrate points
    if !stage_migrate_points::is_completed(&state) {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: migrate points");
        stage_migrate_points::drive(
            &reshard_key,
            &state,
            &progress,
            shard_holder.clone(),
            consensus,
            &channel_service,
            &collection_id,
            shared_storage_config,
        )
        .await?;
    }

    // Stage 3: replicate to match replication factor
    if !stage_replicate::is_completed(&reshard_key, &state, &shard_holder, &collection_config)
        .await?
    {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: replicate");
        stage_replicate::drive(
            &reshard_key,
            &state,
            &progress,
            shard_holder.clone(),
            consensus,
            &collection_id,
            collection_config.clone(),
            shared_storage_config,
        )
        .await?;
    }

    // Stage 4: commit new hashring
    if !stage_commit_hashring::is_completed(&state) {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: commit hashring");
        stage_commit_hashring::drive(
            &reshard_key,
            &state,
            &progress,
            consensus,
            &channel_service,
            &collection_id,
        )
        .await?;
    }

    // Stage 5: propagate deletes
    if !stage_propagate_deletes::is_completed(&state) {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: propagate deletes");
        stage_propagate_deletes::drive(
            &reshard_key,
            &state,
            &progress,
            shard_holder.clone(),
            consensus,
        )
        .await?;
    }

    // Stage 6: finalize
    log::debug!("Resharding {collection_id}:{to_shard_id} stage: finalize");
    stage_finalize::drive(&state, &progress, consensus)?;

    // Delete the state file after successful resharding
    if let Err(err) = state.delete().await {
        log::error!(
            "Failed to remove resharding state file after successful resharding, ignoring: {err}"
        );
    }

    Ok(true)
}

fn resharding_state_path(reshard_key: &ReshardKey, collection_path: &Path) -> PathBuf {
    collection_path.join(format!("resharding_state_{}.json", reshard_key.shard_id))
}

/// Await for a resharding shard transfer to succeed.
///
/// Yields on a successful transfer.
///
/// Returns an error if:
/// - the transfer failed or got aborted
/// - the transfer timed out
/// - no matching transfer is ongoing; it never started or went missing without a notification
///
/// Yields on a successful transfer. Returns an error if an error occurred or if the global timeout
/// is reached.
pub(super) async fn await_transfer_success(
    reshard_key: &ReshardKey,
    transfer: &ShardTransfer,
    shard_holder: &Arc<LockedShardHolder>,
    collection_id: &CollectionId,
    consensus: &dyn ShardTransferConsensus,
    await_transfer_end: impl Future<Output = CollectionResult<Result<(), ()>>>,
) -> CollectionResult<()> {
    // Periodic sanity check, returns if the shard transfer we're waiting on has gone missing
    // Prevents this await getting stuck indefinitely
    let sanity_check = async {
        let transfer_key = transfer.key();
        while shard_holder
            .read()
            .await
            .check_transfer_exists(&transfer_key)
        {
            sleep(AWAIT_SHARD_TRANSFER_SANITY_CHECK_INTERVAL).await;
        }

        // Give our normal logic time process the transfer end
        sleep(Duration::from_secs(1)).await;
    };

    tokio::select! {
        biased;
        // Await the transfer end
        result = await_transfer_end => match result {
            Ok(Ok(_)) => Ok(()),
            // Transfer aborted
            Ok(Err(_)) => {
                Err(CollectionError::service_error(format!(
                            "Transfer of shard {} failed, transfer got aborted",
                            reshard_key.shard_id,
                )))
            }
            // Transfer timed out
            Err(_) => {
                let abort_transfer = consensus
                    .abort_shard_transfer_confirm_and_retry(
                        transfer.key(),
                        collection_id,
                        "resharding transfer transfer timed out",
                    )
                    .await;
                if let Err(err) = abort_transfer {
                    log::warn!("Failed to abort shard transfer for shard {} resharding to clean up after timeout, ignoring: {err}", reshard_key.shard_id);
                }
                Err(CollectionError::service_error(format!(
                            "Transfer of shard {} failed, transfer timed out",
                            reshard_key.shard_id,
                )))
            }
        },
        // Sanity check to ensure the tranfser is still ongoing and we're waiting on something
        _ = sanity_check => {
            debug_assert!(false, "no transfer for shard {}, it never properly started or we missed the end notification for it", reshard_key.shard_id);
            Err(CollectionError::service_error(format!(
                "No transfer for shard {} exists, assuming it failed",
                reshard_key.shard_id,
            )))
        },
    }
}
