use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::task::block_in_place;

use super::tasks_pool::ReshardTaskProgress;
use super::ReshardKey;
use crate::config::CollectionConfig;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::save_on_disk::SaveOnDisk;
use crate::shards::channel_service::ChannelService;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::LockedShardHolder;
use crate::shards::transfer::{ShardTransfer, ShardTransferConsensus, ShardTransferMethod};
use crate::shards::CollectionId;

/// Maximum time a point migration transfer might take.
const MIGRATE_POINT_TRANSFER_MAX_DURATION: Duration = Duration::from_secs(24 * 60 * 60);

/// Maximum time a shard replication transfer might take.
const REPLICATE_TRANSFER_MAX_DURATION: Duration = MIGRATE_POINT_TRANSFER_MAX_DURATION;

type PersistedState = SaveOnDisk<DriverState>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DriverState {
    key: ReshardKey,
    /// State of each peer we know about
    peers: HashMap<PeerId, Stage>,
    /// List of shard IDs successfully migrated to the new shard
    migrated_shards: Vec<ShardId>,
}

impl DriverState {
    pub fn new(key: ReshardKey) -> Self {
        Self {
            key,
            peers: HashMap::new(),
            migrated_shards: vec![],
        }
    }

    /// Check whether all peers have reached at least the given stage
    fn all_peers_reached(&self, stage: Stage) -> bool {
        self.peers.values().all(|peer_stage| peer_stage >= &stage)
    }

    /// Bump the state of all peers to at least the given stage.
    fn bump_all_peers_to(&mut self, stage: Stage) {
        self.peers
            .values_mut()
            .for_each(|peer_stage| *peer_stage = stage.max(*peer_stage));
    }

    /// List the shard IDs we still need to migrate.
    pub fn shards_to_migrate(&self) -> Vec<ShardId> {
        self.source_shards()
            .filter(|shard_id| !self.migrated_shards.contains(shard_id))
            .collect()
    }

    /// Get all the shard IDs which points are sourced from.
    pub fn source_shards(&self) -> impl Iterator<Item = ShardId> {
        0..self.key.shard_id
    }
}

/// State of each node while resharding
///
/// Defines the state each node has reached and completed.
///
/// Important: the states in this enum are ordered, from beginning to end!
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
#[serde(rename_all = "snake_case")]
#[allow(non_camel_case_types)]
enum Stage {
    #[serde(rename = "init_start")]
    S1_InitStart,
    #[serde(rename = "init_end")]
    S1_InitEnd,
    #[serde(rename = "migrate_points_start")]
    S2_MigratePointsStart,
    #[serde(rename = "migrate_points_end")]
    S2_MigratePointsEnd,
    #[serde(rename = "replicate_start")]
    S3_ReplicateStart,
    #[serde(rename = "replicate_end")]
    S3_ReplicateEnd,
    #[serde(rename = "commit_hash_ring")]
    S4_CommitHashring,
    #[serde(rename = "propagate_deletes")]
    S5_PropagateDeletes,
    #[serde(rename = "finalize")]
    S6_Finalize,
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
    _progress: Arc<Mutex<ReshardTaskProgress>>,
    shard_holder: Arc<LockedShardHolder>,
    // TODO(resharding): we might want to separate this type into shard transfer and resharding
    consensus: &dyn ShardTransferConsensus,
    collection_id: CollectionId,
    collection_path: PathBuf,
    collection_config: Arc<RwLock<CollectionConfig>>,
    _channel_service: ChannelService,
    _temp_dir: &Path,
) -> CollectionResult<bool> {
    let resharding_state_path = resharding_state_path(&reshard_key, &collection_path);
    let state: PersistedState = SaveOnDisk::load_or_init(&resharding_state_path, || {
        DriverState::new(reshard_key.clone())
    })?;

    // Stage 1: init
    if !completed_init(&state) {
        stage_init(&state)?;
    }

    // Stage 2: init
    if !completed_migrate_points(&state) {
        stage_migrate_points(
            &reshard_key,
            &state,
            shard_holder.clone(),
            consensus,
            &collection_id,
        )
        .await?;
    }

    // Stage 3: replicate to match replication factor
    if !completed_replicate(&reshard_key, &state, &shard_holder, &collection_config).await? {
        stage_replicate(
            &reshard_key,
            &state,
            shard_holder.clone(),
            consensus,
            &collection_id,
            collection_config.clone(),
        )
        .await?;
    }

    // Stage 4: commit new hashring
    if !completed_commit_hashring() {
        stage_commit_hashring()?;
    }

    // Stage 5: propagate deletes
    if !completed_propagate_deletes() {
        stage_propagate_deletes()?;
    }

    // Stage 6: finalize
    stage_finalize()?;

    // Remove the state file after successful resharding
    if let Err(err) = tokio::fs::remove_file(resharding_state_path).await {
        log::error!(
            "Failed to remove resharding state file after successful resharding, ignoring: {err}"
        );
    }

    Ok(true)
}

fn resharding_state_path(reshard_key: &ReshardKey, collection_path: &Path) -> PathBuf {
    collection_path.join(format!("resharding_state_{}.json", reshard_key.shard_id))
}

/// Stage 1: init
///
/// Check whether we need to initialize the resharding process.
fn completed_init(state: &PersistedState) -> bool {
    state.read().all_peers_reached(Stage::S1_InitEnd)
}

/// Stage 1: init
///
/// Do initialize the resharding process.
fn stage_init(state: &PersistedState) -> CollectionResult<()> {
    state.write(|data| {
        data.bump_all_peers_to(Stage::S1_InitEnd);
    })?;
    todo!();
}

/// Stage 2: migrate points
///
/// Check whether we need to migrate points into the new shard.
fn completed_migrate_points(state: &PersistedState) -> bool {
    let state_read = state.read();
    state_read.all_peers_reached(Stage::S2_MigratePointsEnd)
        && state_read.shards_to_migrate().is_empty()
}

/// Stage 2: migrate points
///
/// Keeps checking what shards are still pending point migrations. For each of them it starts a
/// shard transfer if needed, waiting for them to finish. Once this returns, all points are
/// migrated to the target shard.
async fn stage_migrate_points(
    reshard_key: &ReshardKey,
    state: &PersistedState,
    shard_holder: Arc<LockedShardHolder>,
    consensus: &dyn ShardTransferConsensus,
    collection_id: &CollectionId,
) -> CollectionResult<()> {
    state.write(|data| {
        data.bump_all_peers_to(Stage::S2_MigratePointsStart);
    })?;

    while let Some(source_shard_id) = block_in_place(|| state.read().shards_to_migrate().pop()) {
        let ongoing_transfer = shard_holder
            .read()
            .await
            .get_transfers(|transfer| {
                transfer.method == Some(ShardTransferMethod::ReshardingStreamRecords)
                    && transfer.shard_id == source_shard_id
                    && transfer.to_shard_id == Some(reshard_key.shard_id)
            })
            .pop();

        // Get the transfer, start one if there is none
        let (transfer, start_transfer) = match ongoing_transfer {
            Some(transfer) => (transfer, false),
            None => {
                // TODO(resharding): also support local (direct) transfers without consensus
                // TODO(resharding): do not just pick random source, consider transfer limits
                let active_remote_shards = {
                    let shard_holder = shard_holder.read().await;

                    let replica_set =
                        shard_holder.get_shard(&source_shard_id).ok_or_else(|| {
                            CollectionError::service_error(format!(
                        "Shard {source_shard_id} not found in the shard holder for resharding",
                    ))
                        })?;

                    replica_set.active_remote_shards().await
                };
                let source_peer_id = active_remote_shards
                .choose(&mut rand::thread_rng())
                .cloned()
                .ok_or_else(|| {
                    CollectionError::service_error(format!(
                        "No remote peer with shard {source_shard_id} in active state for resharding",
                    ))
                })?;

                let transfer = ShardTransfer {
                    shard_id: source_shard_id,
                    from: source_peer_id,
                    to: consensus.this_peer_id(),
                    sync: true,
                    method: Some(ShardTransferMethod::ReshardingStreamRecords),
                    to_shard_id: Some(reshard_key.shard_id),
                };
                (transfer, true)
            }
        };

        // Create listener for transfer end before proposing to start the transfer
        // That way we're sure we receive all transfer related messages
        let await_transfer_end = shard_holder
            .read()
            .await
            .await_shard_transfer_end(transfer.key(), MIGRATE_POINT_TRANSFER_MAX_DURATION);

        if start_transfer {
            consensus
                .start_shard_transfer_confirm_and_retry(&transfer, collection_id)
                .await?;
        }

        // Wait for transfer to end
        match await_transfer_end.await {
            Ok(Ok(_)) => {
                log::debug!(
                    "Points of shard {source_shard_id} migrated successfully for resharding"
                )
            }
            // Transfer aborted
            Ok(Err(_)) => {
                return Err(CollectionError::service_error(format!(
                    "Shard {source_shard_id} failed to be migrated to this node for resharding, transfer got aborted",
                )));
            }
            // Transfer timed out
            Err(_) => {
                let abort_transfer = consensus
                    .abort_shard_transfer_confirm_and_retry(
                        transfer.key(),
                        collection_id,
                        "resharding migration transfer timed out",
                    )
                    .await;
                if let Err(err) = abort_transfer {
                    log::warn!("Failed to abort migration transfer for shard {source_shard_id} to clean up after timeout, ignoring: {err}");
                }
                return Err(CollectionError::service_error(format!(
                    "Shard {source_shard_id} failed to be migrated to this node for resharding, transfer timed out",
                )));
            }
        }

        state.write(|data| {
            data.migrated_shards.push(source_shard_id);
        })?;
    }

    state.write(|data| {
        data.bump_all_peers_to(Stage::S2_MigratePointsEnd);
    })?;

    Ok(())
}

/// Stage 3: replicate to match replication factor
///
/// Check whether we need to replicate to match replication factor.
async fn completed_replicate(
    reshard_key: &ReshardKey,
    state: &PersistedState,
    shard_holder: &Arc<LockedShardHolder>,
    collection_config: &Arc<RwLock<CollectionConfig>>,
) -> CollectionResult<bool> {
    Ok(state.read().all_peers_reached(Stage::S3_ReplicateEnd)
        && has_enough_replicas(reshard_key, shard_holder, collection_config).await?)
}

/// Check whether we have the desired number of replicas for our new shard.
async fn has_enough_replicas(
    reshard_key: &ReshardKey,
    shard_holder: &Arc<LockedShardHolder>,
    collection_config: &Arc<RwLock<CollectionConfig>>,
) -> CollectionResult<bool> {
    let desired_replication_factor = collection_config
        .read()
        .await
        .params
        .replication_factor
        .get();
    let current_replication_factor = {
        let shard_holder_read = shard_holder.read().await;
        let Some(replica_set) = shard_holder_read.get_shard(&reshard_key.shard_id) else {
            return Err(CollectionError::service_error(format!(
                "Shard {} not found in the shard holder for resharding",
                reshard_key.shard_id,
            )));
        };
        replica_set.peers().len() as u32
    };

    Ok(current_replication_factor >= desired_replication_factor)
}

/// Stage 3: replicate to match replication factor
///
/// Do replicate replicate to match replication factor.
async fn stage_replicate(
    reshard_key: &ReshardKey,
    state: &PersistedState,
    shard_holder: Arc<LockedShardHolder>,
    consensus: &dyn ShardTransferConsensus,
    collection_id: &CollectionId,
    collection_config: Arc<RwLock<CollectionConfig>>,
) -> CollectionResult<()> {
    state.write(|data| {
        data.bump_all_peers_to(Stage::S3_ReplicateStart);
    })?;

    //shard_holder.read().await.shard_transfers.wait_for(check, timeout)

    while !has_enough_replicas(reshard_key, &shard_holder, &collection_config).await? {
        // Select a peer to replicate to, not having a replica yet
        let occupied_peers = {
            let shard_holder_read = shard_holder.read().await;
            let Some(replica_set) = shard_holder_read.get_shard(&reshard_key.shard_id) else {
                return Err(CollectionError::service_error(format!(
                    "Shard {} not found in the shard holder for resharding",
                    reshard_key.shard_id,
                )));
            };
            replica_set.peers().into_keys().collect()
        };
        let all_peers = consensus.peers().into_iter().collect::<HashSet<_>>();
        let candidate_peers: Vec<_> = all_peers.difference(&occupied_peers).cloned().collect();
        // TODO(resharding): do not just pick random source, consider shard distribution
        let Some(target_peer) = candidate_peers.choose(&mut rand::thread_rng()).cloned() else {
            log::warn!("Resharding could not match desired replication factors as all peers are occupied, continuing with lower replication factor");
            break;
        };

        let transfer = ShardTransfer {
            shard_id: reshard_key.shard_id,
            from: consensus.this_peer_id(),
            to: target_peer,
            sync: true,
            // TODO(resharding): define preferred shard transfer method here!
            method: Some(ShardTransferMethod::default()),
            to_shard_id: None,
        };

        // Create listener for transfer end before proposing to start the transfer
        // That way we're sure we receive all transfer related messages
        let await_transfer_end = shard_holder
            .read()
            .await
            .await_shard_transfer_end(transfer.key(), REPLICATE_TRANSFER_MAX_DURATION);

        consensus
            .start_shard_transfer_confirm_and_retry(&transfer, collection_id)
            .await?;

        // Wait for transfer to end
        match await_transfer_end.await {
            Ok(Ok(_)) => {
                log::debug!(
                    "Shard {} successfully replicated to peer {target_peer} for resharding",
                    reshard_key.shard_id,
                )
            }
            // Transfer aborted
            Ok(Err(_)) => {
                return Err(CollectionError::service_error(format!(
                    "Shard {} failed to be replicated to peer {target_peer} for resharding, transfer got aborted",
                    reshard_key.shard_id,
                )));
            }
            // Transfer timed out
            Err(_) => {
                let abort_transfer = consensus
                    .abort_shard_transfer_confirm_and_retry(
                        transfer.key(),
                        collection_id,
                        "resharding replication transfer timed out",
                    )
                    .await;
                if let Err(err) = abort_transfer {
                    log::warn!("Failed to abort replication transfer for shard {} to node {target_peer} to clean up after timeout, ignoring: {err}", reshard_key.shard_id);
                }
                return Err(CollectionError::service_error(format!(
                    "Shard {} failed to be replicated to peer {target_peer} for resharding, transfer timed out",
                    reshard_key.shard_id,
                )));
            }
        }
    }

    state.write(|data| {
        data.bump_all_peers_to(Stage::S3_ReplicateEnd);
    })?;

    Ok(())
}

/// Stage 4: commit new hashring
///
/// Check whether the new hashring still needs to be committed.
fn completed_commit_hashring() -> bool {
    todo!()
}

/// Stage 4: commit new hashring
///
/// Do commit the new hashring.
fn stage_commit_hashring() -> CollectionResult<()> {
    todo!()
}

/// Stage 5: propagate deletes
///
/// Check whether migrated points still need to be deleted in their old shards.
fn completed_propagate_deletes() -> bool {
    todo!()
}

/// Stage 5: commit new hashring
///
/// Do delete migrated points from their old shards.
fn stage_propagate_deletes() -> CollectionResult<()> {
    todo!()
}

/// Stage 6: finalize
///
/// Finalize the resharding operation.
fn stage_finalize() -> CollectionResult<()> {
    todo!()
}
