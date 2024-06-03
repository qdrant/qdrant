use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

use super::tasks_pool::ReshardTaskProgress;
use super::ReshardKey;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::channel_service::ChannelService;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::LockedShardHolder;
use crate::shards::transfer::{ShardTransfer, ShardTransferConsensus, ShardTransferMethod};
use crate::shards::CollectionId;

/// Maximum time a point migration transfer might take.
const MIGRATE_POINT_TRANSFER_MAX_DURATION: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Serialize, Deserialize)]
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
    #[serde(rename = "replicate")]
    S3_Replicate,
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
    // TODO: we might want to separate this type into shard transfer and resharding
    consensus: &dyn ShardTransferConsensus,
    collection_id: CollectionId,
    _channel_service: ChannelService,
    _temp_dir: &Path,
) -> CollectionResult<bool> {
    let mut state = DriverState::new(reshard_key);

    // Stage 1: init
    if !completed_init(&state) {
        stage_init(&mut state)?;
    }

    // Stage 2: init
    if !completed_migrate_points(&state) {
        stage_migrate_points(&mut state, shard_holder.clone(), consensus, &collection_id).await?;
    }

    // Stage 3: replicate to match replication factor
    if !completed_replicate() {
        stage_replicate()?;
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

    Ok(true)
}

/// Stage 1: init
///
/// Check whether we need to initialize the resharding process.
fn completed_init(state: &DriverState) -> bool {
    state.all_peers_reached(Stage::S1_InitEnd)
}

/// Stage 1: init
///
/// Do initialize the resharding process.
fn stage_init(state: &mut DriverState) -> CollectionResult<()> {
    state.bump_all_peers_to(Stage::S1_InitEnd);
    todo!();
}

/// Stage 2: migrate points
///
/// Check whether we need to migrate points into the new shard.
fn completed_migrate_points(state: &DriverState) -> bool {
    state.all_peers_reached(Stage::S2_MigratePointsEnd) && state.shards_to_migrate().is_empty()
}

/// Stage 2: migrate points
///
/// Keeps checking what shards are still pending point migrations. For each of them it starts a
/// shard transfer if needed, waiting for them to finish. Once this returns, all points are
/// migrated to the target shard.
async fn stage_migrate_points(
    state: &mut DriverState,
    shard_holder: Arc<LockedShardHolder>,
    consensus: &dyn ShardTransferConsensus,
    collection_id: &CollectionId,
) -> CollectionResult<()> {
    state.bump_all_peers_to(Stage::S2_MigratePointsStart);

    while let Some(source_shard_id) = state.shards_to_migrate().pop() {
        let ongoing_transfer = shard_holder
            .read()
            .await
            .get_transfers(|transfer| {
                transfer.method == Some(ShardTransferMethod::ReshardingStreamRecords)
                    && transfer.shard_id == source_shard_id
                    && transfer.to_shard_id == Some(state.key.shard_id)
            })
            .pop();

        // Get the transfer, if there is no transfer yet, start one now
        let (transfer, start_transfer) = match ongoing_transfer {
            Some(transfer) => (transfer, false),
            None => {
                // TODO: also support local transfers (without consensus?)
                // TODO: do not just pick random source, consider transfer limits
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
                    to: consensus.this_peer_id()?,
                    sync: true,
                    method: Some(ShardTransferMethod::ReshardingStreamRecords),
                    to_shard_id: Some(state.key.shard_id),
                };
                (transfer, true)
            }
        };

        let await_transfer_end = shard_holder
            .read()
            .await
            .await_shard_transfer_end(transfer.key(), MIGRATE_POINT_TRANSFER_MAX_DURATION);

        if start_transfer {
            consensus
                .start_shard_transfer_confirm_and_retry(&transfer, collection_id)
                .await?;
        }

        // Wait for the transfer to finish
        let transfer_result = await_transfer_end.await?;
        if transfer_result.is_err() {
            return Err(CollectionError::service_error(format!(
                "Shard {source_shard_id} failed to be transferred to this node for resharding",
            )));
        }

        state.migrated_shards.push(source_shard_id);
    }

    state.bump_all_peers_to(Stage::S2_MigratePointsEnd);

    Ok(())
}

/// Stage 3: replicate to match replication factor
///
/// Check whether we need to replicate to match replication factor.
fn completed_replicate() -> bool {
    todo!()
}

/// Stage 3: replicate to match replication factor
///
/// Do replicate replicate to match replication factor.
fn stage_replicate() -> CollectionResult<()> {
    todo!()
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
