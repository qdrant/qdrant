use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::tasks_pool::ReshardTaskProgress;
use super::ReshardKey;
use crate::operations::types::CollectionResult;
use crate::shards::channel_service::ChannelService;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::LockedShardHolder;
use crate::shards::transfer::ShardTransferConsensus;
use crate::shards::CollectionId;

#[derive(Debug, Serialize, Deserialize)]
struct DriverState {
    key: ReshardKey,

    /// State of each peer we know about
    peers: HashMap<PeerId, Stage>,

    /// List of shard IDs successfully migrated to the new shard.
    migrated_shard_ids: Vec<u32>,
}

impl DriverState {
    pub fn new(key: ReshardKey) -> Self {
        Self {
            key,
            peers: HashMap::new(),
            migrated_shard_ids: vec![],
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
    pub fn shard_ids_to_migrate(&self) -> Vec<ShardId> {
        self.source_shard_ids()
            .filter(|shard_id| !self.migrated_shard_ids.contains(shard_id))
            .collect()
    }

    /// Get all the shard IDs which points are sourced from.
    pub fn source_shard_ids(&self) -> impl Iterator<Item = ShardId> {
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
    _shard_holder: Arc<LockedShardHolder>,
    _consensus: &dyn ShardTransferConsensus,
    _collection_id: CollectionId,
    _channel_service: ChannelService,
    _temp_dir: &Path,
) -> CollectionResult<bool> {
    let mut state = DriverState::new(reshard_key);

    // Stage 1: init
    if !completed_init(&state) {
        stage_init(&mut state)?;
    }

    // Stage 2: init
    if !check_migrate_points() {
        stage_migrate_points()?;
    }

    // Stage 3: replicate to match replication factor
    if !check_replicate_replication_factor() {
        stage_replicate_replication_factor()?;
    }

    // Stage 4: commit new hashring
    if !check_commit_hashring() {
        stage_commit_hashring()?;
    }

    // Stage 5: propagate deletes
    if !check_propagate_deletes() {
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

/// Stage 4: commit new hashring
///
/// Check whether the new hashring still needs to be committed.
fn check_commit_hashring() -> bool {
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
fn check_propagate_deletes() -> bool {
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
