use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::Future;
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::task::block_in_place;

use super::tasks_pool::ReshardTaskProgress;
use super::ReshardKey;
use crate::config::CollectionConfig;
use crate::operations::point_ops::{PointOperations, WriteOrdering};
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::CollectionUpdateOperations;
use crate::save_on_disk::SaveOnDisk;
use crate::shards::channel_service::ChannelService;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::LockedShardHolder;
use crate::shards::transfer::resharding_stream_records::transfer_resharding_stream_records;
use crate::shards::transfer::transfer_tasks_pool::TransferTaskProgress;
use crate::shards::transfer::{ShardTransfer, ShardTransferConsensus, ShardTransferMethod};
use crate::shards::{await_consensus_sync, CollectionId};

/// Maximum time a point migration transfer might take.
const MIGRATE_POINT_TRANSFER_MAX_DURATION: Duration = Duration::from_secs(24 * 60 * 60);

/// Maximum time a shard replication transfer might take.
const REPLICATE_TRANSFER_MAX_DURATION: Duration = MIGRATE_POINT_TRANSFER_MAX_DURATION;

/// Interval for the sanity check while awaiting shard transfers.
const AWAIT_SHARD_TRANSFER_SANITY_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// Batch size for deleting migrated points in existing shards.
const DELETE_BATCH_SIZE: usize = 500;

type PersistedState = SaveOnDisk<DriverState>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DriverState {
    key: ReshardKey,
    /// Stage each peer is currently in
    peers: HashMap<PeerId, Stage>,
    /// List of shard IDs successfully migrated to the new shard
    migrated_shards: Vec<ShardId>,
    /// List of shard IDs in which we successfully deleted migrated points
    deleted_shards: Vec<ShardId>,
}

impl DriverState {
    pub fn new(key: ReshardKey, peers: &[PeerId]) -> Self {
        Self {
            key,
            peers: HashMap::from_iter(peers.iter().map(|peer_id| (*peer_id, Stage::default()))),
            migrated_shards: vec![],
            deleted_shards: vec![],
        }
    }

    /// Sync the peers we know about with this state.
    ///
    /// This will update this driver state to have exactly the peers given in the list. New peers
    /// are initialized with the default stage, now unknown peers are removed.
    fn sync_peers(&mut self, peers: &[PeerId]) {
        self.peers.retain(|peer_id, _| peers.contains(peer_id));
        for peer_id in peers {
            self.peers.entry(*peer_id).or_default();
        }
    }

    /// Check whether all peers have reached at least the given stage
    fn all_peers_completed(&self, stage: Stage) -> bool {
        self.peers.values().all(|peer_stage| peer_stage > &stage)
    }

    /// Bump the state of all peers to at least the given stage.
    fn complete_for_all_peers(&mut self, stage: Stage) {
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
    pub fn source_shards(&self) -> impl Iterator<Item = ShardId> {
        0..self.key.shard_id
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
                self.shards_to_migrate().collect::<Vec<_>>(),
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
enum Stage {
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
            Self::Finished => unreachable!()
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
    _progress: Arc<Mutex<ReshardTaskProgress>>,
    shard_holder: Arc<LockedShardHolder>,
    consensus: &dyn ShardTransferConsensus,
    collection_id: CollectionId,
    collection_path: PathBuf,
    collection_config: Arc<RwLock<CollectionConfig>>,
    shared_storage_config: &SharedStorageConfig,
    channel_service: ChannelService,
    _temp_dir: &Path,
) -> CollectionResult<bool> {
    let to_shard_id = reshard_key.shard_id;
    let resharding_state_path = resharding_state_path(&reshard_key, &collection_path);
    let state: PersistedState = SaveOnDisk::load_or_init(&resharding_state_path, || {
        DriverState::new(reshard_key.clone(), &consensus.peers())
    })?;

    state.write(|data| {
        data.sync_peers(&consensus.peers());
    })?;

    // Stage 1: init
    if !completed_init(&state) {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: init");
        stage_init(&state)?;
    }

    // Stage 2: init
    if !completed_migrate_points(&state) {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: migrate points");
        stage_migrate_points(
            &reshard_key,
            &state,
            shard_holder.clone(),
            consensus,
            &channel_service,
            &collection_id,
        )
        .await?;
    }

    // Stage 3: replicate to match replication factor
    if !completed_replicate(&reshard_key, &state, &shard_holder, &collection_config).await? {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: replicate");
        stage_replicate(
            &reshard_key,
            &state,
            shard_holder.clone(),
            consensus,
            &collection_id,
            collection_config.clone(),
            shared_storage_config,
        )
        .await?;
    }

    // Stage 4: commit new hashring
    if !completed_commit_hashring(&state) {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: commit hashring");
        stage_commit_hashring(
            &reshard_key,
            &state,
            consensus,
            &channel_service,
            &collection_id,
        )
        .await?;
    }

    // Stage 5: propagate deletes
    if !completed_propagate_deletes(&state) {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: propagate deletes");
        stage_propagate_deletes(&reshard_key, &state, shard_holder.clone()).await?;
    }

    // Stage 6: finalize
    log::debug!("Resharding {collection_id}:{to_shard_id} stage: finalize");
    stage_finalize(&state)?;

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
    state.read().all_peers_completed(Stage::S1_Init)
}

/// Stage 1: init
///
/// Do initialize the resharding process.
fn stage_init(state: &PersistedState) -> CollectionResult<()> {
    state.write(|data| {
        data.complete_for_all_peers(Stage::S1_Init);
    })?;

    Ok(())
}

/// Stage 2: migrate points
///
/// Check whether we need to migrate points into the new shard.
fn completed_migrate_points(state: &PersistedState) -> bool {
    let state_read = state.read();
    state_read.all_peers_completed(Stage::S2_MigratePoints)
        && state_read.shards_to_migrate().next().is_none()
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
    channel_service: &ChannelService,
    collection_id: &CollectionId,
) -> CollectionResult<()> {
    while let Some(source_shard_id) = block_in_place(|| state.read().shards_to_migrate().next()) {
        let ongoing_transfer = shard_holder
            .read()
            .await
            .get_transfers(|transfer| {
                transfer.method == Some(ShardTransferMethod::ReshardingStreamRecords)
                    && transfer.shard_id == source_shard_id
                    && transfer.to_shard_id == Some(reshard_key.shard_id)
            })
            .pop();

        // Take the existing transfer if ongoing, or decide on what new transfer we want to start
        let (transfer, start_transfer) = match ongoing_transfer {
            Some(transfer) => (Some(transfer), false),
            None => {
                // TODO(resharding): do not just pick random source, consider transfer limits
                let active_shards = {
                    let shard_holder = shard_holder.read().await;
                    let replica_set =
                        shard_holder.get_shard(&source_shard_id).ok_or_else(|| {
                            CollectionError::service_error(format!(
                                "Shard {source_shard_id} not found in the shard holder for resharding",
                            ))
                        })?;
                    replica_set.active_shards().await
                };
                let source_peer_id = active_shards
                    .choose(&mut rand::thread_rng())
                    .cloned()
                    .ok_or_else(|| {
                        CollectionError::service_error(format!(
                            "No remote peer with shard {source_shard_id} in active state for resharding",
                        ))
                    })?;

                // Configure shard transfer object, or use none if doing a local transfer
                if source_peer_id != consensus.this_peer_id() {
                    debug_assert_ne!(source_peer_id, consensus.this_peer_id());
                    debug_assert_ne!(source_shard_id, reshard_key.shard_id);
                    let transfer = ShardTransfer {
                        shard_id: source_shard_id,
                        to_shard_id: Some(reshard_key.shard_id),
                        from: source_peer_id,
                        to: consensus.this_peer_id(),
                        sync: true,
                        method: Some(ShardTransferMethod::ReshardingStreamRecords),
                    };
                    (Some(transfer), true)
                } else {
                    (None, false)
                }
            }
        };

        match transfer {
            // Transfer from a different peer, start the transfer if needed and await completion
            Some(transfer) => {
                // Create listener for transfer end before proposing to start the transfer
                // That way we're sure we receive all transfer notifications the next operation might create
                let await_transfer_end = shard_holder
                    .read()
                    .await
                    .await_shard_transfer_end(transfer.key(), MIGRATE_POINT_TRANSFER_MAX_DURATION);

                if start_transfer {
                    consensus
                        .start_shard_transfer_confirm_and_retry(&transfer, collection_id)
                        .await?;
                }

                await_transfer_success(
                    reshard_key,
                    &transfer,
                    &shard_holder,
                    collection_id,
                    consensus,
                    await_transfer_end,
                )
                .await
                .map_err(|err| {
                    CollectionError::service_error(format!(
                        "Failed to migrate points from shard {source_shard_id} to {} for resharding: {err}",
                        reshard_key.shard_id,
                    ))
                })?;
            }
            // Transfer locally, within this peer
            None => {
                migrate_local(
                    reshard_key,
                    shard_holder.clone(),
                    consensus,
                    channel_service.clone(),
                    collection_id,
                    source_shard_id,
                )
                .await?;
            }
        }

        state.write(|data| {
            data.migrated_shards.push(source_shard_id);
        })?;
        log::debug!(
            "Points of shard {source_shard_id} successfully migrated into shard {} for resharding",
            reshard_key.shard_id,
        );
    }

    // Switch new shard on this node into active state
    consensus
        .set_shard_replica_set_state_confirm_and_retry(
            collection_id,
            reshard_key.shard_id,
            ReplicaState::Active,
            Some(ReplicaState::Resharding),
        )
        .await?;

    state.write(|data| {
        data.complete_for_all_peers(Stage::S2_MigratePoints);
    })?;

    Ok(())
}

/// Migrate a shard locally, within the same node.
///
/// This is a special case for migration transfers, because normal shard transfer don't support the
/// same source and target node.
// TODO(resharding): improve this, don't rely on shard transfers and remote shards, copy directly
// between the two local shard replica
async fn migrate_local(
    reshard_key: &ReshardKey,
    shard_holder: Arc<LockedShardHolder>,
    consensus: &dyn ShardTransferConsensus,
    channel_service: ChannelService,
    collection_id: &CollectionId,
    source_shard_id: ShardId,
) -> CollectionResult<()> {
    log::debug!(
        "Migrating points of shard {source_shard_id} into shard {} locally for resharding",
        reshard_key.shard_id,
    );

    // Target shard is on the same node, but has a different shard ID
    let target_shard = RemoteShard::new(
        reshard_key.shard_id,
        collection_id.clone(),
        consensus.this_peer_id(),
        channel_service,
    );

    let progress = Arc::new(Mutex::new(TransferTaskProgress::new()));
    transfer_resharding_stream_records(
        shard_holder,
        progress,
        source_shard_id,
        target_shard,
        collection_id,
    )
    .await?;

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
    Ok(state.read().all_peers_completed(Stage::S3_Replicate)
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
    shared_storage_config: &SharedStorageConfig,
) -> CollectionResult<()> {
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
            to_shard_id: None,
            from: consensus.this_peer_id(),
            to: target_peer,
            sync: true,
            method: Some(
                shared_storage_config
                    .default_shard_transfer_method
                    .unwrap_or_default(),
            ),
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

        // Await transfer success
        await_transfer_success(
            reshard_key,
            &transfer,
            &shard_holder,
            collection_id,
            consensus,
            await_transfer_end,
        )
        .await
        .map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to replicate shard {} to peer {target_peer} for resharding: {err}",
                reshard_key.shard_id
            ))
        })?;
        log::debug!(
            "Shard {} successfully replicated to peer {target_peer} for resharding",
            reshard_key.shard_id,
        );
    }

    state.write(|data| {
        data.complete_for_all_peers(Stage::S3_Replicate);
    })?;

    Ok(())
}

/// Stage 4: commit new hashring
///
/// Check whether the new hashring still needs to be committed.
fn completed_commit_hashring(state: &PersistedState) -> bool {
    state.read().all_peers_completed(Stage::S4_CommitHashring)
}

/// Stage 4: commit new hashring
///
/// Do commit the new hashring.
async fn stage_commit_hashring(
    reshard_key: &ReshardKey,
    state: &PersistedState,
    consensus: &dyn ShardTransferConsensus,
    channel_service: &ChannelService,
    collection_id: &CollectionId,
) -> CollectionResult<()> {
    // Commit read hashring and sync cluster
    consensus
        .commit_read_hashring_confirm_and_retry(collection_id, reshard_key)
        .await?;
    await_consensus_sync(consensus, channel_service).await;

    // Commit write hashring and sync cluster
    consensus
        .commit_write_hashring_confirm_and_retry(collection_id, reshard_key)
        .await?;
    await_consensus_sync(consensus, channel_service).await;

    state.write(|data| {
        data.complete_for_all_peers(Stage::S4_CommitHashring);
    })?;

    Ok(())
}

/// Stage 5: propagate deletes
///
/// Check whether migrated points still need to be deleted in their old shards.
fn completed_propagate_deletes(state: &PersistedState) -> bool {
    let state_read = state.read();
    state_read.all_peers_completed(Stage::S5_PropagateDeletes)
        && state_read.shards_to_delete().next().is_none()
}

/// Stage 5: commit new hashring
///
/// Do delete migrated points from their old shards.
// TODO(resharding): this is a naive implementation, delete by hashring filter directly!
async fn stage_propagate_deletes(
    reshard_key: &ReshardKey,
    state: &PersistedState,
    shard_holder: Arc<LockedShardHolder>,
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

            let replica_set = shard_holder.get_shard(&source_shard_id).ok_or_else(|| {
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

            replica_set
                .update_with_consistency(operation, offset.is_none(), WriteOrdering::Weak)
                .await?;

            if offset.is_none() {
                break;
            }
        }

        state.write(|data| {
            data.deleted_shards.push(source_shard_id);
        })?;
    }

    state.write(|data| {
        data.complete_for_all_peers(Stage::S5_PropagateDeletes);
    })?;

    Ok(())
}

/// Stage 6: finalize
///
/// Finalize the resharding operation.
fn stage_finalize(state: &PersistedState) -> CollectionResult<()> {
    state.write(|data| {
        data.complete_for_all_peers(Stage::S6_Finalize);
    })?;

    Ok(())
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
async fn await_transfer_success(
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
            tokio::time::sleep(AWAIT_SHARD_TRANSFER_SANITY_CHECK_INTERVAL).await;
        }

        // Give our normal logic time process the transfer end
        tokio::time::sleep(Duration::from_secs(1)).await;
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
