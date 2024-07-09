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
use tokio::time::sleep;

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

/// If the shard transfer IO limit is reached, retry with this interval.
const SHARD_TRANSFER_IO_LIMIT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

type PersistedState = SaveOnDisk<DriverState>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DriverState {
    key: ReshardKey,
    /// Stage each peer is currently in
    peers: HashMap<PeerId, Stage>,
    /// List of shard IDs that must be migrated into the new shard
    source_shard_ids: HashSet<ShardId>,
    /// List of shard IDs successfully migrated to the new shard
    migrated_shards: Vec<ShardId>,
    /// List of shard IDs in which we successfully deleted migrated points
    deleted_shards: Vec<ShardId>,
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
    _temp_dir: &Path,
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
    let state: PersistedState = SaveOnDisk::load_or_init(&resharding_state_path, || {
        let mut shard_ids = hash_ring.unique_nodes();
        shard_ids.remove(&reshard_key.shard_id);

        DriverState::new(reshard_key.clone(), shard_ids, &consensus.peers())
    })?;
    progress.lock().description.replace(state.read().describe());

    log::debug!(
        "Resharding {collection_id}:{to_shard_id} from shards {:?}",
        state.read().source_shards().collect::<Vec<_>>(),
    );

    // Stage 1: init
    if !completed_init(&state) {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: init");
        stage_init(&state, &progress, consensus)?;
    }

    // Stage 2: init
    if !completed_migrate_points(&state) {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: migrate points");
        stage_migrate_points(
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
    if !completed_replicate(&reshard_key, &state, &shard_holder, &collection_config).await? {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: replicate");
        stage_replicate(
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
    if !completed_commit_hashring(&state) {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: commit hashring");
        stage_commit_hashring(
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
    if !completed_propagate_deletes(&state) {
        log::debug!("Resharding {collection_id}:{to_shard_id} stage: propagate deletes");
        stage_propagate_deletes(
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
    stage_finalize(&state, &progress, consensus)?;

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

/// Stage 1: init
///
/// Check whether we need to initialize the resharding process.
fn completed_init(state: &PersistedState) -> bool {
    state.read().all_peers_completed(Stage::S1_Init)
}

/// Stage 1: init
///
/// Do initialize the resharding process.
fn stage_init(
    state: &PersistedState,
    progress: &Mutex<ReshardTaskProgress>,
    consensus: &dyn ShardTransferConsensus,
) -> CollectionResult<()> {
    state.write(|data| {
        data.complete_for_all_peers(Stage::S1_Init);
        data.update(progress, consensus);
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
#[allow(clippy::too_many_arguments)]
async fn stage_migrate_points(
    reshard_key: &ReshardKey,
    state: &PersistedState,
    progress: &Mutex<ReshardTaskProgress>,
    shard_holder: Arc<LockedShardHolder>,
    consensus: &dyn ShardTransferConsensus,
    channel_service: &ChannelService,
    collection_id: &CollectionId,
    shared_storage_config: &SharedStorageConfig,
) -> CollectionResult<()> {
    let this_peer_id = consensus.this_peer_id();

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
                let incoming_limit = shared_storage_config
                    .incoming_shard_transfers_limit
                    .unwrap_or(usize::MAX);
                let outgoing_limit = shared_storage_config
                    .outgoing_shard_transfers_limit
                    .unwrap_or(usize::MAX);

                let source_peer_ids = {
                    let shard_holder = shard_holder.read().await;
                    let replica_set =
                        shard_holder.get_shard(&source_shard_id).ok_or_else(|| {
                            CollectionError::service_error(format!(
                                "Shard {source_shard_id} not found in the shard holder for resharding",
                            ))
                        })?;

                    let active_peer_ids = replica_set.active_shards().await;
                    if active_peer_ids.is_empty() {
                        return Err(CollectionError::service_error(format!(
                            "No peer with shard {source_shard_id} in active state for resharding",
                        )));
                    }

                    // Respect shard transfer limits, always allow local transfers
                    let (incoming, _) = shard_holder.count_shard_transfer_io(&this_peer_id);
                    if incoming < incoming_limit {
                        active_peer_ids
                            .into_iter()
                            .filter(|peer_id| {
                                let (_, outgoing) = shard_holder.count_shard_transfer_io(peer_id);
                                outgoing < outgoing_limit || peer_id == &this_peer_id
                            })
                            .collect()
                    } else if active_peer_ids.contains(&this_peer_id) {
                        vec![this_peer_id]
                    } else {
                        vec![]
                    }
                };

                if source_peer_ids.is_empty() {
                    log::trace!("Postponing resharding migration transfer from shard {source_shard_id} to stay below transfer limit on peers");
                    sleep(SHARD_TRANSFER_IO_LIMIT_RETRY_INTERVAL).await;
                    continue;
                }

                let source_peer_id = *source_peer_ids.choose(&mut rand::thread_rng()).unwrap();

                // Configure shard transfer object, or use none if doing a local transfer
                if source_peer_id != this_peer_id {
                    debug_assert_ne!(source_peer_id, this_peer_id);
                    debug_assert_ne!(source_shard_id, reshard_key.shard_id);
                    let transfer = ShardTransfer {
                        shard_id: source_shard_id,
                        to_shard_id: Some(reshard_key.shard_id),
                        from: source_peer_id,
                        to: this_peer_id,
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
            data.update(progress, consensus);
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
        data.update(progress, consensus);
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
    let result = transfer_resharding_stream_records(
        Arc::clone(&shard_holder),
        progress,
        source_shard_id,
        target_shard,
        collection_id,
    )
    .await;

    // Unproxify forward proxy on local shard we just transferred
    // Normally consensus takes care of this, but we don't use consensus here
    {
        let shard_holder = shard_holder.read().await;
        let replica_set = shard_holder.get_shard(&source_shard_id).ok_or_else(|| {
            CollectionError::service_error(format!(
                "Shard {source_shard_id} not found in the shard holder for resharding",
            ))
        })?;
        replica_set.un_proxify_local().await?;
    }

    result
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
#[allow(clippy::too_many_arguments)]
async fn stage_replicate(
    reshard_key: &ReshardKey,
    state: &PersistedState,
    progress: &Mutex<ReshardTaskProgress>,
    shard_holder: Arc<LockedShardHolder>,
    consensus: &dyn ShardTransferConsensus,
    collection_id: &CollectionId,
    collection_config: Arc<RwLock<CollectionConfig>>,
    shared_storage_config: &SharedStorageConfig,
) -> CollectionResult<()> {
    let this_peer_id = consensus.this_peer_id();

    while !has_enough_replicas(reshard_key, &shard_holder, &collection_config).await? {
        // Find peer candidates to replicate to
        let candidate_peers = {
            let incoming_limit = shared_storage_config
                .incoming_shard_transfers_limit
                .unwrap_or(usize::MAX);
            let outgoing_limit = shared_storage_config
                .outgoing_shard_transfers_limit
                .unwrap_or(usize::MAX);

            // Ensure we don't exceed the outgoing transfer limits
            let shard_holder = shard_holder.read().await;
            let (_, outgoing) = shard_holder.count_shard_transfer_io(&this_peer_id);
            if outgoing >= outgoing_limit {
                log::trace!("Postponing resharding replication transfer to stay below transfer limit (outgoing: {outgoing})");
                sleep(SHARD_TRANSFER_IO_LIMIT_RETRY_INTERVAL).await;
                continue;
            }

            // Select peers that don't have this replica yet
            let Some(replica_set) = shard_holder.get_shard(&reshard_key.shard_id) else {
                return Err(CollectionError::service_error(format!(
                    "Shard {} not found in the shard holder for resharding",
                    reshard_key.shard_id,
                )));
            };
            let occupied_peers = replica_set.peers().into_keys().collect();
            let all_peers = consensus.peers().into_iter().collect::<HashSet<_>>();
            let candidate_peers: Vec<_> = all_peers
                .difference(&occupied_peers)
                .map(|peer_id| (*peer_id, shard_holder.count_peer_shards(*peer_id)))
                .collect();
            if candidate_peers.is_empty() {
                log::warn!("Resharding could not match desired replication factors as all peers are occupied, continuing with lower replication factor");
                break;
            };

            // To balance, only keep candidates with lowest number of shards
            // Peers must have room for an incoming transfer
            let lowest_shard_count = *candidate_peers
                .iter()
                .map(|(_, count)| count)
                .min()
                .unwrap();
            let candidate_peers: Vec<_> = candidate_peers
                .into_iter()
                .filter(|(peer_id, shard_count)| {
                    let (incoming, _) = shard_holder.count_shard_transfer_io(peer_id);
                    lowest_shard_count == *shard_count && incoming < incoming_limit
                })
                .map(|(peer_id, _)| peer_id)
                .collect();
            if candidate_peers.is_empty() {
                log::trace!("Postponing resharding replication transfer to stay below transfer limit on peers");
                sleep(SHARD_TRANSFER_IO_LIMIT_RETRY_INTERVAL).await;
                continue;
            };

            candidate_peers
        };

        let target_peer = *candidate_peers.choose(&mut rand::thread_rng()).unwrap();
        let transfer = ShardTransfer {
            shard_id: reshard_key.shard_id,
            to_shard_id: None,
            from: this_peer_id,
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
        data.update(progress, consensus);
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
    progress: &Mutex<ReshardTaskProgress>,
    consensus: &dyn ShardTransferConsensus,
    channel_service: &ChannelService,
    collection_id: &CollectionId,
) -> CollectionResult<()> {
    // Commit read hashring
    progress
        .lock()
        .description
        .replace(format!("{} (switching read)", state.read().describe()));
    consensus
        .commit_read_hashring_confirm_and_retry(collection_id, reshard_key)
        .await?;

    // Sync cluster
    progress.lock().description.replace(format!(
        "{} (await cluster sync for read)",
        state.read().describe(),
    ));
    await_consensus_sync(consensus, channel_service).await;

    // Commit write hashring
    progress
        .lock()
        .description
        .replace(format!("{} (switching write)", state.read().describe()));
    consensus
        .commit_write_hashring_confirm_and_retry(collection_id, reshard_key)
        .await?;

    // Sync cluster
    progress.lock().description.replace(format!(
        "{} (await cluster sync for write)",
        state.read().describe(),
    ));
    await_consensus_sync(consensus, channel_service).await;

    state.write(|data| {
        data.complete_for_all_peers(Stage::S4_CommitHashring);
        data.update(progress, consensus);
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

            // Wait on all updates here, not just the last batch
            // If we don't wait on all updates it somehow results in inconsistent deletes
            replica_set
                .update_with_consistency(operation, true, WriteOrdering::Weak)
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
        data.complete_for_all_peers(Stage::S5_PropagateDeletes);
        data.update(progress, consensus);
    })?;

    Ok(())
}

/// Stage 6: finalize
///
/// Finalize the resharding operation.
fn stage_finalize(
    state: &PersistedState,
    progress: &Mutex<ReshardTaskProgress>,
    consensus: &dyn ShardTransferConsensus,
) -> CollectionResult<()> {
    state.write(|data| {
        data.complete_for_all_peers(Stage::S6_Finalize);
        data.update(progress, consensus);
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
