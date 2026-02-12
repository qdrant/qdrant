use std::time::Duration;

use async_trait::async_trait;
use common::defaults::{self, CONSENSUS_CONFIRM_RETRIES};
use schemars::JsonSchema;
use segment::types::Filter;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use super::CollectionId;
use super::channel_service::ChannelService;
use super::remote_shard::RemoteShard;
use super::resharding::ReshardKey;
use super::shard::{PeerId, ShardId};
use crate::operations::cluster_ops::ReshardingDirection;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::replica_set::replica_set_state::ReplicaState;

pub mod driver;
pub mod helpers;
pub mod resharding_stream_records;
pub mod snapshot;
pub mod stream_records;
pub mod transfer_tasks_pool;
pub mod wal_delta;

/// Current stage of a shard transfer operation.
///
/// Stages are intentionally coarse-grained for clarity.
/// The transfer method (snapshot, stream_records, etc.) provides additional context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferStage {
    /// Setting up queue/forward proxy on source shard
    Proxifying,
    /// Waiting for pending operations in the update queue to be processed
    Plunging,
    /// Creating snapshot of the shard (snapshot method only)
    CreatingSnapshot,
    /// Transferring data to remote (snapshot file or record batches)
    Transferring,
    /// Remote is recovering/applying the transferred data
    Recovering,
    /// Transferring queued updates accumulated during transfer
    FlushingQueue,
    /// Waiting for consensus state synchronization
    WaitingConsensus,
    /// Finalizing transfer (un-proxifying)
    Finalizing,
}

impl TransferStage {
    /// Short lowercase name for display in comment
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Proxifying => "proxifying",
            Self::Plunging => "applying queued updates",
            Self::CreatingSnapshot => "creating snapshot",
            Self::Transferring => "transferring",
            Self::Recovering => "recovering",
            Self::FlushingQueue => "syncing queued updates",
            Self::WaitingConsensus => "waiting consensus",
            Self::Finalizing => "finalizing",
        }
    }
}

/// Current stage of snapshot recovery on the receiver (destination) node.
///
/// These sub-stages break down what happens during the `Recovering` stage
/// as seen from the destination peer's perspective.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStage {
    /// HTTP download of snapshot from source node
    Downloading,
    /// Extracting tar archive
    Unpacking,
    /// Applying data to shard
    Restoring,
}

impl RecoveryStage {
    /// Short lowercase name for display in comment
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Downloading => "downloading",
            Self::Unpacking => "unpacking",
            Self::Restoring => "restoring",
        }
    }
}

/// Time between consensus confirmation retries.
const CONSENSUS_CONFIRM_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Time after which confirming a consensus operation times out.
const CONSENSUS_CONFIRM_TIMEOUT: Duration = defaults::CONSENSUS_META_OP_WAIT;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardTransfer {
    pub shard_id: ShardId,
    /// Target shard ID if different than source shard ID
    ///
    /// Used exclusively with `ReshardingStreamRecords` transfer method.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_shard_id: Option<ShardId>,
    pub from: PeerId,
    pub to: PeerId,
    /// If this flag is true, this is a replication related transfer of shard from 1 peer to another
    /// Shard on original peer will not be deleted in this case
    pub sync: bool,
    /// Method to transfer shard with. `None` to choose automatically.
    #[serde(default)]
    pub method: Option<ShardTransferMethod>,

    // Optional filter to apply when transferring points
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
}

impl ShardTransfer {
    pub fn key(&self) -> ShardTransferKey {
        ShardTransferKey {
            shard_id: self.shard_id,
            to_shard_id: self.to_shard_id,
            from: self.from,
            to: self.to,
        }
    }

    pub fn is_resharding(&self) -> bool {
        self.method.is_some_and(|method| method.is_resharding())
    }

    /// Checks whether this peer and shard ID pair is the source or target of this transfer
    #[inline]
    pub fn is_source_or_target(&self, peer_id: PeerId, shard_id: ShardId) -> bool {
        self.is_source(peer_id, shard_id) || self.is_target(peer_id, shard_id)
    }

    /// Checks whether this peer and shard ID pair is the source of this transfer
    #[inline]
    pub fn is_source(&self, peer_id: PeerId, shard_id: ShardId) -> bool {
        self.from == peer_id && self.shard_id == shard_id
    }

    /// Checks whether this peer and shard ID pair is the target of this transfer
    #[inline]
    pub fn is_target(&self, peer_id: PeerId, shard_id: ShardId) -> bool {
        self.to == peer_id && self.to_shard_id.unwrap_or(self.shard_id) == shard_id
    }

    /// Check if this transfer is related to a specific resharding operation
    pub fn is_related_to_resharding(&self, key: &ReshardKey) -> bool {
        // Must be a resharding transfer
        if !self.method.is_some_and(|method| method.is_resharding()) {
            return false;
        }

        match key.direction {
            // Resharding up: all related transfers target the resharding shard ID
            ReshardingDirection::Up => self
                .to_shard_id
                .is_some_and(|to_shard_id| key.shard_id == to_shard_id),
            // Resharding down: all related transfers are sourced from the resharding shard ID
            ReshardingDirection::Down => self.shard_id == key.shard_id,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardTransferRestart {
    pub shard_id: ShardId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_shard_id: Option<ShardId>,
    pub from: PeerId,
    pub to: PeerId,
    pub method: ShardTransferMethod,
}

impl ShardTransferRestart {
    pub fn key(&self) -> ShardTransferKey {
        ShardTransferKey {
            shard_id: self.shard_id,
            to_shard_id: self.to_shard_id,
            from: self.from,
            to: self.to,
        }
    }
}

impl From<ShardTransfer> for ShardTransferRestart {
    fn from(transfer: ShardTransfer) -> Self {
        Self {
            shard_id: transfer.shard_id,
            to_shard_id: transfer.to_shard_id,
            from: transfer.from,
            to: transfer.to,
            method: transfer.method.unwrap_or_default(),
        }
    }
}

/// Unique identifier of a transfer, agnostic of transfer method
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardTransferKey {
    pub shard_id: ShardId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_shard_id: Option<ShardId>,
    pub from: PeerId,
    pub to: PeerId,
}

impl ShardTransferKey {
    pub fn check(self, transfer: &ShardTransfer) -> bool {
        self == transfer.key()
    }
}

/// Methods for transferring a shard from one node to another.
///
/// - `stream_records` - Stream all shard records in batches until the whole shard is transferred.
///
/// - `snapshot` - Snapshot the shard, transfer and restore it on the receiver.
///
/// - `wal_delta` - Attempt to transfer shard difference by WAL delta.
///
/// - `resharding_stream_records` - Shard transfer for resharding: stream all records in batches until all points are transferred.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ShardTransferMethod {
    // Stream all shard records in batches until the whole shard is transferred.
    #[default]
    StreamRecords,
    // Snapshot the shard, transfer and restore it on the receiver.
    Snapshot,
    // Attempt to transfer shard difference by WAL delta.
    WalDelta,
    // Shard transfer for resharding: stream all records in batches until all points are
    // transferred.
    ReshardingStreamRecords,
}

impl ShardTransferMethod {
    pub fn is_resharding(&self) -> bool {
        matches!(self, Self::ReshardingStreamRecords)
    }
}

/// Interface to consensus for shard transfer operations.
#[async_trait]
pub trait ShardTransferConsensus: Send + Sync {
    /// Get the peer ID for the current node.
    fn this_peer_id(&self) -> PeerId;

    /// Get all peer IDs, including that of the current node.
    fn peers(&self) -> Vec<PeerId>;

    /// Get the current consensus commit and term state.
    ///
    /// Returns `(commit, term)`.
    fn consensus_commit_term(&self) -> (u64, u64);

    /// After snapshot or WAL delta recovery, propose to switch shard to `Partial`
    ///
    /// This is called after shard snapshot or WAL delta recovery has been completed on the remote.
    /// It submits a proposal to consensus to switch the shard state from `Recovery` to `Partial`.
    ///
    /// # Warning
    ///
    /// This only submits a proposal to consensus. Calling this does not guarantee that consensus
    /// will actually apply the operation across the cluster.
    fn recovered_switch_to_partial(
        &self,
        transfer_config: &ShardTransfer,
        collection_id: CollectionId,
    ) -> CollectionResult<()>;

    /// After snapshot or WAL delta recovery, propose to switch shard to `Partial` and confirm on
    /// remote shard
    ///
    /// This is called after shard snapshot or WAL delta recovery has been completed on the remote.
    /// It submits a proposal to consensus to switch the shard state from `Recovery` to `Partial`.
    ///
    /// This method also confirms consensus applied the operation before returning by asserting the
    /// change is propagated on a remote shard. For the next stage only the remote needs to be in
    /// `Partial` to accept updates, we therefore assert the state on the remote explicitly rather
    /// than asserting locally. If it fails, it will be retried for up to
    /// `CONSENSUS_CONFIRM_RETRIES` times.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn recovered_switch_to_partial_confirm_remote(
        &self,
        transfer_config: &ShardTransfer,
        collection_id: &CollectionId,
        remote_shard: &RemoteShard,
    ) -> CollectionResult<()> {
        let mut result = Err(CollectionError::service_error(
            "`recovered_switch_to_partial_confirm_remote` exit without attempting any work, \
             this is a programming error",
        ));

        for attempt in 0..CONSENSUS_CONFIRM_RETRIES {
            if attempt > 0 {
                sleep(CONSENSUS_CONFIRM_RETRY_DELAY).await;
            }

            result = self.recovered_switch_to_partial(transfer_config, collection_id.clone());

            if let Err(err) = &result {
                log::error!("Failed to propose recovered operation to consensus: {err}");
                continue;
            }

            log::trace!("Wait for remote shard to reach `Partial` state");

            result = remote_shard
                .wait_for_shard_state(
                    collection_id,
                    transfer_config.shard_id,
                    ReplicaState::Partial,
                    CONSENSUS_CONFIRM_TIMEOUT,
                )
                .await
                .map(|_| ());

            match &result {
                Ok(()) => break,
                Err(err) => {
                    log::error!("Failed to confirm recovered operation on consensus: {err}");
                }
            }
        }

        result.map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to confirm recovered operation on consensus after {CONSENSUS_CONFIRM_RETRIES} retries: {err}",
            ))
        })
    }

    /// After a stream records transfer between different shard IDs, propose to switch shard to
    /// `ActiveRead` and confirm on remote shard
    ///
    /// This is called after shard stream records has been completed on the remote.
    /// It submits a proposal to consensus to switch the shard state from `Partial` to `ActiveRead`.
    ///
    /// This method also confirms consensus applied the operation on ALL peers before returning. If
    /// it fails, it will be retried for up to `CONSENSUS_CONFIRM_RETRIES` times.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn switch_partial_to_read_active_confirm_peers(
        &self,
        channel_service: &ChannelService,
        collection_id: &CollectionId,
        remote_shard: &RemoteShard,
    ) -> CollectionResult<()> {
        let mut result = Err(CollectionError::service_error(
            "`switch_partial_to_read_active_confirm_peers` exit without attempting any work, \
             this is a programming error",
        ));

        for attempt in 0..CONSENSUS_CONFIRM_RETRIES {
            if attempt > 0 {
                sleep(CONSENSUS_CONFIRM_RETRY_DELAY).await;
            }

            log::trace!(
                "Propose and confirm to switch peer from `Partial` into `ActiveRead` state"
            );

            result = self
                .set_shard_replica_set_state(
                    Some(remote_shard.peer_id),
                    collection_id.clone(),
                    remote_shard.id,
                    ReplicaState::ActiveRead,
                    Some(ReplicaState::Partial),
                )
                .await;

            if let Err(err) = &result {
                log::error!("Failed to propose state switch operation to consensus: {err}");
                continue;
            }

            log::trace!("Wait for all peers to reach `ActiveRead` state");

            result = self.await_consensus_sync(channel_service).await;

            match &result {
                Ok(()) => break,
                Err(err) => {
                    log::error!("Failed to confirm state switch operation on consensus: {err}");
                }
            }
        }

        result.map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to confirm state switch operation on consensus after {CONSENSUS_CONFIRM_RETRIES} retries: {err}",
            ))
        })
    }

    /// Propose to start a shard transfer
    ///
    /// # Warning
    ///
    /// This only submits a proposal to consensus. Calling this does not guarantee that consensus
    /// will actually apply the operation across the cluster.
    async fn start_shard_transfer(
        &self,
        transfer_config: ShardTransfer,
        collection_id: CollectionId,
    ) -> CollectionResult<()>;

    /// Propose to start a shard transfer
    ///
    /// This internally confirms and retries a few times if needed to ensure consensus picks up the
    /// operation.
    async fn start_shard_transfer_confirm_and_retry(
        &self,
        transfer_config: &ShardTransfer,
        collection_id: &str,
    ) -> CollectionResult<()> {
        let mut result = Err(CollectionError::service_error(
            "`start_shard_transfer_confirm_and_retry` exit without attempting any work, \
             this is a programming error",
        ));

        for attempt in 0..CONSENSUS_CONFIRM_RETRIES {
            if attempt > 0 {
                sleep(CONSENSUS_CONFIRM_RETRY_DELAY).await;
            }

            log::trace!("Propose and confirm shard transfer start operation");
            result = self
                .start_shard_transfer(transfer_config.clone(), collection_id.into())
                .await;

            match &result {
                Ok(()) => break,
                Err(err) => {
                    log::error!(
                        "Failed to confirm start shard transfer operation on consensus: {err}"
                    );
                }
            }
        }

        result.map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to start shard transfer through consensus \
                 after {CONSENSUS_CONFIRM_RETRIES} retries: {err}"
            ))
        })
    }

    /// Propose to restart a shard transfer with a different given configuration
    ///
    /// # Warning
    ///
    /// This only submits a proposal to consensus. Calling this does not guarantee that consensus
    /// will actually apply the operation across the cluster.
    async fn restart_shard_transfer(
        &self,
        transfer_config: ShardTransfer,
        collection_id: CollectionId,
    ) -> CollectionResult<()>;

    /// Propose to restart a shard transfer with a different given configuration
    ///
    /// This internally confirms and retries a few times if needed to ensure consensus picks up the
    /// operation.
    async fn restart_shard_transfer_confirm_and_retry(
        &self,
        transfer_config: &ShardTransfer,
        collection_id: &CollectionId,
    ) -> CollectionResult<()> {
        let mut result = Err(CollectionError::service_error(
            "`restart_shard_transfer_confirm_and_retry` exit without attempting any work, \
             this is a programming error",
        ));

        for attempt in 0..CONSENSUS_CONFIRM_RETRIES {
            if attempt > 0 {
                sleep(CONSENSUS_CONFIRM_RETRY_DELAY).await;
            }

            log::trace!("Propose and confirm shard transfer restart operation");
            result = self
                .restart_shard_transfer(transfer_config.clone(), collection_id.into())
                .await;

            match &result {
                Ok(()) => break,
                Err(err) => {
                    log::error!(
                        "Failed to confirm restart shard transfer operation on consensus: {err}"
                    );
                }
            }
        }

        result.map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to restart shard transfer through consensus \
                 after {CONSENSUS_CONFIRM_RETRIES} retries: {err}"
            ))
        })
    }

    /// Propose to abort a shard transfer
    ///
    /// # Warning
    ///
    /// This only submits a proposal to consensus. Calling this does not guarantee that consensus
    /// will actually apply the operation across the cluster.
    async fn abort_shard_transfer(
        &self,
        transfer: ShardTransferKey,
        collection_id: CollectionId,
        reason: &str,
    ) -> CollectionResult<()>;

    /// Propose to abort a shard transfer
    ///
    /// This internally confirms and retries a few times if needed to ensure consensus picks up the
    /// operation.
    async fn abort_shard_transfer_confirm_and_retry(
        &self,
        transfer: ShardTransferKey,
        collection_id: &CollectionId,
        reason: &str,
    ) -> CollectionResult<()> {
        let mut result = Err(CollectionError::service_error(
            "`abort_shard_transfer_confirm_and_retry` exit without attempting any work, \
             this is a programming error",
        ));

        for attempt in 0..CONSENSUS_CONFIRM_RETRIES {
            if attempt > 0 {
                sleep(CONSENSUS_CONFIRM_RETRY_DELAY).await;
            }

            log::trace!("Propose and confirm shard transfer abort operation");
            result = self
                .abort_shard_transfer(transfer, collection_id.into(), reason)
                .await;

            match &result {
                Ok(()) => break,
                Err(err) => {
                    log::error!(
                        "Failed to confirm abort shard transfer operation on consensus: {err}"
                    );
                }
            }
        }

        result.map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to abort shard transfer through consensus \
                 after {CONSENSUS_CONFIRM_RETRIES} retries: {err}"
            ))
        })
    }

    /// Set the shard replica state on this peer through consensus
    ///
    /// If the peer ID is not provided, this will set the replica state for the current peer.
    ///
    /// # Warning
    ///
    /// This only submits a proposal to consensus. Calling this does not guarantee that consensus
    /// will actually apply the operation across the cluster.
    async fn set_shard_replica_set_state(
        &self,
        peer_id: Option<PeerId>,
        collection_id: CollectionId,
        shard_id: ShardId,
        state: ReplicaState,
        from_state: Option<ReplicaState>,
    ) -> CollectionResult<()>;

    /// Propose to commit the read hash ring.
    ///
    /// # Warning
    ///
    /// This only submits a proposal to consensus. Calling this does not guarantee that consensus
    /// will actually apply the operation across the cluster.
    async fn commit_read_hashring(
        &self,
        collection_id: CollectionId,
        reshard_key: ReshardKey,
    ) -> CollectionResult<()>;

    /// Propose to commit the read hash ring, then confirm or retry.
    ///
    /// This internally confirms and retries a few times if needed to ensure consensus picks up the
    /// operation.
    async fn commit_read_hashring_confirm_and_retry(
        &self,
        collection_id: &CollectionId,
        reshard_key: &ReshardKey,
    ) -> CollectionResult<()> {
        let mut result = Err(CollectionError::service_error(
            "`commit_read_hashring_confirm_and_retry` exit without attempting any work, \
             this is a programming error",
        ));

        for attempt in 0..CONSENSUS_CONFIRM_RETRIES {
            if attempt > 0 {
                sleep(CONSENSUS_CONFIRM_RETRY_DELAY).await;
            }

            log::trace!("Propose and confirm commit read hashring operation");
            result = self
                .commit_read_hashring(collection_id.into(), reshard_key.clone())
                .await;

            match &result {
                Ok(()) => break,
                Err(err) => {
                    log::error!(
                        "Failed to confirm commit read hashring operation on consensus: {err}"
                    );
                }
            }
        }

        result.map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to commit read hashring through consensus \
                 after {CONSENSUS_CONFIRM_RETRIES} retries: {err}"
            ))
        })
    }

    /// Propose to commit the write hash ring.
    ///
    /// # Warning
    ///
    /// This only submits a proposal to consensus. Calling this does not guarantee that consensus
    /// will actually apply the operation across the cluster.
    async fn commit_write_hashring(
        &self,
        collection_id: CollectionId,
        reshard_key: ReshardKey,
    ) -> CollectionResult<()>;

    /// Propose to commit the write hash ring, then confirm or retry.
    ///
    /// This internally confirms and retries a few times if needed to ensure consensus picks up the
    /// operation.
    async fn commit_write_hashring_confirm_and_retry(
        &self,
        collection_id: &CollectionId,
        reshard_key: &ReshardKey,
    ) -> CollectionResult<()> {
        let mut result = Err(CollectionError::service_error(
            "`commit_write_hashring_confirm_and_retry` exit without attempting any work, \
             this is a programming error",
        ));

        for attempt in 0..CONSENSUS_CONFIRM_RETRIES {
            if attempt > 0 {
                sleep(CONSENSUS_CONFIRM_RETRY_DELAY).await;
            }

            log::trace!("Propose and confirm commit write hashring operation");
            result = self
                .commit_write_hashring(collection_id.into(), reshard_key.clone())
                .await;

            match &result {
                Ok(()) => break,
                Err(err) => {
                    log::error!(
                        "Failed to confirm commit write hashring operation on consensus: {err}"
                    );
                }
            }
        }

        result.map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to commit write hashring through consensus \
                 after {CONSENSUS_CONFIRM_RETRIES} retries: {err}"
            ))
        })
    }

    /// Wait for all other peers to reach the current consensus
    ///
    /// This will take the current consensus state of this node. It then explicitly awaits on all
    /// other nodes to reach this consensus state.
    ///
    /// # Errors
    ///
    /// This errors if:
    /// - any of the peers is not on the same term
    /// - waiting takes longer than the specified timeout
    /// - any of the peers cannot be reached
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn await_consensus_sync(&self, channel_service: &ChannelService) -> CollectionResult<()> {
        let other_peer_count = channel_service.id_to_address.read().len().saturating_sub(1);
        if other_peer_count == 0 {
            log::warn!("There are no other peers, skipped synchronizing consensus");
            return Ok(());
        }

        let (commit, term) = self.consensus_commit_term();
        log::trace!(
            "Waiting on {other_peer_count} peer(s) to reach consensus (commit: {commit}, term: {term}) before finalizing shard transfer"
        );
        channel_service
            .await_commit_on_all_peers(
                self.this_peer_id(),
                commit,
                term,
                defaults::CONSENSUS_META_OP_WAIT,
            )
            .await
    }
}
