use std::time::Duration;

use async_trait::async_trait;
use common::defaults::{self, CONSENSUS_CONFIRM_RETRIES};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use super::channel_service::ChannelService;
use super::remote_shard::RemoteShard;
use super::replica_set::ReplicaState;
use super::resharding::ReshardKey;
use super::shard::{PeerId, ShardId};
use super::CollectionId;
use crate::operations::types::{CollectionError, CollectionResult};

pub mod driver;
pub mod helpers;
pub mod resharding_stream_records;
pub mod snapshot;
pub mod stream_records;
pub mod transfer_tasks_pool;
pub mod wal_delta;

/// Time between consensus confirmation retries.
const CONSENSUS_CONFIRM_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Time after which confirming a consensus operation times out.
const CONSENSUS_CONFIRM_TIMEOUT: Duration = defaults::CONSENSUS_META_OP_WAIT;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ShardTransfer {
    pub shard_id: ShardId,
    /// For resharding, a different target shard ID may be configured
    /// By default the shard ID on the target peer is the same.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(skip)] // TODO(resharding): expose once we release resharding
    pub to_shard_id: Option<ShardId>,
    pub from: PeerId,
    pub to: PeerId,
    /// If this flag is true, this is a replication related transfer of shard from 1 peer to another
    /// Shard on original peer will not be deleted in this case
    pub sync: bool,
    /// Method to transfer shard with. `None` to choose automatically.
    #[serde(default)]
    pub method: Option<ShardTransferMethod>,
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
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ShardTransferRestart {
    pub shard_id: ShardId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(skip)] // TODO(resharding): expose once we release resharding
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
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ShardTransferKey {
    pub shard_id: ShardId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(skip)] // TODO(resharding): expose once we release resharding
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
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ShardTransferMethod {
    /// Stream all shard records in batches until the whole shard is transferred.
    #[default]
    StreamRecords,
    /// Snapshot the shard, transfer and restore it on the receiver.
    Snapshot,
    /// Attempt to transfer shard difference by WAL delta.
    WalDelta,
    /// Shard transfer for resharding: stream all records in batches until all points are
    /// transferred.
    #[schemars(skip)]
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

            result = self.recovered_switch_to_partial(transfer_config, collection_id.to_string());

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
                    continue;
                }
            }
        }

        result.map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to confirm recovered operation on consensus after {CONSENSUS_CONFIRM_RETRIES} retries: {err}",
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
                    continue;
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
                    continue;
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
                    continue;
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
    /// # Warning
    ///
    /// This only submits a proposal to consensus. Calling this does not guarantee that consensus
    /// will actually apply the operation across the cluster.
    async fn set_shard_replica_set_state(
        &self,
        collection_id: CollectionId,
        shard_id: ShardId,
        state: ReplicaState,
        from_state: Option<ReplicaState>,
    ) -> CollectionResult<()>;

    /// Propose to set the shard replica state on this peer through consensus
    ///
    /// This internally confirms and retries a few times if needed to ensure consensus picks up the
    /// operation.
    async fn set_shard_replica_set_state_confirm_and_retry(
        &self,
        collection_id: &CollectionId,
        shard_id: ShardId,
        state: ReplicaState,
        from_state: Option<ReplicaState>,
    ) -> CollectionResult<()> {
        let mut result = Err(CollectionError::service_error(
            "`set_shard_replica_set_state_confirm_and_retry` exit without attempting any work, \
             this is a programming error",
        ));

        for attempt in 0..CONSENSUS_CONFIRM_RETRIES {
            if attempt > 0 {
                sleep(CONSENSUS_CONFIRM_RETRY_DELAY).await;
            }

            log::trace!("Propose and confirm set shard replica set state");
            result = self
                .set_shard_replica_set_state(collection_id.clone(), shard_id, state, from_state)
                .await;

            match &result {
                Ok(()) => break,
                Err(err) => {
                    log::error!(
                        "Failed to confirm set shard replica set state operation on consensus: {err}"
                    );
                    continue;
                }
            }
        }

        result.map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to set shard replica set state through consensus \
                 after {CONSENSUS_CONFIRM_RETRIES} retries: {err}"
            ))
        })
    }

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
                    continue;
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
                    continue;
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
