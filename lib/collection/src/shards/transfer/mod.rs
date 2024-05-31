use std::time::Duration;

use async_trait::async_trait;
use common::defaults::{self, CONSENSUS_CONFIRM_RETRIES};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, sleep_until, timeout_at};

use super::channel_service::ChannelService;
use super::remote_shard::RemoteShard;
use super::replica_set::ReplicaState;
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
    pub from: PeerId,
    pub to: PeerId,
    /// If this flag is true, this is a replication related transfer of shard from 1 peer to another
    /// Shard on original peer will not be deleted in this case
    pub sync: bool,
    /// Method to transfer shard with. `None` to choose automatically.
    #[serde(default)]
    pub method: Option<ShardTransferMethod>,
    /// For resharding, a different target shard ID may be configured
    /// By default the shard ID on the target peer is the same.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_shard_id: Option<ShardId>,
}

impl ShardTransfer {
    pub fn key(&self) -> ShardTransferKey {
        ShardTransferKey {
            shard_id: self.shard_id,
            from: self.from,
            to: self.to,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ShardTransferRestart {
    pub shard_id: ShardId,
    pub from: PeerId,
    pub to: PeerId,
    pub method: ShardTransferMethod,
}

impl ShardTransferRestart {
    pub fn key(&self) -> ShardTransferKey {
        ShardTransferKey {
            shard_id: self.shard_id,
            from: self.from,
            to: self.to,
        }
    }
}

impl From<ShardTransfer> for ShardTransferRestart {
    fn from(transfer: ShardTransfer) -> Self {
        Self {
            shard_id: transfer.shard_id,
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
    #[doc(hidden)]
    #[schemars(skip)]
    ReshardingStreamRecords,
}

/// Interface to consensus for shard transfer operations.
#[async_trait]
pub trait ShardTransferConsensus: Send + Sync {
    /// Get the current consensus commit and term state.
    ///
    /// Returns `(commit, term)`.
    fn consensus_commit_term(&self) -> (u64, u64);

    /// After snapshot recovery, propose to switch shard to `Partial`
    ///
    /// This is called after shard snapshot recovery has been completed on the remote. It submits a
    /// proposal to consensus to switch the shard state from `PartialSnapshot` to `Partial`.
    ///
    /// # Warning
    ///
    /// This only submits a proposal to consensus. Calling this does not guarantee that consensus
    /// will actually apply the operation across the cluster.
    fn snapshot_recovered_switch_to_partial(
        &self,
        transfer_config: &ShardTransfer,
        collection_id: CollectionId,
    ) -> CollectionResult<()>;

    /// After snapshot recovery, propose to switch shard to `Partial` and confirm on remote shard
    ///
    /// This is called after shard snapshot recovery has been completed on the remote. It submits a
    /// proposal to consensus to switch the shard state from `PartialSnapshot` to `Partial`.
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
    async fn snapshot_recovered_switch_to_partial_confirm_remote(
        &self,
        transfer_config: &ShardTransfer,
        collection_id: &CollectionId,
        remote_shard: &RemoteShard,
    ) -> CollectionResult<()> {
        let mut result = Err(CollectionError::service_error(
            "`snapshot_recovered_switch_to_partial_confirm_remote` exit without attempting any work, \
             this is a programming error"
        ));

        for attempt in 0..CONSENSUS_CONFIRM_RETRIES {
            if attempt > 0 {
                sleep(CONSENSUS_CONFIRM_RETRY_DELAY).await;
            }

            result = self
                .snapshot_recovered_switch_to_partial(transfer_config, collection_id.to_string());

            if let Err(err) = &result {
                log::error!("Failed to propose snapshot recovered operation to consensus: {err}");
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
                    log::error!(
                        "Failed to confirm snapshot recovered operation on consensus: {err}"
                    );
                    continue;
                }
            }
        }

        result.map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to confirm snapshot recovered operation on consensus \
                 after {CONSENSUS_CONFIRM_RETRIES} retries: \
                 {err}"
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
                        "Failed to confirm snapshot recovered operation on consensus: {err}"
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
    async fn await_consensus_sync(
        &self,
        this_peer_id: PeerId,
        channel_service: &ChannelService,
    ) -> CollectionResult<()> {
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
            .await_commit_on_all_peers(this_peer_id, commit, term, defaults::CONSENSUS_META_OP_WAIT)
            .await
    }
}

/// Await for consensus to synchronize across all peers
///
/// This will take the current consensus state of this node. It then explicitly waits on all other
/// nodes to reach the same (or later) consensus.
///
/// If awaiting on other nodes fails for any reason, this simply continues after the consensus
/// timeout.
///
/// # Cancel safety
///
/// This function is cancel safe.
async fn await_consensus_sync(
    consensus: &dyn ShardTransferConsensus,
    channel_service: &ChannelService,
    this_peer_id: PeerId,
) {
    let wait_until = tokio::time::Instant::now() + defaults::CONSENSUS_META_OP_WAIT;
    let sync_consensus = timeout_at(
        wait_until,
        consensus.await_consensus_sync(this_peer_id, channel_service),
    )
    .await;

    match sync_consensus {
        Ok(Ok(_)) => log::trace!("All peers reached consensus"),
        // Failed to sync explicitly, waiting until timeout to assume synchronization
        Ok(Err(err)) => {
            log::warn!("All peers failed to synchronize consensus, waiting until timeout: {err}");
            sleep_until(wait_until).await;
        }
        // Reached timeout, assume consensus is synchronized
        Err(err) => {
            log::warn!(
                "All peers failed to synchronize consensus, continuing after timeout: {err}"
            );
        }
    }
}
