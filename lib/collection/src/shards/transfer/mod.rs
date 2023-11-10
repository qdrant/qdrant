use std::time::Duration;

use async_trait::async_trait;
use common::defaults;
use tokio::time::sleep;

use self::shard_transfer::ShardTransfer;
use super::channel_service::ChannelService;
use super::remote_shard::RemoteShard;
use super::replica_set::ReplicaState;
use super::shard::PeerId;
use super::CollectionId;
use crate::operations::types::{CollectionError, CollectionResult};

pub mod shard_transfer;
pub mod transfer_tasks_pool;

/// Number of retries for confirming a consensus operation.
const CONSENSUS_CONFIRM_RETRIES: usize = 3;

/// Time between consensus confirmation retries.
const CONSENSUS_CONFIRM_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Time after which confirming a consensus operation times out.
const CONSENSUS_CONFIRM_TIMEOUT: Duration = defaults::CONSENSUS_META_OP_WAIT;

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
    /// proposal to consensus to switch the the shard state from `PartialSnapshot` to `Partial`.
    ///
    /// # Warning
    ///
    /// This only submits a proposal to consensus. Calling this does not guarantee that consensus
    /// will actually apply the operation across the cluster.
    fn snapshot_recovered_switch_to_partial(
        &self,
        transfer_config: &ShardTransfer,
        collection_name: CollectionId,
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
        collection_name: &str,
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
                .snapshot_recovered_switch_to_partial(transfer_config, collection_name.to_string());

            if let Err(err) = &result {
                log::error!("Failed to propose snapshot recovered operation to consensus: {err}");
                continue;
            }

            log::trace!("Wait for remote shard to reach `Partial` state");

            result = remote_shard
                .wait_for_shard_state(
                    collection_name,
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
            "Waiting on {other_peer_count} peer(s) to reach consensus (commit: {commit}, term: {term}) before finalizing shard snapshot transfer"
        );
        channel_service
            .await_commit_on_all_peers(this_peer_id, commit, term, defaults::CONSENSUS_META_OP_WAIT)
            .await
    }
}
