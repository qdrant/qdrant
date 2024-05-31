use std::sync::Weak;
use std::time::Duration;

use async_trait::async_trait;
use collection::operations::types::{CollectionError, CollectionResult};
use collection::shards::shard::PeerId;
use collection::shards::transfer::{ShardTransfer, ShardTransferConsensus};
use collection::shards::CollectionId;

use super::TableOfContent;
use crate::content_manager::collection_meta_ops::{
    CollectionMetaOperations, ShardTransferOperations,
};
use crate::content_manager::consensus_manager::ConsensusStateRef;
use crate::content_manager::consensus_ops::ConsensusOperations;

#[derive(Clone)]
pub struct ShardTransferDispatcher {
    /// Reference to table of contents
    ///
    /// This dispatcher is stored inside the table of contents after construction. It therefore
    /// uses a weak reference to avoid a reference cycle which would prevent dropping the table of
    /// contents on exit.
    toc: Weak<TableOfContent>,
    consensus_state: ConsensusStateRef,
}

impl ShardTransferDispatcher {
    pub fn new(toc: Weak<TableOfContent>, consensus_state: ConsensusStateRef) -> Self {
        Self {
            toc,
            consensus_state,
        }
    }
}

#[async_trait]
impl ShardTransferConsensus for ShardTransferDispatcher {
    fn this_peer_id(&self) -> CollectionResult<PeerId> {
        let Some(toc) = self.toc.upgrade() else {
            return Err(CollectionError::service_error(
                "Failed to get this peer ID, table of contents is dropped",
            ));
        };
        Ok(toc.this_peer_id())
    }

    fn consensus_commit_term(&self) -> (u64, u64) {
        let state = self.consensus_state.hard_state();
        (state.commit, state.term)
    }

    fn snapshot_recovered_switch_to_partial(
        &self,
        transfer_config: &ShardTransfer,
        collection_id: CollectionId,
    ) -> CollectionResult<()> {
        let Some(toc) = self.toc.upgrade() else {
            return Err(CollectionError::service_error(
                "Can't set shard state, table of contents is dropped",
            ));
        };
        let Some(proposal_sender) = toc.consensus_proposal_sender.as_ref() else {
            return Err(CollectionError::service_error(
                "Can't set shard state, this is a single node deployment",
            ));
        };

        // Propose operation to progress transfer, setting shard state to partial
        let operation =
            ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::TransferShard(
                collection_id,
                ShardTransferOperations::RecoveryToPartial(transfer_config.key()),
            )));
        proposal_sender.send(operation).map_err(|err| {
            CollectionError::service_error(format!("Failed to submit consensus proposal: {err}"))
        })?;

        Ok(())
    }

    async fn start_shard_transfer(
        &self,
        transfer_config: ShardTransfer,
        collection_name: CollectionId,
    ) -> CollectionResult<()> {
        let operation =
            ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::TransferShard(
                collection_name,
                ShardTransferOperations::Start(transfer_config),
            )));
        self
            .consensus_state
            .propose_consensus_op_with_await(operation.clone(), None)
            .await
            .map(|_| ())
            .map_err(|err| {
                CollectionError::service_error(format!("Failed to propose and confirm shard transfer start operation through consensus: {err}"))
            })
    }

    async fn restart_shard_transfer(
        &self,
        transfer_config: ShardTransfer,
        collection_id: CollectionId,
    ) -> CollectionResult<()> {
        let operation =
            ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::TransferShard(
                collection_id,
                ShardTransferOperations::Restart(transfer_config.into()),
            )));
        self
            .consensus_state
            .propose_consensus_op_with_await(operation.clone(), None)
            .await
            .map(|_| ())
            .map_err(|err| {
                CollectionError::service_error(format!("Failed to propose and confirm shard transfer restart operation through consensus: {err}"))
            })
    }

    // TODO: rework internals, listen on finish/abort channel instead
    async fn await_shard_transfer_end(
        &self,
        transfer: ShardTransfer,
        collection_id: CollectionId,
        timeout: Option<Duration>,
    ) -> CollectionResult<Result<(), ()>> {
        let success = ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::TransferShard(
                        collection_id.clone(),
                        ShardTransferOperations::Finish(transfer.clone()),
            )));
        let failure =
            // TODO: this does not work because reason can be anything
            ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::TransferShard(
                        collection_id,
                        ShardTransferOperations::Abort {
                            transfer: transfer.key(),
                            reason: String::new(),
                        },
            )));

        let result = self.consensus_state.await_for_any_operation(
            vec![
                success.clone(),
                failure.clone(),
            ],
            timeout,
        ).await;

        match result {
            // We saw the success operation
            Ok(Ok(operation)) if operation == success => Ok(Ok(())),
            // We saw the failure operation
            Ok(Ok(_operation)) => Ok(Err(())),
            // Storage error
            Ok(Err(err)) => Err(CollectionError::service_error(format!("Failed to await for shard transfer end: {err}"))),
            // Timeout
            Err(err) => Err(CollectionError::service_error(format!("Awaiting for shard transfer end timed out: {err}"))),
        }
    }
}
