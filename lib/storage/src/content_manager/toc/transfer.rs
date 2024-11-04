use async_trait::async_trait;
use collection::operations::types::{CollectionError, CollectionResult};
use collection::shards::replica_set::ReplicaState;
use collection::shards::resharding::ReshardKey;
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::transfer::{ShardTransfer, ShardTransferConsensus, ShardTransferKey};
use collection::shards::CollectionId;

use super::dispatcher::TocDispatcher;
use crate::content_manager::collection_meta_ops::{
    CollectionMetaOperations, ReshardingOperation, ShardTransferOperations,
};
use crate::content_manager::consensus_ops::ConsensusOperations;

#[async_trait]
impl ShardTransferConsensus for TocDispatcher {
    fn this_peer_id(&self) -> PeerId {
        self.consensus_state.this_peer_id()
    }

    fn peers(&self) -> Vec<PeerId> {
        self.consensus_state.peers()
    }

    fn consensus_commit_term(&self) -> (u64, u64) {
        self.consensus_state
            .0
            .persistent
            .read()
            .applied_commit_term()
    }

    fn recovered_switch_to_partial(
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
        collection_id: CollectionId,
    ) -> CollectionResult<()> {
        let operation =
            ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::TransferShard(
                collection_id,
                ShardTransferOperations::Start(transfer_config),
            )));
        self
            .consensus_state
            .propose_consensus_op_with_await(operation, None)
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
            .propose_consensus_op_with_await(operation, None)
            .await
            .map(|_| ())
            .map_err(|err| {
                CollectionError::service_error(format!("Failed to propose and confirm shard transfer restart operation through consensus: {err}"))
            })
    }

    async fn abort_shard_transfer(
        &self,
        transfer: ShardTransferKey,
        collection_id: CollectionId,
        reason: &str,
    ) -> CollectionResult<()> {
        let operation =
            ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::TransferShard(
                collection_id,
                ShardTransferOperations::Abort {
                    transfer,
                    reason: reason.into(),
                },
            )));
        self
            .consensus_state
            .propose_consensus_op_with_await(operation, None)
            .await
            .map(|_| ())
            .map_err(|err| {
                CollectionError::service_error(format!("Failed to propose and confirm shard transfer abort operation through consensus: {err}"))
            })
    }

    async fn set_shard_replica_set_state(
        &self,
        collection_id: CollectionId,
        shard_id: ShardId,
        state: ReplicaState,
        from_state: Option<ReplicaState>,
    ) -> CollectionResult<()> {
        let operation = ConsensusOperations::set_replica_state(
            collection_id,
            shard_id,
            self.this_peer_id(),
            state,
            from_state,
        );
        self
            .consensus_state
            .propose_consensus_op_with_await(operation.clone(), None)
            .await
            .map(|_| ())
            .map_err(|err| {
                CollectionError::service_error(format!("Failed to propose and confirm set replica set state operation through consensus: {err}"))
            })
    }

    async fn commit_read_hashring(
        &self,
        collection_id: CollectionId,
        reshard_key: ReshardKey,
    ) -> CollectionResult<()> {
        let operation =
            ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::Resharding(
                collection_id,
                ReshardingOperation::CommitRead(reshard_key),
            )));
        self
            .consensus_state
            .propose_consensus_op_with_await(operation, None)
            .await
            .map(|_| ())
            .map_err(|err| {
                CollectionError::service_error(format!("Failed to propose and confirm commit read hashring operation through consensus: {err}"))
            })
    }

    async fn commit_write_hashring(
        &self,
        collection_id: CollectionId,
        reshard_key: ReshardKey,
    ) -> CollectionResult<()> {
        let operation =
            ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::Resharding(
                collection_id,
                ReshardingOperation::CommitWrite(reshard_key),
            )));
        self
            .consensus_state
            .propose_consensus_op_with_await(operation, None)
            .await
            .map(|_| ())
            .map_err(|err| {
                CollectionError::service_error(format!("Failed to propose and confirm commit write hasrhing operation through consensus: {err}"))
            })
    }
}
