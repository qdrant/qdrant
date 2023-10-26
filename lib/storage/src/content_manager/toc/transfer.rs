use std::sync::Weak;

use async_trait::async_trait;
use collection::operations::types::{CollectionError, CollectionResult};
use collection::shards::replica_set::ReplicaState;
use collection::shards::transfer::shard_transfer::ShardTransfer;
use collection::shards::transfer::ShardTransferConsensus;

use super::TableOfContent;
use crate::content_manager::collection_meta_ops::{CollectionMetaOperations, SetShardReplicaState};
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
    fn consensus_commit_term(&self) -> (u64, u64) {
        let state = self.consensus_state.hard_state();
        (state.commit, state.term)
    }

    fn snapshot_recovered_switch_to_partial(
        &self,
        transfer_config: &ShardTransfer,
        collection_name: String,
    ) -> CollectionResult<()> {
        let Some(toc) = self.toc.upgrade() else {
            return Err(CollectionError::service_error(
                "Table of contents is dropped",
            ));
        };
        let Some(proposal_sender) = toc.consensus_proposal_sender.as_ref() else {
            return Err(CollectionError::service_error(
                "Can't set shard state, this is a single node deployment",
            ));
        };

        // Send operation to set shard state to partial
        let operation = ConsensusOperations::CollectionMeta(Box::new(
            CollectionMetaOperations::SetShardReplicaState(SetShardReplicaState {
                collection_name,
                shard_id: transfer_config.shard_id,
                peer_id: transfer_config.to,
                state: ReplicaState::Partial,
                from_state: Some(ReplicaState::PartialSnapshot),
            }),
        ));
        proposal_sender.send(operation).map_err(|err| {
            CollectionError::service_error(format!("Failed to submit consensus proposal: {err}"))
        })?;

        Ok(())
    }
}
