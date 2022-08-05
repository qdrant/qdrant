use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    ClusterStatus, CollectionMetaOperations, ConsensusOperations, ConsensusStateRef, StorageError,
    TableOfContent,
};

pub struct Dispatcher {
    toc: Arc<TableOfContent>,
    consensus_state: Option<ConsensusStateRef>,
}

impl Dispatcher {
    pub fn new(toc: Arc<TableOfContent>) -> Self {
        Self {
            toc,
            consensus_state: None,
        }
    }

    pub fn with_consensus(self, state_ref: ConsensusStateRef) -> Self {
        Self {
            consensus_state: Some(state_ref),
            ..self
        }
    }

    pub fn toc(&self) -> &Arc<TableOfContent> {
        &self.toc
    }

    pub fn consensus_state(&self) -> Option<&ConsensusStateRef> {
        self.consensus_state.as_ref()
    }

    /// If `wait_timeout` is not supplied - then default duration will be used.
    /// This function needs to be called from a runtime with timers enabled.
    pub async fn submit_collection_meta_op(
        &self,
        operation: CollectionMetaOperations,
        wait_timeout: Option<Duration>,
    ) -> Result<bool, StorageError> {
        // if distributed deployment is enabled
        if let Some(state) = self.consensus_state.as_ref() {
            let op = match operation {
                CollectionMetaOperations::CreateCollection(op) => {
                    let number_of_peers = state.0.peer_count();
                    let shard_distribution = self
                        .toc
                        .suggest_shard_distribution(&op, number_of_peers as u32)
                        .await;
                    CollectionMetaOperations::CreateCollectionDistributed(op, shard_distribution)
                }
                op => op,
            };
            state
                .propose_consensus_op_with_await(
                    ConsensusOperations::CollectionMeta(Box::new(op)),
                    wait_timeout,
                )
                .await
        } else {
            self.toc.perform_collection_meta_op(operation).await
        }
    }

    pub fn cluster_status(&self) -> ClusterStatus {
        match self.consensus_state.as_ref() {
            Some(state) => state.cluster_status(),
            None => ClusterStatus::Disabled,
        }
    }
}

impl Deref for Dispatcher {
    type Target = TableOfContent;

    fn deref(&self) -> &Self::Target {
        self.toc.deref()
    }
}
