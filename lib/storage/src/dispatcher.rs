use std::num::NonZeroU32;
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
                CollectionMetaOperations::CreateCollection(mut op) => {
                    self.toc.check_write_lock()?;
                    debug_assert!(
                        op.take_distribution().is_none(),
                        "Distribution should be only set in this method."
                    );
                    let number_of_peers = state.0.peer_count();
                    let shard_distribution = self
                        .toc
                        .suggest_shard_distribution(
                            &op,
                            NonZeroU32::new(number_of_peers as u32)
                                .expect("Peer count should be always >= 1"),
                        )
                        .await;
                    op.set_distribution(shard_distribution);
                    CollectionMetaOperations::CreateCollection(op)
                }
                CollectionMetaOperations::UpdateCollection(mut op) => {
                    if let Some(repl_factor) = op
                        .update_collection
                        .params
                        .as_ref()
                        .and_then(|params| params.replication_factor)
                    {
                        let changes = self
                            .toc
                            .suggest_shard_replica_changes(&op.collection_name, repl_factor)
                            .await?;
                        op.set_shard_replica_changes(changes.into_iter().collect());
                    }
                    CollectionMetaOperations::UpdateCollection(op)
                }
                op => op,
            };
            state
                .propose_consensus_op_with_await(
                    ConsensusOperations::CollectionMeta(Box::new(op)),
                    wait_timeout,
                    true,
                )
                .await
        } else {
            if let CollectionMetaOperations::CreateCollection(_) = &operation {
                self.toc.check_write_lock()?;
            }
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
