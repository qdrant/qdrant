//! In-memory consensus state machine.
//!
//! [`ClusterState`] holds everything consensus decides on. [`ConsensusStateMachine::apply`] reads
//! it and returns the [`Action`]s an operation applies, in order. Each action is one call to a
//! state change method on `TableOfContent`, `Collection` or `ShardHolder`, which checks only that
//! the object it touches exists and then persists the change. Every decision is made here.
//!
//! [`ClusterState::apply_action`] is the only way to change the state, and it cannot fail. So a
//! rejected operation leaves the state untouched, and applying the first N actions of an operation
//! gives the state that a crash after N writes leaves behind.
//!
//! Two rules every operation follows:
//!
//! 1. Never reject an operation that is partially or fully applied.
//! 2. Emit only the actions left to reach the goal state, each idempotent, and the action that
//!    records the operation as applied last.
//!
//! Rule 2 is measured against [`ClusterState`]. An action whose applier also does work outside it,
//! such as propagating a payload index to local shards, is emitted even when the state already
//! matches.

pub mod action;
pub mod state;

#[cfg(test)]
mod tests;

use collection::shards::shard::PeerId;
use collection::shards::transfer::ShardTransferMethod;
use segment::data_types::collection_defaults::CollectionConfigDefaults;

pub use self::action::Action;
pub use self::state::ClusterState;
use super::errors::StorageResult;
use crate::content_manager::collection_meta_ops::CollectionMetaOperations;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::errors::StorageError;

#[derive(Clone, Debug)]
pub struct ConsensusStateMachine {
    state: ClusterState,
    context: NodeContext,
}

impl ConsensusStateMachine {
    pub fn new(state: ClusterState, context: NodeContext) -> Self {
        Self { state, context }
    }

    pub fn state(&self) -> &ClusterState {
        &self.state
    }

    pub fn context(&self) -> &NodeContext {
        &self.context
    }

    /// Plan `operation` and advance the state by the actions it returns
    pub fn apply(&mut self, operation: &ConsensusOperations) -> ApplyOutcome {
        let outcome = self.plan(operation);

        if let ApplyOutcome::Accepted(actions) = &outcome {
            for action in actions {
                self.state.apply_action(action);
            }
        }

        outcome
    }

    /// Plan `operation` and return actions to apply or an error
    fn plan(&self, operation: &ConsensusOperations) -> ApplyOutcome {
        match operation {
            ConsensusOperations::CollectionMeta(operation) => self.plan_collection_meta(operation),

            ConsensusOperations::UpdatePeerMetadata { peer_id, metadata } => {
                let actions = self.state.plan_update_peer_metadata(*peer_id, metadata);
                ApplyOutcome::Accepted(actions)
            }

            ConsensusOperations::UpdateClusterMetadata { key, value } => {
                let actions = self.state.plan_update_cluster_metadata(key, value);
                ApplyOutcome::Accepted(actions)
            }

            ConsensusOperations::SetQuotaConfig(config) => {
                ApplyOutcome::Accepted(self.state.plan_set_quota_config(config))
            }

            ConsensusOperations::AddPeer { .. } | ConsensusOperations::RemovePeer(_) => {
                ApplyOutcome::NotCovered
            }

            // Never reach the apply path: consensus handles them in its own thread
            ConsensusOperations::RequestSnapshot | ConsensusOperations::ReportSnapshot { .. } => {
                ApplyOutcome::NotCovered
            }
        }
    }

    fn plan_collection_meta(&self, operation: &CollectionMetaOperations) -> ApplyOutcome {
        match operation {
            CollectionMetaOperations::Nop { .. } => ApplyOutcome::Accepted(Vec::new()),

            CollectionMetaOperations::CreateCollection(_)
            | CollectionMetaOperations::UpdateCollection(_)
            | CollectionMetaOperations::DeleteCollection(_)
            | CollectionMetaOperations::CreateShardKey(_)
            | CollectionMetaOperations::DropShardKey(_)
            | CollectionMetaOperations::SetShardReplicaState(_)
            | CollectionMetaOperations::TransferShard(_, _)
            | CollectionMetaOperations::Resharding(_, _) => ApplyOutcome::NotCovered,

            CollectionMetaOperations::ChangeAliases(operation) => {
                ApplyOutcome::new(self.state.plan_change_aliases(operation))
            }

            CollectionMetaOperations::CreateNamedVector(operation) => {
                ApplyOutcome::new(self.state.plan_create_named_vector(operation))
            }
            CollectionMetaOperations::DeleteNamedVector(operation) => {
                ApplyOutcome::new(self.state.plan_delete_named_vector(operation))
            }

            CollectionMetaOperations::CreatePayloadIndex(operation) => {
                ApplyOutcome::new(self.state.plan_create_payload_index(operation))
            }
            CollectionMetaOperations::DropPayloadIndex(operation) => {
                ApplyOutcome::new(self.state.plan_drop_payload_index(operation))
            }

            // Sleeps on a peer, or fails at random. The roll is not deterministic, so it
            // belongs to the applier, not to planning.
            #[cfg(feature = "staging")]
            CollectionMetaOperations::TestSlowDown(operation) => {
                ApplyOutcome::Accepted(vec![Action::TestSlowDown(operation.clone())])
            }
            #[cfg(feature = "staging")]
            CollectionMetaOperations::TestTransientError(operation) => {
                ApplyOutcome::Accepted(vec![Action::TestTransientError(operation.clone())])
            }
        }
    }
}

/// Node-local values operations read.
///
/// These come from this node's config, not from consensus, so two peers can read different values
/// for the same operation.
#[derive(Clone, Debug)]
pub struct NodeContext {
    pub peer_id: PeerId,
    pub is_distributed: bool,
    /// Collection defaults from this node's storage config
    pub collection_defaults: Option<CollectionConfigDefaults>,
    /// Transfer method this node picks when an operation does not name one
    pub default_shard_transfer_method: Option<ShardTransferMethod>,
}

/// What the machine decided about an operation.
#[derive(Clone, Debug)]
pub enum ApplyOutcome {
    /// Apply these actions, in order
    Accepted(Vec<Action>),

    /// Reject the operation with a user error.
    /// Consensus counts the entry as applied and moves on.
    Rejected(StorageError),

    /// Operation is not yet implemented.
    /// Caller must rebuild the state from `TableOfContent`.
    NotCovered,
}

impl ApplyOutcome {
    pub fn new(result: StorageResult<Vec<Action>>) -> Self {
        match result {
            Ok(actions) => ApplyOutcome::Accepted(actions),
            Err(err) => ApplyOutcome::Rejected(err),
        }
    }
}
