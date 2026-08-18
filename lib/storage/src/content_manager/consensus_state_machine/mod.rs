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

pub mod state;

use collection::shards::shard::PeerId;
use collection::shards::transfer::ShardTransferMethod;
use segment::data_types::collection_defaults::CollectionConfigDefaults;

pub use self::state::ClusterState;

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
