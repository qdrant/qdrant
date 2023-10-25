use std::sync::Weak;

use collection::shards::transfer::ShardTransferConsensus;

use super::TableOfContent;
use crate::content_manager::consensus_manager::ConsensusStateRef;

#[derive(Clone)]
pub struct ShardTransferDispatcher {
    /// Reference to table of contents
    ///
    /// This dispatcher is stored inside the table of contents after construction. It therefore
    /// uses a weak reference to avoid a reference cycle which would prevent dropping the table of
    /// contents on exit.
    _toc: Weak<TableOfContent>,
    consensus_state: ConsensusStateRef,
}

impl ShardTransferDispatcher {
    pub fn new(toc: Weak<TableOfContent>, consensus_state: ConsensusStateRef) -> Self {
        Self {
            _toc: toc,
            consensus_state,
        }
    }
}

impl ShardTransferConsensus for ShardTransferDispatcher {
    fn consensus_commit_term(&self) -> (u64, u64) {
        let state = self.consensus_state.hard_state();
        (state.commit, state.term)
    }
}
