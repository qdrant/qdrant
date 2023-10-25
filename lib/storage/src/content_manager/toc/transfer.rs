use collection::operations::types::{CollectionError, CollectionResult};
use collection::shards::transfer::ShardTransferConsensus;

use crate::dispatcher::Dispatcher;

impl ShardTransferConsensus for Dispatcher {
    fn consensus_commit_term(&self) -> CollectionResult<(u64, u64)> {
        self.consensus_state()
            .map(|consensus| {
                let state = consensus.hard_state();
                (state.commit, state.term)
            })
            .ok_or_else(|| CollectionError::service_error("Consensus is not available"))
    }
}
