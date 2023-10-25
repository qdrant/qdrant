use crate::operations::types::CollectionResult;

pub mod shard_transfer;
pub mod transfer_tasks_pool;

/// Interface to consensus for shard transfer operations.
pub trait ShardTransferConsensus: Send + Sync {
    /// Get the current consensus commit and term state.
    ///
    /// Returns `(commit, term)`.
    fn consensus_commit_term(&self) -> CollectionResult<(u64, u64)>;
}
