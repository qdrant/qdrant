pub mod shard_transfer;
pub mod transfer_tasks_pool;

/// Interface to consensus for shard transfer operations.
pub trait ShardTransferConsensus: Send + Sync {
    /// Get the current consensus state.
    ///
    /// Returns `(commit, term)`.
    fn consensus_state(&self) -> (usize, usize);
}
