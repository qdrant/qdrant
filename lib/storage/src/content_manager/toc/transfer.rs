use crate::content_manager::consensus::operation_sender::OperationSender;

/// Interface to consensus for shard transfer operations.
#[derive(Clone)]
pub struct ShardTransferConsensus {
    // Will be used later for shard transfer to propose consensus updates
    _consensus_proposal_sender: Option<OperationSender>,
}

impl ShardTransferConsensus {
    pub fn new(consensus_proposal_sender: Option<OperationSender>) -> Self {
        Self {
            _consensus_proposal_sender: consensus_proposal_sender,
        }
    }
}

impl collection::shards::transfer::ShardTransferConsensus for ShardTransferConsensus {
    fn consensus_state(&self) -> (usize, usize) {
        todo!()
    }
}
