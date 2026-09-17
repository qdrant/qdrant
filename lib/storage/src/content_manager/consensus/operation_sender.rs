use std::sync::mpsc::Sender;
use std::time::Instant;

use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::{ConsensusOperations, StorageError};

/// Structure used to notify consensus about operation
pub struct OperationSender(Mutex<Sender<ConsensusRequest>>);

pub enum ConsensusRequest {
    Operation(ConsensusOperations),
    RemovePeer(PeerRemoval),
}

/// A local request, not a logged operation. The consensus loop discards it if
/// it observes a closed reply receiver before submission. Closure can race
/// with submission and cannot cancel a Raft proposal.
pub struct PeerRemoval {
    pub peer_id: u64,
    pub deadline: Instant,
    /// Result of local Raft submission, not commitment or application.
    pub proposed: oneshot::Sender<Result<(), StorageError>>,
}

impl OperationSender {
    pub fn new(sender: Sender<ConsensusRequest>) -> Self {
        OperationSender(Mutex::new(sender))
    }

    pub fn send(&self, operation: ConsensusOperations) -> Result<(), StorageError> {
        self.0.lock().send(ConsensusRequest::Operation(operation))?;
        Ok(())
    }

    pub fn remove_peer(
        &self,
        peer_id: u64,
        deadline: Instant,
    ) -> Result<oneshot::Receiver<Result<(), StorageError>>, StorageError> {
        let (proposed, receiver) = oneshot::channel();
        self.0
            .lock()
            .send(ConsensusRequest::RemovePeer(PeerRemoval {
                peer_id,
                deadline,
                proposed,
            }))?;
        Ok(receiver)
    }
}

impl Clone for OperationSender {
    fn clone(&self) -> Self {
        OperationSender::new(self.0.lock().clone())
    }
}
