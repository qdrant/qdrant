use std::sync::mpsc::Sender;

use parking_lot::Mutex;

use crate::{ConsensusOperations, StorageError};

/// Structure used to notify consensus about operation
pub struct OperationSender(Mutex<Sender<ConsensusOperations>>);

impl OperationSender {
    pub fn new(sender: Sender<ConsensusOperations>) -> Self {
        OperationSender(Mutex::new(sender))
    }

    pub fn send(&self, operation: ConsensusOperations) -> Result<(), StorageError> {
        self.0.lock().send(operation)?;
        Ok(())
    }
}

impl Clone for OperationSender {
    fn clone(&self) -> Self {
        OperationSender::new(self.0.lock().clone())
    }
}
