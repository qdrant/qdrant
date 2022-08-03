use std::sync::mpsc::Sender;

use parking_lot::Mutex;

use crate::{ConsensusOperations, StorageError};

/// Structure used to notify consensus about operation
pub struct OperationSender(Mutex<Sender<Vec<u8>>>);

impl OperationSender {
    pub fn new(sender: Sender<Vec<u8>>) -> Self {
        OperationSender(Mutex::new(sender))
    }

    pub fn send(&self, operation: &ConsensusOperations) -> Result<(), StorageError> {
        let data = serde_cbor::to_vec(&operation)?;
        self.0.lock().send(data)?;
        Ok(())
    }
}

impl Clone for OperationSender {
    fn clone(&self) -> Self {
        OperationSender::new(self.0.lock().clone())
    }
}
