use std::sync::mpsc::Sender;

use collection::shard::{CollectionId, ShardTransfer};
use parking_lot::Mutex;

use crate::content_manager::collection_meta_ops::ShardTransferOperations;
use crate::{CollectionMetaOperations, ConsensusOperations, StorageError};

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

    pub fn cancel_transfer(
        &self,
        collection_id: CollectionId,
        transfer: ShardTransfer,
        reason: &str,
    ) -> Result<(), StorageError> {
        let operation =
            ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::TransferShard(
                collection_id,
                ShardTransferOperations::Abort {
                    transfer,
                    reason: reason.to_string(),
                },
            )));

        self.send(operation)
    }
}

impl Clone for OperationSender {
    fn clone(&self) -> Self {
        OperationSender::new(self.0.lock().clone())
    }
}
