use std::sync::mpsc::Sender;

use collection::shard::{CollectionId, ShardTransfer};
use parking_lot::Mutex;

use crate::content_manager::collection_meta_ops::ShardTransferOperations;
use crate::{CollectionMetaOperations, ConsensusOperations, StorageError};

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

        self.send(&operation)
    }
}

impl Clone for OperationSender {
    fn clone(&self) -> Self {
        OperationSender::new(self.0.lock().clone())
    }
}
