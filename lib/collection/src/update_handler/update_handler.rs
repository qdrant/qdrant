use crate::segment_manager::segment_managers::SegmentUpdater;
use crossbeam_channel::Receiver;
use segment::types::SeqNumberType;
use std::future::Future;
use std::sync::{Arc, RwLock};
use crate::wal::SerdeWal;
use crate::operations::CollectionUpdateOperations;

pub struct UpdateHandler {
    updater: Box<dyn SegmentUpdater>,
    receiver: Receiver<SeqNumberType>,
    worker: Box<dyn Future<Output=()>>,
    wal: Arc<RwLock<SerdeWal<CollectionUpdateOperations>>>,
}


impl UpdateHandler {

    pub fn worker_fn(
        updater: &dyn SegmentUpdater,
        receiver: Receiver<SeqNumberType>,
        wal: Arc<RwLock<SerdeWal<CollectionUpdateOperations>>>,
    ) -> () {
        loop {
            let operation_id = receiver.recv();
            match operation_id {
                Ok(_) => {
                    let wal_guard =  wal.read().unwrap();
                },
                Err(_) => break, // Transmitter was destroyed
            }
        }
    }
}