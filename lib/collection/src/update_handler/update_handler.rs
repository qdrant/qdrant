use crate::segment_manager::segment_managers::{SegmentUpdater, SegmentOptimizer};
use crossbeam_channel::Receiver;
use segment::types::SeqNumberType;
use std::future::Future;
use std::sync::{Arc, RwLock};
use crate::wal::SerdeWal;
use crate::operations::CollectionUpdateOperations;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

type Optimizer = dyn SegmentOptimizer + Sync + Send;

pub struct UpdateHandler {
    optimizer: Arc<Optimizer>,
    receiver: Receiver<SeqNumberType>,
    worker: JoinHandle<()>,
    runtime_handle: Handle,
}


impl UpdateHandler {

    pub fn new(
        optimizer: Arc<Optimizer>,
        receiver: Receiver<SeqNumberType>,
        runtime_handle: Handle
    ) -> UpdateHandler {

        let worker = runtime_handle.spawn(
            Self::worker_fn(optimizer.clone(), receiver.clone()));

        UpdateHandler {
            optimizer,
            receiver,
            worker,
            runtime_handle
        }
    }

    async fn worker_fn(
        updater: Arc<Optimizer>,
        receiver: Receiver<SeqNumberType>,
    ) -> () {
        loop {
            let recv_res = receiver.recv();
            match recv_res {
                Ok(operation_id) => {
                    if updater.check_condition(operation_id) {
                        updater.optimize()
                    }
                },
                Err(_) => break, // Transmitter was destroyed
            }
        }
    }
}