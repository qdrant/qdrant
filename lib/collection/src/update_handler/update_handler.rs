use crate::segment_manager::segment_managers::SegmentOptimizer;
use crossbeam_channel::Receiver;
use segment::types::SeqNumberType;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

pub type Optimizer = dyn SegmentOptimizer + Sync + Send;

pub struct UpdateHandler {
    optimizers: Arc<Vec<Box<Optimizer>>>,
    receiver: Receiver<SeqNumberType>,
    worker: JoinHandle<()>,
    runtime_handle: Handle,
}


impl UpdateHandler {

    pub fn new(
        optimizers: Arc<Vec<Box<Optimizer>>>,
        receiver: Receiver<SeqNumberType>,
        runtime_handle: Handle
    ) -> UpdateHandler {

        let worker = runtime_handle.spawn(
            Self::worker_fn(optimizers.clone(), receiver.clone()));

        UpdateHandler {
            optimizers,
            receiver,
            worker,
            runtime_handle
        }
    }

    async fn worker_fn(
        optimizers: Arc<Vec<Box<Optimizer>>>,
        receiver: Receiver<SeqNumberType>,
    ) -> () {
        loop {
            let recv_res = receiver.recv();
            match recv_res {
                Ok(operation_id) => {
                    for optimizer in optimizers.iter() {
                        if optimizer.check_condition(operation_id) {
                            optimizer.optimize()
                        }
                    }
                },
                Err(_) => break, // Transmitter was destroyed
            }
        }
    }
}