use crossbeam_channel::Receiver;
use segment::types::SeqNumberType;
use std::sync::{Arc};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use crate::segment_manager::optimizers::segment_optimizer::SegmentOptimizer;
use crate::segment_manager::holders::segment_holder::{LockerSegmentHolder};

pub type Optimizer = dyn SegmentOptimizer + Sync + Send;

pub struct UpdateHandler {
    optimizers: Arc<Vec<Box<Optimizer>>>,
    segments: LockerSegmentHolder,
    receiver: Receiver<SeqNumberType>,
    worker: Option<JoinHandle<()>>,
    runtime_handle: Handle,
}


impl UpdateHandler {
    pub fn new(
        optimizers: Arc<Vec<Box<Optimizer>>>,
        receiver: Receiver<SeqNumberType>,
        runtime_handle: Handle,
        segments: LockerSegmentHolder,
    ) -> UpdateHandler {
        let mut handler = UpdateHandler {
            optimizers,
            segments,
            receiver,
            worker: None,
            runtime_handle,
        };
        handler.run_worker();
        handler
    }

    pub fn run_worker(&mut self) {
        self.worker = Some(self.runtime_handle.spawn(
            Self::worker_fn(
                self.optimizers.clone(),
                self.receiver.clone(),
                self.segments.clone())
        ));
    }

    async fn worker_fn(
        optimizers: Arc<Vec<Box<Optimizer>>>,
        receiver: Receiver<SeqNumberType>,
        segments: LockerSegmentHolder,
    ) -> () {
        loop {
            let recv_res = receiver.recv();
            match recv_res {
                Ok(_operation_id) => {
                    for optimizer in optimizers.iter() {
                        let unoptimal_segment_ids = optimizer.check_condition(segments.clone());
                        if !unoptimal_segment_ids.is_empty() {
                            // ToDo: Add logging here
                            optimizer.optimize(segments.clone(), unoptimal_segment_ids).unwrap();
                        }
                    }
                }
                Err(_) => break, // Transmitter was destroyed
            }
        }
    }
}