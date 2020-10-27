use crossbeam_channel::Receiver;
use segment::types::SeqNumberType;
use std::sync::{Arc};
use tokio::task::JoinHandle;
use crate::segment_manager::optimizers::segment_optimizer::SegmentOptimizer;
use crate::segment_manager::holders::segment_holder::{LockedSegmentHolder};
use parking_lot::Mutex;
use crate::wal::SerdeWal;
use crate::operations::CollectionUpdateOperations;
use tokio::time::{Duration, Instant};
use tokio::runtime::Runtime;

pub type Optimizer = dyn SegmentOptimizer + Sync + Send;

pub enum UpdateSignal {
    Operation(SeqNumberType),
    Stop,
}

pub struct UpdateHandler {
    optimizers: Arc<Vec<Box<Optimizer>>>,
    segments: LockedSegmentHolder,
    receiver: Receiver<UpdateSignal>,
    worker: Option<JoinHandle<()>>,
    runtime_handle: Arc<Runtime>,
    wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    flush_timeout_sec: u64,
}


impl UpdateHandler {
    pub fn new(
        optimizers: Arc<Vec<Box<Optimizer>>>,
        receiver: Receiver<UpdateSignal>,
        runtime_handle: Arc<Runtime>,
        segments: LockedSegmentHolder,
        wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
        flush_timeout_sec: u64,
    ) -> UpdateHandler {
        let mut handler = UpdateHandler {
            optimizers,
            segments,
            receiver,
            worker: None,
            runtime_handle,
            wal,
            flush_timeout_sec,
        };
        handler.run_worker();
        handler
    }

    pub fn run_worker(&mut self) {
        self.worker = Some(self.runtime_handle.spawn(
            Self::worker_fn(
                self.optimizers.clone(),
                self.receiver.clone(),
                self.segments.clone(),
                self.wal.clone(),
                self.flush_timeout_sec,
            ),
        ));
    }

    async fn worker_fn(
        optimizers: Arc<Vec<Box<Optimizer>>>,
        receiver: Receiver<UpdateSignal>,
        segments: LockedSegmentHolder,
        wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
        flush_timeout_sec: u64,
    ) -> () {
        let flush_timeout = Duration::from_secs(flush_timeout_sec);
        let mut last_flushed = Instant::now();
        loop {
            let recv_res = receiver.recv();
            match recv_res {
                Ok(signal) => {
                    match signal {
                        UpdateSignal::Operation(_) => {
                            for optimizer in optimizers.iter() {
                                let unoptimal_segment_ids = optimizer.check_condition(segments.clone());
                                if !unoptimal_segment_ids.is_empty() {
                                    // ToDo: Add logging here
                                    optimizer.optimize(segments.clone(), unoptimal_segment_ids).unwrap();
                                }
                            }
                            let elapsed = last_flushed.elapsed();
                            if elapsed > flush_timeout {
                                last_flushed = Instant::now();
                                let flushed_operation = segments.read().flush_all().unwrap();
                                wal.lock().ack(flushed_operation).unwrap();
                            }
                        }
                        UpdateSignal::Stop => break, // Stop gracefully
                    }
                }
                Err(_) => break, // Transmitter was destroyed
            }
        }
    }
}