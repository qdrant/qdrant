use crate::operations::types::CollectionResult;
use crate::operations::CollectionUpdateOperations;
use crate::segment_manager::holders::segment_holder::LockedSegmentHolder;
use crate::segment_manager::optimizers::segment_optimizer::SegmentOptimizer;
use crate::wal::SerdeWal;
use crossbeam_channel::Receiver;
use log::debug;
use parking_lot::Mutex;
use segment::types::SeqNumberType;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

pub type Optimizer = dyn SegmentOptimizer + Sync + Send;

pub enum UpdateSignal {
    /// Info that operation with
    Operation(SeqNumberType),
    /// Stop all optimizers and listening
    Stop,
    /// Empty signal used to trigger optimizers
    Nop,
}

pub struct UpdateHandler {
    pub optimizers: Arc<Vec<Box<Optimizer>>>,
    pub flush_timeout_sec: u64,
    segments: LockedSegmentHolder,
    receiver: Receiver<UpdateSignal>,
    worker: Option<JoinHandle<()>>,
    runtime_handle: Handle,
    wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
}

impl UpdateHandler {
    pub fn new(
        optimizers: Arc<Vec<Box<Optimizer>>>,
        receiver: Receiver<UpdateSignal>,
        runtime_handle: Handle,
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
        self.worker = Some(self.runtime_handle.spawn(Self::worker_fn(
            self.optimizers.clone(),
            self.receiver.clone(),
            self.segments.clone(),
            self.wal.clone(),
            self.flush_timeout_sec,
        )));
    }

    /// Gracefully wait before all optimizations stop.
    /// If some optimization is in progress - it will be finished before shutdown.
    /// Blocking function.
    pub fn wait_worker_stops(&mut self) -> CollectionResult<()> {
        let res = match &mut self.worker {
            None => (),
            Some(handle) => self.runtime_handle.block_on(handle)?,
        };

        self.worker = None;
        Ok(res)
    }

    fn process_optimization(optimizers: Arc<Vec<Box<Optimizer>>>, segments: LockedSegmentHolder) {
        for optimizer in optimizers.iter() {
            let mut nonoptimal_segment_ids = optimizer.check_condition(segments.clone());
            while !nonoptimal_segment_ids.is_empty() {
                debug!(
                    "Start optimization on segments: {:?}",
                    nonoptimal_segment_ids
                );
                // If optimization fails, it could not be reported to anywhere except for console.
                // So the only recovery here is to stop optimization and await for restart
                optimizer
                    .optimize(segments.clone(), nonoptimal_segment_ids)
                    .unwrap();
                nonoptimal_segment_ids = optimizer.check_condition(segments.clone());
            }
        }
    }

    async fn worker_fn(
        optimizers: Arc<Vec<Box<Optimizer>>>,
        receiver: Receiver<UpdateSignal>,
        segments: LockedSegmentHolder,
        wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
        flush_timeout_sec: u64,
    ) {
        let flush_timeout = Duration::from_secs(flush_timeout_sec);
        let mut last_flushed = Instant::now();
        loop {
            let recv_res = receiver.recv();
            match recv_res {
                Ok(signal) => {
                    match signal {
                        UpdateSignal::Nop => {
                            Self::process_optimization(optimizers.clone(), segments.clone());
                        }
                        UpdateSignal::Operation(operation_id) => {
                            debug!("Performing update operation: {}", operation_id);
                            Self::process_optimization(optimizers.clone(), segments.clone());

                            let elapsed = last_flushed.elapsed();
                            if elapsed > flush_timeout {
                                debug!("Performing flushing: {}", operation_id);
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
