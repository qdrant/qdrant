use crate::collection_manager::holders::segment_holder::LockedSegmentHolder;
use crate::collection_manager::optimizers::segment_optimizer::SegmentOptimizer;
use crate::operations::types::{CollectionResult, is_service_error};
use crate::operations::CollectionUpdateOperations;
use crate::wal::SerdeWal;
use crossbeam_channel::Receiver;
use log::debug;
use parking_lot::Mutex;
use segment::types::SeqNumberType;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};
use std::cmp::min;
use crate::collection_manager::collection_updater::CollectionUpdater;

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

    /// Gracefully wait before all optimizations stop
    /// If some optimization is in progress - it will be finished before shutdown.
    /// Blocking function.
    pub async fn wait_worker_stops(&mut self) -> CollectionResult<()> {
        let maybe_handle = self.worker.take();
        if let Some(handle) = maybe_handle {
            handle.await?;
        }
        Ok(())
    }

    /// Checks if there are any failed operations.
    /// If so - attempts to re-apply all failed operations.
    fn recover_failed(
        segments: LockedSegmentHolder,
        wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    ) {
        // Try to re-apply everything starting from the first failed operation
        let mut fixed_operations: Vec<SeqNumberType> = vec![];
        let first_failed_operation_option = segments.read().failed_operation.iter().cloned().min();
        match first_failed_operation_option {
            None => {},
            Some(first_failed_op) => {
                let wal_lock = wal.lock();
                for (op_num, operation) in wal_lock.read(first_failed_op) {
                    let res = CollectionUpdater::update(&segments, op_num, operation);
                    if !is_service_error(&res) {
                        fixed_operations.push(op_num)
                    }
                }
            }
        };

        // Remove failed status of according error from all related segments
        if !fixed_operations.is_empty() {
            // Full lock is ok here, since error recovery is not speed priority operation
            let mut write_segments = segments.write();
            for fixed_operation in fixed_operations {
                write_segments.failed_operation.remove(&fixed_operation);
                for (_sid, segment) in write_segments.iter() {
                    segment.get().write().reset_error_state(fixed_operation);
                }
            }
        }
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
                            Self::recover_failed(segments.clone(), wal.clone());
                            Self::process_optimization(optimizers.clone(), segments.clone());
                        }
                        UpdateSignal::Operation(operation_id) => {
                            Self::recover_failed(segments.clone(), wal.clone());
                            Self::process_optimization(optimizers.clone(), segments.clone());

                            let elapsed = last_flushed.elapsed();
                            if elapsed > flush_timeout {
                                debug!("Performing flushing: {}", operation_id);
                                last_flushed = Instant::now();
                                let confirmed_version = {
                                    let read_segments = segments.read();
                                    let flushed_version = read_segments.flush_all().unwrap();
                                    match read_segments.failed_operation.iter().cloned().min() {
                                        None => flushed_version,
                                        Some(failed_operation) => min(failed_operation, flushed_version)
                                    }
                                };
                                wal.lock().ack(confirmed_version).unwrap();
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
