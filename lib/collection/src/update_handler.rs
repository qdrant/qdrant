use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::collection_manager::holders::segment_holder::LockedSegmentHolder;
use crate::collection_manager::optimizers::segment_optimizer::SegmentOptimizer;
use crate::common::stoppable_async_task::{async_spawn_stoppable, StoppableAsyncTaskHandle};
use crate::common::stoppable_task::{spawn_stoppable, StoppableTaskHandle};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::CollectionUpdateOperations;
use crate::wal::SerdeWal;
use async_channel::{Receiver, Sender, TryRecvError};
use itertools::Itertools;
use log::{debug, info};
use segment::types::SeqNumberType;
use std::cmp::min;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

pub type Optimizer = dyn SegmentOptimizer + Sync + Send;

/// Information, required to perform operation and notify regarding the result
pub struct OperationData {
    /// Sequential number of the operation
    pub op_num: SeqNumberType,
    /// Operation
    pub operation: CollectionUpdateOperations,
    /// Callback notification channel
    pub sender: Option<Sender<CollectionResult<usize>>>,
}

/// Signal, used to inform Updater process
pub enum UpdateSignal {
    /// Requested operation to perform
    Operation(OperationData),
    /// Empty signal used to trigger optimizers
    Nop,
}

/// Signal, used to inform Optimization process
pub enum OptimizerSignal {
    /// Sequential number of the operation
    Operation(SeqNumberType),
    /// Empty signal used to trigger optimizers
    Nop,
}

/// Structure, which holds object, required for processing updates of the collection
pub struct UpdateHandler {
    /// List of used optimizers
    pub optimizers: Arc<Vec<Arc<Optimizer>>>,
    /// How frequent can we flush data
    pub flush_timeout_sec: u64,
    segments: LockedSegmentHolder,
    /// Channel receiver, which is listened by the updater process
    update_receiver: Receiver<UpdateSignal>,
    /// Process, that listens updates signals and perform updates
    update_worker: Option<StoppableAsyncTaskHandle<()>>,
    /// Process, that listens for post-update signals and performs optimization
    optimizer_worker: Option<StoppableAsyncTaskHandle<()>>,
    runtime_handle: Handle,
    /// WAL, required for operations
    wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    optimization_handles: Arc<Mutex<Vec<StoppableTaskHandle<bool>>>>,
}

impl UpdateHandler {
    pub fn new(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        update_receiver: Receiver<UpdateSignal>,
        runtime_handle: Handle,
        segments: LockedSegmentHolder,
        wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
        flush_timeout_sec: u64,
    ) -> UpdateHandler {
        let mut handler = UpdateHandler {
            optimizers,
            segments,
            update_receiver,
            update_worker: None,
            optimizer_worker: None,
            runtime_handle,
            wal,
            flush_timeout_sec,
            optimization_handles: Arc::new(Mutex::new(vec![])),
        };
        handler.run_workers();
        handler
    }

    pub fn run_workers(&mut self) {
        let (tx, rx) = async_channel::unbounded();
        self.optimizer_worker = Some(async_spawn_stoppable(&self.runtime_handle, |stop| {
            Self::optimization_worker_fn(
                self.optimizers.clone(),
                rx,
                self.segments.clone(),
                self.wal.clone(),
                self.flush_timeout_sec,
                self.optimization_handles.clone(),
                stop,
            )
        }));
        self.update_worker = Some(async_spawn_stoppable(&self.runtime_handle, |stop| {
            Self::update_worker_fn(
                self.update_receiver.clone(),
                tx,
                self.segments.clone(),
                stop,
            )
        }));
    }

    /// Gracefully wait before all optimizations stop
    /// If some optimization is in progress - it will be finished before shutdown.
    pub async fn wait_workers_stops(&mut self) -> CollectionResult<()> {
        debug!("waiting for workers to stop");
        let maybe_update_handle = self.update_worker.take();
        if let Some(handle) = maybe_update_handle {
            debug!("awaiting update_worker thread handle");
            handle.stop().await?;
        }
        let maybe_optimizer_handle = self.optimizer_worker.take();
        if let Some(handle) = maybe_optimizer_handle {
            debug!("awaiting optimizer_worker thread handle");
            handle.stop().await?;
        }
        let mut opt_handles_guard = self.optimization_handles.lock().await;
        let opt_handles = std::mem::take(&mut *opt_handles_guard);
        let stopping_handles = opt_handles.into_iter().map(|h| h.stop()).collect_vec();
        debug!("waiting for {} stop handles", { stopping_handles.len() });
        for res in stopping_handles {
            res.await?;
        }

        Ok(())
    }

    /// Checks if there are any failed operations.
    /// If so - attempts to re-apply all failed operations.
    async fn try_recover(
        segments: LockedSegmentHolder,
        wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    ) -> CollectionResult<usize> {
        // Try to re-apply everything starting from the first failed operation
        let first_failed_operation_option = segments.read().failed_operation.iter().cloned().min();
        match first_failed_operation_option {
            None => {}
            Some(first_failed_op) => {
                debug!("try_recover trying to get Mutex on WAL {}", thread::current().name().unwrap());
                let wal_lock = wal.lock().await;
                debug!("try_recover got wal lock {}", thread::current().name().unwrap());
                for (op_num, operation) in wal_lock.read(first_failed_op) {
                    CollectionUpdater::update(&segments, op_num, operation)?;
                }
                debug!("try_recover release wal lock {}", thread::current().name().unwrap());
            }
        };
        Ok(0)
    }

    /// Checks conditions for all optimizers until there is no suggested segment
    /// Starts a task for each optimization
    /// Returns handles for started tasks
    pub(crate) fn launch_optimization(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        segments: LockedSegmentHolder,
    ) -> Vec<StoppableTaskHandle<bool>> {
        let mut scheduled_segment_ids: HashSet<_> = Default::default();
        let mut handles = vec![];
        for (i, optimizer) in optimizers.iter().enumerate() {
            loop {
                let nonoptimal_segment_ids =
                    optimizer.check_condition(segments.clone(), &scheduled_segment_ids);
                if nonoptimal_segment_ids.is_empty() {
                    break;
                } else {
                    let optim = optimizer.clone();
                    let segs = segments.clone();
                    let nsi = nonoptimal_segment_ids.clone();
                    for sid in &nsi {
                        scheduled_segment_ids.insert(*sid);
                    }

                    handles.push(spawn_stoppable(move |stopped| {
                        debug!("launch_optimization spawn_stoppable '{}' on {}", i, thread::current().name().unwrap());
                        match optim.as_ref().optimize(segs, nsi, stopped) {
                            Ok(result) => result,
                            Err(error) => match error {
                                CollectionError::Cancelled { description } => {
                                    log::info!("Optimization cancelled - {} - {}", description, thread::current().name().unwrap());
                                    false
                                }
                                _ => {
                                    // Error of the optimization can not be handled by API user
                                    // It is only possible to fix after full restart,
                                    // so the best available action here is to stop whole
                                    // optimization thread and log the error
                                    panic!("Optimization error: {}", error);
                                }
                            },
                        }
                    }));
                }
            }
        }
        handles
    }

    pub(crate) async fn process_optimization(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        segments: LockedSegmentHolder,
        optimization_handles: Arc<Mutex<Vec<StoppableTaskHandle<bool>>>>,
    ) {
        let mut new_handles = Self::launch_optimization(optimizers.clone(), segments.clone());
        let mut handles = optimization_handles.lock().await;
        handles.append(&mut new_handles);
        handles.retain(|h| !h.is_finished())
    }

    async fn optimization_worker_fn(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        receiver: Receiver<OptimizerSignal>,
        segments: LockedSegmentHolder,
        wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
        flush_timeout_sec: u64,
        optimization_handles: Arc<Mutex<Vec<StoppableTaskHandle<bool>>>>,
        stopped: Arc<AtomicBool>,
    ) {
        let flush_timeout = Duration::from_secs(flush_timeout_sec);
        let mut last_flushed = Instant::now();
        loop {
            if stopped.load(Ordering::SeqCst) {
                debug!("stopping optimization_worker_fn loop");
                break;
            }
            // do not block on receiving to have the possibility to shutdown an idle pipeline
            let recv_res = receiver.try_recv();
            match recv_res {
                Ok(signal) => {
                    match signal {
                        OptimizerSignal::Nop => {
                            debug!("optimization_worker_fn receiving `NOP`");
                            if Self::try_recover(segments.clone(), wal.clone())
                                .await
                                .is_err()
                            {
                                continue;
                            }
                            Self::process_optimization(
                                optimizers.clone(),
                                segments.clone(),
                                optimization_handles.clone(),
                            )
                            .await;
                        }
                        OptimizerSignal::Operation(operation_id) => {
                            debug!("optimization_worker_fn operation!");
                            if Self::try_recover(segments.clone(), wal.clone())
                                .await
                                .is_err()
                            {
                                continue;
                            }
                            Self::process_optimization(
                                optimizers.clone(),
                                segments.clone(),
                                optimization_handles.clone(),
                            )
                            .await;

                            let elapsed = last_flushed.elapsed();
                            if elapsed > flush_timeout {
                                debug!("Performing flushing: {}", operation_id);
                                last_flushed = Instant::now();
                                let confirmed_version = {
                                    let read_segments = segments.read();
                                    let flushed_version = read_segments.flush_all().unwrap();
                                    match read_segments.failed_operation.iter().cloned().min() {
                                        None => flushed_version,
                                        Some(failed_operation) => {
                                            min(failed_operation, flushed_version)
                                        }
                                    }
                                };
                                wal.lock().await.ack(confirmed_version).unwrap();
                            }
                        }
                    }
                }
                Err(err) => {
                    match err {
                        TryRecvError::Empty => {
                            // loop back after a small delay
                            debug!("optimization_worker_fn looping!");
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        TryRecvError::Closed => {
                            debug!("channel closed");
                            break;
                        } // Transmitter was destroyed
                    }
                }
            }
        }
    }

    async fn update_worker_fn(
        receiver: Receiver<UpdateSignal>,
        optimize_sender: Sender<OptimizerSignal>,
        segments: LockedSegmentHolder,
        stopped: Arc<AtomicBool>,
    ) {
        loop {
            if stopped.load(Ordering::SeqCst) {
                debug!("stopping update_worker_fn loop");
                break;
            }
            // do not block on receiving to have the possibility to shutdown an idle pipeline
            let recv_res = receiver.try_recv();
            match recv_res {
                Ok(signal) => {
                    //debug!("update_worker_fn got signal!");
                    match signal {
                        UpdateSignal::Operation(OperationData {
                            op_num,
                            operation,
                            sender,
                        }) => {
                            let res = match CollectionUpdater::update(&segments, op_num, operation)
                            {
                                Ok(update_res) => optimize_sender
                                    .send(OptimizerSignal::Operation(op_num))
                                    .await
                                    .and(Ok(update_res))
                                    .map_err(|send_err| send_err.into()),
                                Err(err) => Err(err),
                            };
                            if let Some(feedback) = sender {
                                feedback.send(res).await.unwrap_or_else(|_| {
                                    info!("Can't report operation {} result. Assume already not required", op_num);
                                });
                            };
                        }
                        UpdateSignal::Nop => {
                            debug!("update_worker_fn receiving `STOP`");
                            optimize_sender.send(OptimizerSignal::Nop).await.unwrap_or_else(|_| {
                                info!("Can't notify optimizers, assume process is dead. Restart is required");
                            })
                        }
                    }
                }

                Err(err) => {
                    match err {
                        TryRecvError::Empty => {
                            // loop back after a small delay
                            debug!("update_worker_fn looping!");
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        TryRecvError::Closed => {
                            debug!("channel closed");
                            break;
                        } // Transmitter was destroyed
                    }
                }
            }
        }
    }
}
