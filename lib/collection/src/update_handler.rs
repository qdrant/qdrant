use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::collection_manager::holders::segment_holder::LockedSegmentHolder;
use crate::collection_manager::optimizers::segment_optimizer::SegmentOptimizer;
use crate::common::stoppable_task::{spawn_stoppable, StoppableTaskHandle};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::CollectionUpdateOperations;
use crate::wal::SerdeWal;
use itertools::Itertools;
use log::{debug, info};
use segment::types::SeqNumberType;
use std::cmp::min;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    oneshot, Mutex,
};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

pub type Optimizer = dyn SegmentOptimizer + Sync + Send;

/// Information, required to perform operation and notify regarding the result
#[derive(Debug)]
pub struct OperationData {
    /// Sequential number of the operation
    pub op_num: SeqNumberType,
    /// Operation
    pub operation: CollectionUpdateOperations,
    /// Callback notification channel
    pub sender: Option<oneshot::Sender<CollectionResult<usize>>>,
}

/// Signal, used to inform Updater process
#[derive(Debug)]
pub enum UpdateSignal {
    /// Requested operation to perform
    Operation(OperationData),
    /// Stop all optimizers and listening
    Stop,
    /// Empty signal used to trigger optimizers
    Nop,
}

/// Signal, used to inform Optimization process
pub enum OptimizerSignal {
    /// Sequential number of the operation
    Operation(SeqNumberType),
    /// Stop all optimizers and listening
    Stop,
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
    /// Process, that listens updates signals and perform updates
    update_worker: Option<JoinHandle<()>>,
    /// Process, that listens for post-update signals and performs optimization
    optimizer_worker: Option<JoinHandle<()>>,
    runtime_handle: Handle,
    /// WAL, required for operations
    wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    optimization_handles: Arc<Mutex<Vec<StoppableTaskHandle<bool>>>>,
}

impl UpdateHandler {
    pub fn new(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        runtime_handle: Handle,
        segments: LockedSegmentHolder,
        wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
        flush_timeout_sec: u64,
    ) -> UpdateHandler {
        UpdateHandler {
            optimizers,
            segments,
            update_worker: None,
            optimizer_worker: None,
            runtime_handle,
            wal,
            flush_timeout_sec,
            optimization_handles: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn run_workers(&mut self, update_receiver: UnboundedReceiver<UpdateSignal>) {
        let (tx, rx) = mpsc::unbounded_channel();
        self.optimizer_worker = Some(self.runtime_handle.spawn(Self::optimization_worker_fn(
            self.optimizers.clone(),
            tx.clone(),
            rx,
            self.segments.clone(),
            self.wal.clone(),
            self.flush_timeout_sec,
            self.optimization_handles.clone(),
        )));
        self.update_worker = Some(self.runtime_handle.spawn(Self::update_worker_fn(
            update_receiver,
            tx,
            self.segments.clone(),
        )));
    }

    /// Gracefully wait before all optimizations stop
    /// If some optimization is in progress - it will be finished before shutdown.
    pub async fn wait_workers_stops(&mut self) -> CollectionResult<()> {
        let maybe_handle = self.update_worker.take();
        if let Some(handle) = maybe_handle {
            handle.await?;
        }
        let maybe_handle = self.optimizer_worker.take();
        if let Some(handle) = maybe_handle {
            handle.await?;
        }

        let mut opt_handles_guard = self.optimization_handles.lock().await;
        let opt_handles = std::mem::take(&mut *opt_handles_guard);
        let stopping_handles = opt_handles.into_iter().map(|h| h.stop()).collect_vec();

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
                let wal_lock = wal.lock().await;
                for (op_num, operation) in wal_lock.read(first_failed_op) {
                    CollectionUpdater::update(&segments, op_num, operation)?;
                }
            }
        };
        Ok(0)
    }

    /// Checks conditions for all optimizers until there is no suggested segment
    /// Starts a task for each optimization
    /// Returns handles for started tasks
    pub(crate) fn launch_optimization<F>(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        segments: LockedSegmentHolder,
        callback: F,
    ) -> Vec<StoppableTaskHandle<bool>>
    where
        F: FnOnce(bool),
        F: Send + 'static,
        F: Clone,
    {
        let mut scheduled_segment_ids: HashSet<_> = Default::default();
        let mut handles = vec![];
        for optimizer in optimizers.iter() {
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
                    let callback_cloned = callback.clone();

                    handles.push(spawn_stoppable(move |stopped| {
                        match optim.as_ref().optimize(segs.clone(), nsi, stopped) {
                            Ok(result) => {
                                callback_cloned(result); // Perform some actions when optimization if finished
                                result
                            }
                            Err(error) => match error {
                                CollectionError::Cancelled { description } => {
                                    log::info!("Optimization cancelled - {}", description);
                                    false
                                }
                                _ => {
                                    {
                                        let mut segments_write = segs.write();
                                        if segments_write.optimizer_errors.is_none() {
                                            // Save only the first error
                                            // If is more likely to be the real cause of all further problems
                                            segments_write.optimizer_errors = Some(error.clone());
                                        }
                                    }
                                    // Error of the optimization can not be handled by API user
                                    // It is only possible to fix after full restart,
                                    // so the best available action here is to stop whole
                                    // optimization thread and log the error
                                    log::error!("Optimization error: {}", error);
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
        sender: UnboundedSender<OptimizerSignal>,
    ) {
        let mut new_handles = Self::launch_optimization(
            optimizers.clone(),
            segments.clone(),
            move |_optimization_result| {
                // After optimization is finished, we still need to check if there are
                // some further optimizations possible.
                // If receiver is already dead - we do not care.
                let _ = sender.send(OptimizerSignal::Nop);
            },
        );
        let mut handles = optimization_handles.lock().await;
        handles.append(&mut new_handles);
        handles.retain(|h| !h.is_finished())
    }

    async fn optimization_worker_fn(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        sender: UnboundedSender<OptimizerSignal>,
        mut receiver: UnboundedReceiver<OptimizerSignal>,
        segments: LockedSegmentHolder,
        wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
        flush_timeout_sec: u64,
        optimization_handles: Arc<Mutex<Vec<StoppableTaskHandle<bool>>>>,
    ) {
        let flush_timeout = Duration::from_secs(flush_timeout_sec);
        let mut last_flushed = Instant::now();
        while let Some(signal) = receiver.recv().await {
            match signal {
                OptimizerSignal::Nop => {
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
                        sender.clone(),
                    )
                    .await;
                }
                OptimizerSignal::Operation(operation_id) => {
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
                        sender.clone(),
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
                                Some(failed_operation) => min(failed_operation, flushed_version),
                            }
                        };
                        wal.lock().await.ack(confirmed_version).unwrap();
                    }
                }
                OptimizerSignal::Stop => break, // Stop gracefully
            }
        }
    }

    async fn update_worker_fn(
        mut receiver: UnboundedReceiver<UpdateSignal>,
        optimize_sender: UnboundedSender<OptimizerSignal>,
        segments: LockedSegmentHolder,
    ) {
        while let Some(signal) = receiver.recv().await {
            match signal {
                UpdateSignal::Operation(OperationData {
                    op_num,
                    operation,
                    sender,
                }) => {
                    let res = match CollectionUpdater::update(&segments, op_num, operation) {
                        Ok(update_res) => optimize_sender
                            .send(OptimizerSignal::Operation(op_num))
                            .and(Ok(update_res))
                            .map_err(|send_err| send_err.into()),
                        Err(err) => Err(err),
                    };

                    if let Some(feedback) = sender {
                        feedback.send(res).unwrap_or_else(|_| {
                            info!(
                                "Can't report operation {} result. Assume already not required",
                                op_num
                            );
                        });
                    };
                }
                UpdateSignal::Stop => {
                    optimize_sender
                        .send(OptimizerSignal::Stop)
                        .unwrap_or_else(|_| debug!("Optimizer already stopped"));
                    break;
                }
                UpdateSignal::Nop => {
                    optimize_sender
                        .send(OptimizerSignal::Nop)
                        .unwrap_or_else(|_| {
                            info!(
                            "Can't notify optimizers, assume process is dead. Restart is required"
                        );
                        })
                }
            }
        }
        // Transmitter was destroyed
        optimize_sender
            .send(OptimizerSignal::Stop)
            .unwrap_or_else(|_| debug!("Optimizer already stopped"));
    }
}
