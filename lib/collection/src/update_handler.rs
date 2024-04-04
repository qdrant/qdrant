use std::cmp::min;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use common::cpu::CpuBudget;
use common::panic;
use itertools::Itertools;
use log::{debug, error, info, trace, warn};
use parking_lot::Mutex;
use segment::common::operation_error::OperationResult;
use segment::index::hnsw_index::num_rayon_threads;
use segment::types::SeqNumberType;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::{self, JoinHandle};
use tokio::time::error::Elapsed;
use tokio::time::{timeout, Duration};

use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::collection_manager::holders::segment_holder::LockedSegmentHolder;
use crate::collection_manager::optimizers::segment_optimizer::SegmentOptimizer;
use crate::collection_manager::optimizers::{Tracker, TrackerLog, TrackerStatus};
use crate::common::stoppable_task::{spawn_stoppable, StoppableTaskHandle};
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::CollectionUpdateOperations;
use crate::shards::local_shard::LocalShardClocks;
use crate::wal::WalError;
use crate::wal_delta::LockedWal;

/// Interval at which the optimizer worker cleans up old optimization handles
///
/// The longer the duration, the longer it  takes for panicked tasks to be reported.
const OPTIMIZER_CLEANUP_INTERVAL: Duration = Duration::from_secs(5);

pub type Optimizer = dyn SegmentOptimizer + Sync + Send;

/// Information, required to perform operation and notify regarding the result
#[derive(Debug)]
pub struct OperationData {
    /// Sequential number of the operation
    pub op_num: SeqNumberType,
    /// Operation
    pub operation: CollectionUpdateOperations,
    /// If operation was requested to wait for result
    pub wait: bool,
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
    /// Ensures that previous updates are applied
    Plunger(oneshot::Sender<()>),
}

/// Signal, used to inform Optimization process
#[derive(PartialEq, Eq, Clone, Copy)]
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
    shared_storage_config: Arc<SharedStorageConfig>,
    /// List of used optimizers
    pub optimizers: Arc<Vec<Arc<Optimizer>>>,
    /// Log of optimizer statuses
    optimizers_log: Arc<Mutex<TrackerLog>>,
    /// Global CPU budget in number of cores for all optimization tasks.
    /// Assigns CPU permits to tasks to limit overall resource utilization.
    optimizer_cpu_budget: CpuBudget,
    /// How frequent can we flush data
    /// This parameter depends on the optimizer config and should be updated accordingly.
    pub flush_interval_sec: u64,
    segments: LockedSegmentHolder,
    /// Process, that listens updates signals and perform updates
    update_worker: Option<JoinHandle<()>>,
    /// Process, that listens for post-update signals and performs optimization
    optimizer_worker: Option<JoinHandle<()>>,
    /// Process that periodically flushes segments and tries to truncate wal
    flush_worker: Option<JoinHandle<()>>,
    /// Sender to stop flush worker
    flush_stop: Option<oneshot::Sender<()>>,
    runtime_handle: Handle,
    /// WAL, required for operations
    wal: LockedWal,
    /// Always keep this WAL version and later and prevent acknowledging/truncating from the WAL.
    /// This is used when other bits of code still depend on information in the WAL, such as the
    /// queue proxy shard.
    /// Defaults to `u64::MAX` to allow acknowledging all confirmed versions.
    pub(super) wal_keep_from: Arc<AtomicU64>,
    optimization_handles: Arc<TokioMutex<Vec<StoppableTaskHandle<bool>>>>,
    /// Maximum number of concurrent optimization jobs in this update handler.
    /// This parameter depends on the optimizer config and should be updated accordingly.
    pub max_optimization_threads: Option<usize>,
    /// Whether optimizers are currently blocked by limits, such as CPU or handle limits.
    optimizers_pending_limits: Arc<AtomicBool>,
    /// Highest and cutoff clocks for the shard WAL.
    clocks: LocalShardClocks,
    shard_path: PathBuf,
}

impl UpdateHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shared_storage_config: Arc<SharedStorageConfig>,
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        optimizers_log: Arc<Mutex<TrackerLog>>,
        optimizer_cpu_budget: CpuBudget,
        runtime_handle: Handle,
        segments: LockedSegmentHolder,
        wal: LockedWal,
        flush_interval_sec: u64,
        max_optimization_threads: Option<usize>,
        clocks: LocalShardClocks,
        shard_path: PathBuf,
    ) -> UpdateHandler {
        UpdateHandler {
            shared_storage_config,
            optimizers,
            segments,
            update_worker: None,
            optimizer_worker: None,
            optimizers_log,
            optimizer_cpu_budget,
            flush_worker: None,
            flush_stop: None,
            runtime_handle,
            wal,
            wal_keep_from: Arc::new(u64::MAX.into()),
            flush_interval_sec,
            optimization_handles: Arc::new(TokioMutex::new(vec![])),
            max_optimization_threads,
            optimizers_pending_limits: Default::default(),
            clocks,
            shard_path,
        }
    }

    pub fn run_workers(&mut self, update_receiver: Receiver<UpdateSignal>) {
        let (tx, rx) = mpsc::channel(self.shared_storage_config.update_queue_size);
        self.optimizer_worker = Some(self.runtime_handle.spawn(Self::optimization_worker_fn(
            self.optimizers.clone(),
            tx.clone(),
            rx,
            self.segments.clone(),
            self.wal.clone(),
            self.optimization_handles.clone(),
            self.optimizers_log.clone(),
            self.optimizer_cpu_budget.clone(),
            self.optimizers_pending_limits.clone(),
            self.max_optimization_threads,
        )));
        self.update_worker = Some(self.runtime_handle.spawn(Self::update_worker_fn(
            update_receiver,
            tx,
            self.wal.clone(),
            self.segments.clone(),
        )));
        let (flush_tx, flush_rx) = oneshot::channel();
        self.flush_worker = Some(self.runtime_handle.spawn(Self::flush_worker(
            self.segments.clone(),
            self.wal.clone(),
            self.wal_keep_from.clone(),
            self.flush_interval_sec,
            flush_rx,
            self.clocks.clone(),
            self.shard_path.clone(),
        )));
        self.flush_stop = Some(flush_tx);
    }

    pub fn stop_flush_worker(&mut self) {
        if let Some(flush_stop) = self.flush_stop.take() {
            if let Err(()) = flush_stop.send(()) {
                warn!("Failed to stop flush worker as it is already stopped.");
            }
        }
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
        let maybe_handle = self.flush_worker.take();
        if let Some(handle) = maybe_handle {
            handle.await?;
        }

        let mut opt_handles_guard = self.optimization_handles.lock().await;
        let opt_handles = std::mem::take(&mut *opt_handles_guard);
        let stopping_handles = opt_handles
            .into_iter()
            .filter_map(|h| h.stop())
            .collect_vec();

        for res in stopping_handles {
            res.await?;
        }

        Ok(())
    }

    /// Checks if there are any failed operations.
    /// If so - attempts to re-apply all failed operations.
    async fn try_recover(segments: LockedSegmentHolder, wal: LockedWal) -> CollectionResult<usize> {
        // Try to re-apply everything starting from the first failed operation
        let first_failed_operation_option = segments.read().failed_operation.iter().cloned().min();
        match first_failed_operation_option {
            None => {}
            Some(first_failed_op) => {
                let wal_lock = wal.lock();
                for (op_num, operation) in wal_lock.read(first_failed_op) {
                    CollectionUpdater::update(&segments, op_num, operation.operation)?;
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
        optimizers_log: Arc<Mutex<TrackerLog>>,
        optimizer_cpu_budget: &CpuBudget,
        segments: LockedSegmentHolder,
        callback: F,
        limit: Option<usize>,
    ) -> Vec<StoppableTaskHandle<bool>>
    where
        F: FnOnce(bool) + Send + Clone + 'static,
    {
        let mut scheduled_segment_ids = HashSet::<_>::default();
        let mut handles = vec![];
        'outer: for optimizer in optimizers.iter() {
            loop {
                // Return early if we reached the optimization job limit
                if limit.map(|extra| handles.len() >= extra).unwrap_or(false) {
                    log::trace!("Reached optimization job limit, postponing other optimizations");
                    break 'outer;
                }

                let nonoptimal_segment_ids =
                    optimizer.check_condition(segments.clone(), &scheduled_segment_ids);
                if nonoptimal_segment_ids.is_empty() {
                    break;
                }

                // Determine how many CPUs we prefer for optimization task, acquire permit for it
                let max_indexing_threads = optimizer.hnsw_config().max_indexing_threads;
                let desired_cpus = num_rayon_threads(max_indexing_threads);
                let permit = match optimizer_cpu_budget.try_acquire(desired_cpus) {
                    Some(permit) => permit,
                    // If there is no CPU budget, break outer loop and return early
                    // If we have no handles (no optimizations) trigger callback so that we wake up
                    // our optimization worker to try again later, otherwise it could get stuck
                    None => {
                        log::trace!(
                            "No available CPU permit for {} optimizer, postponing",
                            optimizer.name(),
                        );
                        if handles.is_empty() {
                            callback(false);
                        }
                        break 'outer;
                    }
                };
                log::trace!(
                    "Acquired {} CPU permit for {} optimizer",
                    permit.num_cpus,
                    optimizer.name(),
                );

                let optimizer = optimizer.clone();
                let optimizers_log = optimizers_log.clone();
                let segments = segments.clone();
                let nsi = nonoptimal_segment_ids.clone();
                scheduled_segment_ids.extend(&nsi);
                let callback = callback.clone();

                let handle = spawn_stoppable(
                    // Stoppable task
                    {
                        let segments = segments.clone();
                        move |stopped| {
                            // Track optimizer status
                            let tracker = Tracker::start(optimizer.as_ref().name(), nsi.clone());
                            let tracker_handle = tracker.handle();
                            optimizers_log.lock().register(tracker);

                            // Optimize and handle result
                            match optimizer.as_ref().optimize(
                                segments.clone(),
                                nsi,
                                permit,
                                stopped,
                            ) {
                                // Perform some actions when optimization if finished
                                Ok(result) => {
                                    tracker_handle.update(TrackerStatus::Done);
                                    callback(result);
                                    result
                                }
                                // Handle and report errors
                                Err(error) => match error {
                                    CollectionError::Cancelled { description } => {
                                        debug!("Optimization cancelled - {}", description);
                                        tracker_handle
                                            .update(TrackerStatus::Cancelled(description));
                                        false
                                    }
                                    _ => {
                                        segments.write().report_optimizer_error(error.clone());

                                        // Error of the optimization can not be handled by API user
                                        // It is only possible to fix after full restart,
                                        // so the best available action here is to stop whole
                                        // optimization thread and log the error
                                        log::error!("Optimization error: {}", error);

                                        tracker_handle
                                            .update(TrackerStatus::Error(error.to_string()));

                                        panic!("Optimization error: {error}");
                                    }
                                },
                            }
                        }
                    },
                    // Panic handler
                    Some(Box::new(move |panic_payload| {
                        let message = panic::downcast_str(&panic_payload).unwrap_or("");
                        let separator = if !message.is_empty() { ": " } else { "" };

                        warn!(
                            "Optimization task panicked, collection may be in unstable state\
                             {separator}{message}"
                        );

                        segments
                            .write()
                            .report_optimizer_error(CollectionError::service_error(format!(
                                "Optimization task panicked{separator}{message}"
                            )));
                    })),
                );
                handles.push(handle);
            }
        }

        handles
    }

    /// Checks conditions for all optimizers and returns whether any is satisfied
    ///
    /// In other words, if this returns true we have pending optimizations.
    pub(crate) fn has_pending_optimizations(&self) -> bool {
        // If optimizations are not pending updates, but are blocked by limits, we do not consider
        // them to be pending. They'll progress once other optimizations finalize
        if self.optimizers_pending_limits.load(Ordering::Relaxed) {
            return false;
        }

        let excluded_ids = HashSet::<_>::default();
        self.optimizers.iter().any(|optimizer| {
            let nonoptimal_segment_ids =
                optimizer.check_condition(self.segments.clone(), &excluded_ids);
            !nonoptimal_segment_ids.is_empty()
        })
    }

    pub(crate) async fn process_optimization(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        segments: LockedSegmentHolder,
        optimization_handles: Arc<TokioMutex<Vec<StoppableTaskHandle<bool>>>>,
        optimizers_log: Arc<Mutex<TrackerLog>>,
        optimizer_cpu_budget: &CpuBudget,
        sender: Sender<OptimizerSignal>,
        limit: usize,
    ) {
        let mut new_handles = Self::launch_optimization(
            optimizers.clone(),
            optimizers_log,
            optimizer_cpu_budget,
            segments.clone(),
            move |_optimization_result| {
                // After optimization is finished, we still need to check if there are
                // some further optimizations possible.
                // If receiver is already dead - we do not care.
                // If channel is full - optimization will be triggered by some other signal
                let _ = sender.try_send(OptimizerSignal::Nop);
            },
            Some(limit),
        );
        let mut handles = optimization_handles.lock().await;
        handles.append(&mut new_handles);
    }

    /// Cleanup finalized optimization task handles
    ///
    /// This finds and removes completed tasks from our list of optimization handles.
    /// It also propagates any panics (and unknown errors) so we properly handle them if desired.
    ///
    /// It is essential to call this every once in a while for handling panics in time.
    async fn cleanup_optimization_handles(
        optimization_handles: Arc<TokioMutex<Vec<StoppableTaskHandle<bool>>>>,
    ) {
        // Remove finished handles
        let finished_handles: Vec<_> = {
            let mut handles = optimization_handles.lock().await;
            (0..handles.len())
                .filter(|i| handles[*i].is_finished())
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .map(|i| handles.remove(i))
                .collect()
        };

        // Finalize all finished handles to propagate panics
        for handle in finished_handles {
            handle.join_and_handle_panic().await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn optimization_worker_fn(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        sender: Sender<OptimizerSignal>,
        mut receiver: Receiver<OptimizerSignal>,
        segments: LockedSegmentHolder,
        wal: LockedWal,
        optimization_handles: Arc<TokioMutex<Vec<StoppableTaskHandle<bool>>>>,
        optimizers_log: Arc<Mutex<TrackerLog>>,
        optimizer_cpu_budget: CpuBudget,
        optimizers_pending_limits: Arc<AtomicBool>,
        max_handles: Option<usize>,
    ) {
        let max_handles = max_handles.unwrap_or(usize::MAX);
        let max_indexing_threads = optimizers
            .first()
            .map(|optimizer| optimizer.hnsw_config().max_indexing_threads)
            .unwrap_or_default();

        // Asynchronous task to trigger optimizers once CPU budget is available again
        let mut cpu_available_trigger: Option<JoinHandle<()>> = None;

        loop {
            let receiver = timeout(OPTIMIZER_CLEANUP_INTERVAL, receiver.recv());
            let result = receiver.await;

            // Always clean up on any signal
            Self::cleanup_optimization_handles(optimization_handles.clone()).await;

            match result {
                // Channel closed or stop signal
                Ok(None | Some(OptimizerSignal::Stop)) => break,
                // Clean up interval
                Err(Elapsed { .. }) => continue,
                // Optimizer signal
                Ok(Some(signal @ (OptimizerSignal::Nop | OptimizerSignal::Operation(_)))) => {
                    // If not forcing with Nop, wait on next signal if we have too many handles
                    if signal != OptimizerSignal::Nop
                        && optimization_handles.lock().await.len() >= max_handles
                    {
                        continue;
                    }

                    if Self::try_recover(segments.clone(), wal.clone())
                        .await
                        .is_err()
                    {
                        continue;
                    }

                    // Continue if we have enough CPU budget available to start an optimization
                    // Otherwise skip now and start a task to trigger the optimizer again once CPU
                    // budget becomes available
                    let desired_cpus = num_rayon_threads(max_indexing_threads);
                    if !optimizer_cpu_budget.has_budget(desired_cpus) {
                        let trigger_active = cpu_available_trigger
                            .as_ref()
                            .map_or(false, |t| !t.is_finished());
                        if !trigger_active {
                            cpu_available_trigger.replace(trigger_optimizers_on_cpu_budget(
                                optimizer_cpu_budget.clone(),
                                desired_cpus,
                                sender.clone(),
                            ));
                        }
                        optimizers_pending_limits.store(true, Ordering::Relaxed);
                        continue;
                    }

                    // Determine optimization handle limit based on max handles we allow
                    // Not related to the CPU budget, but a different limit for the maximum number
                    // of concurrent concrete optimizations per shard as configured by the user in
                    // the Qdrant configuration.
                    // Skip if we reached limit, an ongoing optimization that finishes will trigger this loop again
                    let limit = max_handles.saturating_sub(optimization_handles.lock().await.len());
                    if limit == 0 {
                        log::trace!(
                            "Skipping optimization check, we reached optimization thread limit"
                        );
                        optimizers_pending_limits.store(true, Ordering::Relaxed);
                        continue;
                    }

                    // Reset optimizers are pending limits state
                    optimizers_pending_limits.store(false, Ordering::Relaxed);

                    Self::process_optimization(
                        optimizers.clone(),
                        segments.clone(),
                        optimization_handles.clone(),
                        optimizers_log.clone(),
                        &optimizer_cpu_budget,
                        sender.clone(),
                        limit,
                    )
                    .await;
                }
            }
        }
    }

    async fn update_worker_fn(
        mut receiver: Receiver<UpdateSignal>,
        optimize_sender: Sender<OptimizerSignal>,
        wal: LockedWal,
        segments: LockedSegmentHolder,
    ) {
        while let Some(signal) = receiver.recv().await {
            match signal {
                UpdateSignal::Operation(OperationData {
                    op_num,
                    operation,
                    sender,
                    wait,
                }) => {
                    let flush_res = if wait {
                        wal.lock().flush().map_err(|err| {
                            CollectionError::service_error(format!(
                                "Can't flush WAL before operation {} - {}",
                                op_num, err
                            ))
                        })
                    } else {
                        Ok(())
                    };

                    let operation_result = flush_res
                        .and_then(|_| CollectionUpdater::update(&segments, op_num, operation));

                    let res = match operation_result {
                        Ok(update_res) => optimize_sender
                            .send(OptimizerSignal::Operation(op_num))
                            .await
                            .and(Ok(update_res))
                            .map_err(|send_err| send_err.into()),
                        Err(err) => Err(err),
                    };

                    if let Some(feedback) = sender {
                        feedback.send(res).unwrap_or_else(|_| {
                            debug!(
                                "Can't report operation {} result. Assume already not required",
                                op_num
                            );
                        });
                    };
                }
                UpdateSignal::Stop => {
                    optimize_sender
                        .send(OptimizerSignal::Stop)
                        .await
                        .unwrap_or_else(|_| debug!("Optimizer already stopped"));
                    break;
                }
                UpdateSignal::Nop => optimize_sender
                    .send(OptimizerSignal::Nop)
                    .await
                    .unwrap_or_else(|_| {
                        info!(
                            "Can't notify optimizers, assume process is dead. Restart is required"
                        );
                    }),
                UpdateSignal::Plunger(callback_sender) => {
                    callback_sender.send(()).unwrap_or_else(|_| {
                        debug!("Can't notify sender, assume nobody is waiting anymore");
                    });
                }
            }
        }
        // Transmitter was destroyed
        optimize_sender
            .send(OptimizerSignal::Stop)
            .await
            .unwrap_or_else(|_| debug!("Optimizer already stopped"));
    }

    async fn flush_worker(
        segments: LockedSegmentHolder,
        wal: LockedWal,
        wal_keep_from: Arc<AtomicU64>,
        flush_interval_sec: u64,
        mut stop_receiver: oneshot::Receiver<()>,
        clocks: LocalShardClocks,
        shard_path: PathBuf,
    ) {
        loop {
            // Stop flush worker on signal or if sender was dropped
            // Even if timer did not finish
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(flush_interval_sec)) => {},
                _ = &mut stop_receiver => {
                    debug!("Stopping flush worker.");
                    return;
                }
            }

            trace!("Attempting flushing");
            let wal_flash_job = wal.lock().flush_async();

            if let Err(err) = wal_flash_job.join() {
                error!("Failed to flush wal: {:?}", err);
                segments
                    .write()
                    .report_optimizer_error(WalError::WriteWalError(format!(
                        "WAL flush error: {err:?}"
                    )));
                continue;
            }

            let confirmed_version = Self::flush_segments(segments.clone());
            let confirmed_version = match confirmed_version {
                Ok(version) => version,
                Err(err) => {
                    error!("Failed to flush: {err}");
                    segments.write().report_optimizer_error(err);
                    continue;
                }
            };

            // Acknowledge confirmed version in WAL, but don't acknowledge the specified
            // `keep_from` index or higher.
            // This is to prevent truncating WAL entries that other bits of code still depend on
            // such as the queue proxy shard.
            // Default keep_from is `u64::MAX` to allow acknowledging all confirmed.
            let keep_from = wal_keep_from.load(std::sync::atomic::Ordering::Relaxed);

            // If we should keep the first message, do not acknowledge at all
            if keep_from == 0 {
                continue;
            }

            let ack = confirmed_version.min(keep_from.saturating_sub(1));

            if let Err(err) = clocks.store_if_changed(&shard_path).await {
                log::warn!("Failed to store clock maps to disk: {err}");
                segments.write().report_optimizer_error(err);
            }

            if let Err(err) = wal.lock().ack(ack) {
                log::warn!("Failed to acknowledge WAL version: {err}");
                segments.write().report_optimizer_error(err);
            }
        }
    }

    /// Returns confirmed version after flush of all segments
    ///
    /// # Errors
    /// Returns an error on flush failure
    fn flush_segments(segments: LockedSegmentHolder) -> OperationResult<SeqNumberType> {
        let read_segments = segments.read();
        let flushed_version = read_segments.flush_all(false)?;
        Ok(match read_segments.failed_operation.iter().cloned().min() {
            None => flushed_version,
            Some(failed_operation) => min(failed_operation, flushed_version),
        })
    }
}

/// Trigger optimizers when CPU budget is available
fn trigger_optimizers_on_cpu_budget(
    optimizer_cpu_budget: CpuBudget,
    desired_cpus: usize,
    sender: Sender<OptimizerSignal>,
) -> JoinHandle<()> {
    task::spawn(async move {
        log::trace!("Skipping optimization checks, waiting for CPU budget to be available");
        optimizer_cpu_budget
            .notify_on_budget_available(desired_cpus)
            .await;
        log::trace!("Continue optimization checks, new CPU budget available");

        // Trigger optimizers with Nop operation
        sender.send(OptimizerSignal::Nop).await.unwrap_or_else(|_| {
            log::info!("Can't notify optimizers, assume process is dead. Restart is required")
        });
    })
}
