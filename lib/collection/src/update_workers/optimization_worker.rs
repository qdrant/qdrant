use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use common::budget::ResourceBudget;
use common::counter::hardware_counter::HardwareCounterCell;
use common::panic;
use common::save_on_disk::SaveOnDisk;
use parking_lot::Mutex;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::index::hnsw_index::num_rayon_threads;
use segment::types::QuantizationConfig;
use shard::payload_index_schema::PayloadIndexSchema;
use shard::segment_holder::LockedSegmentHolder;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;
use tokio::time::timeout;

use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::collection_manager::optimizers::segment_optimizer::{
    OptimizationPlanner, OptimizerThresholds,
};
use crate::collection_manager::optimizers::{Tracker, TrackerLog, TrackerStatus};
use crate::common::stoppable_task::{StoppableTaskHandle, spawn_stoppable};
use crate::config::CollectionParams;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::update_tracker::UpdateTracker;
use crate::update_handler::{Optimizer, OptimizerSignal};
use crate::update_workers::UpdateWorkers;
use crate::wal_delta::LockedWal;

/// Interval at which the optimizer worker cleans up old optimization handles
///
/// The longer the duration, the longer it takes for panicked tasks to be reported.
const OPTIMIZER_CLEANUP_INTERVAL: Duration = Duration::from_secs(5);

impl UpdateWorkers {
    #[allow(clippy::too_many_arguments)]
    pub async fn optimization_worker_fn(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        sender: Sender<OptimizerSignal>,
        mut receiver: Receiver<OptimizerSignal>,
        segments: LockedSegmentHolder,
        wal: LockedWal,
        optimization_handles: Arc<TokioMutex<Vec<StoppableTaskHandle<bool>>>>,
        optimizers_log: Arc<Mutex<TrackerLog>>,
        total_optimized_points: Arc<AtomicUsize>,
        optimizer_resource_budget: ResourceBudget,
        max_handles: Option<usize>,
        has_triggered_optimizers: Arc<AtomicBool>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        update_operation_lock: Arc<tokio::sync::RwLock<()>>,
        update_tracker: UpdateTracker,
    ) {
        let max_handles = max_handles.unwrap_or(usize::MAX);
        let max_indexing_threads = optimizers
            .first()
            .map(|optimizer| optimizer.hnsw_config().max_indexing_threads)
            .unwrap_or_default();

        // Asynchronous task to trigger optimizers once CPU budget is available again
        let mut resource_available_trigger: Option<JoinHandle<()>> = None;

        loop {
            let result = timeout(OPTIMIZER_CLEANUP_INTERVAL, receiver.recv()).await;

            let cleaned_any =
                Self::cleanup_optimization_handles(optimization_handles.clone()).await;

            // Either continue below here with the worker, or reloop/break
            // Decision logic doing one of three things:
            // 1. run optimizers
            // 2. reloop and wait for next signal
            // 3. break here and stop the optimization worker
            let ignore_max_handles = match result {
                // Regular optimizer signal: run optimizers: do 1
                Ok(Some(OptimizerSignal::Operation(_))) => false,
                // Optimizer signal ignoring max handles: do 1
                Ok(Some(OptimizerSignal::Nop)) => true,
                // Hit optimizer cleanup interval, did clean up a task: do 1
                Err(Elapsed { .. }) if cleaned_any => {
                    // This branch prevents a race condition where optimizers would get stuck
                    // If the optimizer cleanup interval was triggered and we did clean any task we
                    // must run optimizers now. If we don't there may not be any other ongoing
                    // tasks that'll trigger this for us. If we don't run optimizers here we might
                    // get stuck into yellow state until a new update operation is received.
                    // See: <https://github.com/qdrant/qdrant/pull/5111>
                    log::warn!(
                        "Cleaned an optimization handle after timeout, explicitly triggering optimizers",
                    );
                    true
                }
                // Hit optimizer cleanup interval, did not clean up a task: do 2
                Err(Elapsed { .. }) => continue,
                // Channel closed or received stop signal: do 3
                Ok(None | Some(OptimizerSignal::Stop)) => break,
            };

            has_triggered_optimizers.store(true, Ordering::Relaxed);

            // Ensure we have at least one appendable segment with enough capacity
            // Source required parameters from first optimizer
            if let Some(optimizer) = optimizers.first() {
                let result = Self::ensure_appendable_segment_with_capacity(
                    &segments,
                    optimizer.segments_path(),
                    &optimizer.collection_params(),
                    optimizer.quantization_config().as_ref(),
                    optimizer.threshold_config(),
                    payload_index_schema.clone(),
                );
                if let Err(err) = result {
                    log::error!(
                        "Failed to ensure there are appendable segments with capacity: {err}"
                    );
                    panic!("Failed to ensure there are appendable segments with capacity: {err}");
                }
            }

            // If not forcing, wait on next signal if we have too many handles
            if !ignore_max_handles && optimization_handles.lock().await.len() >= max_handles {
                continue;
            }

            if Self::try_recover(
                segments.clone(),
                wal.clone(),
                update_operation_lock.clone(),
                update_tracker.clone(),
            )
            .await
            .is_err()
            {
                continue;
            }

            // Continue if we have enough resource budget available to start an optimization
            // Otherwise skip now and start a task to trigger the optimizer again once resource
            // budget becomes available
            let desired_cpus = 0;
            let desired_io = num_rayon_threads(max_indexing_threads);
            if !optimizer_resource_budget.has_budget(desired_cpus, desired_io) {
                let trigger_active = resource_available_trigger
                    .as_ref()
                    .is_some_and(|t| !t.is_finished());
                if !trigger_active {
                    resource_available_trigger.replace(
                        Self::trigger_optimizers_on_resource_budget(
                            optimizer_resource_budget.clone(),
                            desired_cpus,
                            desired_io,
                            sender.clone(),
                        ),
                    );
                }
                continue;
            }

            // Determine optimization handle limit based on max handles we allow
            // Not related to the CPU budget, but a different limit for the maximum number
            // of concurrent concrete optimizations per shard as configured by the user in
            // the Qdrant configuration.
            // Skip if we reached limit, an ongoing optimization that finishes will trigger this loop again
            let limit = max_handles.saturating_sub(optimization_handles.lock().await.len());
            if limit == 0 {
                log::trace!("Skipping optimization check, we reached optimization thread limit");
                continue;
            }

            Self::process_optimization(
                optimizers.clone(),
                segments.clone(),
                optimization_handles.clone(),
                optimizers_log.clone(),
                total_optimized_points.clone(),
                &optimizer_resource_budget,
                sender.clone(),
                limit,
            )
            .await;
        }
    }

    /// Cleanup finalized optimization task handles
    ///
    /// This finds and removes completed tasks from our list of optimization handles.
    /// It also propagates any panics (and unknown errors) so we properly handle them if desired.
    ///
    /// It is essential to call this every once in a while for handling panics in time.
    ///
    /// Returns true if any optimization handle was finished, joined and removed.
    async fn cleanup_optimization_handles(
        optimization_handles: Arc<TokioMutex<Vec<StoppableTaskHandle<bool>>>>,
    ) -> bool {
        // Remove finished handles
        let finished_handles: Vec<_> = {
            let mut handles = optimization_handles.lock().await;
            (0..handles.len())
                .filter(|i| handles[*i].is_finished())
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .map(|i| handles.swap_remove(i))
                .collect()
        };

        let finished_any = !finished_handles.is_empty();

        for handle in finished_handles {
            handle.join().await;
        }

        finished_any
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn process_optimization(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        segments: LockedSegmentHolder,
        optimization_handles: Arc<TokioMutex<Vec<StoppableTaskHandle<bool>>>>,
        optimizers_log: Arc<Mutex<TrackerLog>>,
        total_optimized_points: Arc<AtomicUsize>,
        optimizer_resource_budget: &ResourceBudget,
        sender: Sender<OptimizerSignal>,
        limit: usize,
    ) {
        let mut new_handles = Self::launch_optimization(
            optimizers.clone(),
            optimizers_log,
            total_optimized_points,
            optimizer_resource_budget,
            segments.clone(),
            move || {
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

    /// Checks conditions for all optimizers until there is no suggested segment
    /// Starts a task for each optimization
    /// Returns handles for started tasks
    pub(crate) fn launch_optimization<F>(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        optimizers_log: Arc<Mutex<TrackerLog>>,
        total_optimized_points: Arc<AtomicUsize>,
        optimizer_resource_budget: &ResourceBudget,
        segments: LockedSegmentHolder,
        callback: F,
        limit: Option<usize>,
    ) -> Vec<StoppableTaskHandle<bool>>
    where
        F: Fn() + Send + Clone + Sync + 'static,
    {
        let mut handles = vec![];
        let is_optimization_failed = Arc::new(AtomicBool::new(false));

        let scheduled = {
            let segments = segments.read();
            let mut planner = OptimizationPlanner::new(
                segments.running_optimizations.count(),
                segments.iter_original(),
            );
            for optimizer in optimizers.iter() {
                planner.set_optimizer(Arc::clone(optimizer));
                optimizer.plan_optimizations(&mut planner);
            }
            planner.into_scheduled()
        };

        for (optimizer, segments_to_merge) in scheduled {
            let Some(optimizer) = optimizer else {
                debug_assert!(false);
                continue;
            };

            // Return early if we reached the optimization job limit
            if limit.map(|extra| handles.len() >= extra).unwrap_or(false) {
                log::trace!("Reached optimization job limit, postponing other optimizations");
                break;
            }

            // If optimization failed, we should not endlessly try to optimize same segments
            if is_optimization_failed.load(Ordering::Relaxed) {
                log::debug!("Skipping further optimizations due to previous failure");
                break;
            }

            log::debug!(
                "Optimizer '{}' running on segments: {:?}",
                optimizer.name(),
                &segments_to_merge
            );

            // Determine how many Resources we prefer for optimization task, acquire permit for it
            // And use same amount of IO threads as CPUs
            let max_indexing_threads = optimizer.hnsw_config().max_indexing_threads;
            let desired_io = num_rayon_threads(max_indexing_threads);
            let Some(mut permit) = optimizer_resource_budget.try_acquire(0, desired_io) else {
                // If there is no Resource budget, break and return early
                // If we have no handles (no optimizations) trigger callback so that we wake up
                // our optimization worker to try again later, otherwise it could get stuck
                log::trace!(
                    "No available IO permit for {} optimizer, postponing",
                    optimizer.name(),
                );
                if handles.is_empty() {
                    callback();
                }
                break;
            };
            log::trace!(
                "Acquired {} IO permit for {} optimizer",
                permit.num_io,
                optimizer.name(),
            );

            let permit_callback = callback.clone();

            permit.set_on_manual_release(move || {
                // Notify scheduler that resource budget is explicitly changed
                permit_callback();
            });

            let callback = callback.clone();
            let optimizer = optimizer.clone();
            let optimizers_log = optimizers_log.clone();
            let total_optimized_points = total_optimized_points.clone();
            let segments = segments.clone();
            let is_optimization_failed = is_optimization_failed.clone();
            let resource_budget = optimizer_resource_budget.clone();

            // Track optimizer status
            let (tracker, progress) =
                Tracker::start(optimizer.as_ref().name(), segments_to_merge.clone());
            let tracker_handle = tracker.handle();

            let handle = spawn_stoppable(move |stopped| {
                let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    optimizer.as_ref().optimize(
                        segments.clone(),
                        segments_to_merge,
                        permit,
                        resource_budget,
                        stopped,
                        progress,
                        Box::new(move || {
                            // Do not clutter the log with early cancelled optimizations,
                            // wait for `on_successful_start` instead.
                            optimizers_log.lock().register(tracker);
                        }),
                    )
                }));
                let is_optimized;
                let status;
                let reported_error;
                match result {
                    // Success
                    Ok(Ok(optimized_points)) => {
                        is_optimized = optimized_points > 0;
                        status = TrackerStatus::Done;
                        reported_error = None;
                        total_optimized_points.fetch_add(optimized_points, Ordering::Relaxed);
                        callback();
                    }
                    // Cancelled
                    Ok(Err(CollectionError::Cancelled { description })) => {
                        is_optimized = false;
                        log::debug!("Optimization cancelled - {description}");
                        status = TrackerStatus::Cancelled(description);
                        reported_error = None;
                    }
                    // `optimize()` returned Result::Err
                    Ok(Err(error)) => {
                        is_optimized = false;
                        status = TrackerStatus::Error(error.to_string());
                        log::error!("Optimization error: {error}");
                        reported_error = Some(error);
                    }
                    // `optimize()` panicked
                    Err(panic_payload) => {
                        let message = panic::downcast_str(&panic_payload).unwrap_or("");
                        let separator = if !message.is_empty() { ": " } else { "" };
                        let status_msg = format!("Optimization task panicked{separator}{message}");

                        is_optimized = false;
                        status = TrackerStatus::Error(status_msg.clone());
                        reported_error = Some(CollectionError::service_error(status_msg));
                        log::warn!(
                            "Optimization task panicked, collection may be in unstable state\
                             {separator}{message}"
                        );
                    }
                }
                tracker_handle.update(status);
                if let Some(reported_error) = reported_error {
                    segments.write().report_optimizer_error(reported_error);
                    is_optimization_failed.store(true, Ordering::Relaxed);
                }
                is_optimized
            });
            handles.push(handle);
        }

        handles
    }

    /// Ensure there is at least one appendable segment with enough capacity
    ///
    /// If there is no appendable segment, or all are at or over capacity, a new empty one is
    /// created.
    ///
    /// Capacity is determined based on `optimizers.max_segment_size_kb`.
    pub fn ensure_appendable_segment_with_capacity(
        segments: &LockedSegmentHolder,
        segments_path: &Path,
        collection_params: &CollectionParams,
        collection_quantization: Option<&QuantizationConfig>,
        thresholds_config: &OptimizerThresholds,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
    ) -> OperationResult<()> {
        let no_segment_with_capacity = {
            let segments_read = segments.read();
            segments_read
                .appendable_segments_ids()
                .into_iter()
                .filter_map(|segment_id| segments_read.get(segment_id))
                .all(|segment| {
                    let max_vector_size_bytes = segment
                        .get()
                        .read()
                        .max_available_vectors_size_in_bytes()
                        .unwrap_or_default();
                    let max_segment_size_bytes = thresholds_config
                        .max_segment_size_kb
                        .saturating_mul(segment::common::BYTES_IN_KB);

                    max_vector_size_bytes >= max_segment_size_bytes
                })
        };

        if no_segment_with_capacity {
            log::debug!("Creating new appendable segment, all existing segments are over capacity");

            let segment_config = collection_params
                .to_base_segment_config(collection_quantization)
                .map_err(|err| OperationError::service_error(err.to_string()))?;

            segments.write().create_appendable_segment(
                segments_path,
                segment_config,
                payload_index_schema,
            )?;
        }

        Ok(())
    }

    /// Trigger optimizers when CPU budget is available
    fn trigger_optimizers_on_resource_budget(
        optimizer_resource_budget: ResourceBudget,
        desired_cpus: usize,
        desired_io: usize,
        sender: Sender<OptimizerSignal>,
    ) -> JoinHandle<()> {
        task::spawn(async move {
            log::trace!("Skipping optimization checks, waiting for CPU budget to be available");
            optimizer_resource_budget
                .notify_on_budget_available(desired_cpus, desired_io)
                .await;
            log::trace!("Continue optimization checks, new CPU budget available");

            // Trigger optimizers with Nop operation
            sender.send(OptimizerSignal::Nop).await.unwrap_or_else(|_| {
                log::info!("Can't notify optimizers, assume process is dead. Restart is required")
            });
        })
    }

    /// Checks if there are any failed operations.
    /// If so - attempts to re-apply all failed operations.
    async fn try_recover(
        segments: LockedSegmentHolder,
        wal: LockedWal,
        update_operation_lock: Arc<tokio::sync::RwLock<()>>,
        update_tracker: UpdateTracker,
    ) -> CollectionResult<usize> {
        // Try to re-apply everything starting from the first failed operation
        let first_failed_operation_option = segments.read().failed_operation.iter().cloned().min();
        match first_failed_operation_option {
            None => {}
            Some(first_failed_op) => {
                let wal_lock = wal.lock().await;
                for (op_num, operation) in wal_lock.read(first_failed_op) {
                    CollectionUpdater::update(
                        &segments,
                        op_num,
                        operation.operation,
                        update_operation_lock.clone(),
                        update_tracker.clone(),
                        &HardwareCounterCell::disposable(), // Internal operation, no measurement needed
                    )?;
                }
            }
        };
        Ok(0)
    }
}
