use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use parking_lot::Mutex;
use segment::types::SeqNumberType;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{self, Receiver};
use tokio::sync::{Mutex as TokioMutex, oneshot};
use tokio::task::JoinHandle;

use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::collection_manager::holders::segment_holder::LockedSegmentHolder;
use crate::collection_manager::optimizers::TrackerLog;
use crate::collection_manager::optimizers::segment_optimizer::{
    OptimizationPlanner, SegmentOptimizer,
};
use crate::common::stoppable_task::StoppableTaskHandle;
use crate::operations::CollectionUpdateOperations;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::CollectionResult;
use crate::shards::CollectionId;
use crate::shards::local_shard::LocalShardClocks;
use crate::shards::update_tracker::UpdateTracker;
use crate::update_workers::UpdateWorkers;
use crate::wal_delta::LockedWal;

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
    pub hw_measurements: HwMeasurementAcc,
}

/// Signal, used to inform Updater process
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
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
    collection_name: CollectionId,
    shared_storage_config: Arc<SharedStorageConfig>,
    payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
    /// List of used optimizers
    pub optimizers: Arc<Vec<Arc<Optimizer>>>,
    /// Log of optimizer statuses
    optimizers_log: Arc<Mutex<TrackerLog>>,
    /// Total number of optimized points since last start
    total_optimized_points: Arc<AtomicUsize>,
    /// Global CPU budget in number of cores for all optimization tasks.
    /// Assigns CPU permits to tasks to limit overall resource utilization.
    optimizer_resource_budget: ResourceBudget,
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
    /// Highest and cutoff clocks for the shard WAL.
    clocks: LocalShardClocks,
    shard_path: PathBuf,
    /// Whether we have ever triggered optimizers since starting.
    has_triggered_optimizers: Arc<AtomicBool>,

    /// Scroll read lock
    /// The lock, which must prevent updates during scroll + retrieve operations
    /// Consistency of scroll operations is especially important for internal processes like
    /// re-sharding and shard transfer, so explicit lock for those operations is required.
    ///
    /// Write lock must be held for updates, while read lock must be held for scroll
    scroll_read_lock: Arc<tokio::sync::RwLock<()>>,

    update_tracker: UpdateTracker,
}

impl UpdateHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        collection_name: CollectionId,
        shared_storage_config: Arc<SharedStorageConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        optimizers_log: Arc<Mutex<TrackerLog>>,
        total_optimized_points: Arc<AtomicUsize>,
        optimizer_resource_budget: ResourceBudget,
        runtime_handle: Handle,
        segments: LockedSegmentHolder,
        wal: LockedWal,
        flush_interval_sec: u64,
        max_optimization_threads: Option<usize>,
        clocks: LocalShardClocks,
        shard_path: PathBuf,
        scroll_read_lock: Arc<tokio::sync::RwLock<()>>,
        update_tracker: UpdateTracker,
    ) -> UpdateHandler {
        UpdateHandler {
            collection_name,
            shared_storage_config,
            payload_index_schema,
            optimizers,
            segments,
            update_worker: None,
            optimizer_worker: None,
            optimizers_log,
            total_optimized_points,
            optimizer_resource_budget,
            flush_worker: None,
            flush_stop: None,
            runtime_handle,
            wal,
            wal_keep_from: Arc::new(u64::MAX.into()),
            flush_interval_sec,
            optimization_handles: Arc::new(TokioMutex::new(vec![])),
            max_optimization_threads,
            clocks,
            shard_path,
            has_triggered_optimizers: Default::default(),
            scroll_read_lock,
            update_tracker,
        }
    }

    pub fn run_workers(&mut self, update_receiver: Receiver<UpdateSignal>) {
        let (tx, rx) = mpsc::channel(self.shared_storage_config.update_queue_size);

        self.optimizer_worker = Some(self.runtime_handle.spawn(
            UpdateWorkers::optimization_worker_fn(
                self.optimizers.clone(),
                tx.clone(),
                rx,
                self.segments.clone(),
                self.wal.clone(),
                self.optimization_handles.clone(),
                self.optimizers_log.clone(),
                self.total_optimized_points.clone(),
                self.optimizer_resource_budget.clone(),
                self.max_optimization_threads,
                self.has_triggered_optimizers.clone(),
                self.payload_index_schema.clone(),
                self.scroll_read_lock.clone(),
                self.update_tracker.clone(),
            ),
        ));

        let wal = self.wal.clone();
        let segments = self.segments.clone();
        let scroll_read_lock = self.scroll_read_lock.clone();
        let update_tracker = self.update_tracker.clone();
        let collection_name = self.collection_name.clone();
        self.update_worker = Some(self.runtime_handle.spawn(UpdateWorkers::update_worker_fn(
            collection_name,
            update_receiver,
            tx,
            wal,
            segments,
            scroll_read_lock,
            update_tracker,
        )));

        let segments = self.segments.clone();
        let wal = self.wal.clone();
        let wal_keep_from = self.wal_keep_from.clone();
        let clocks = self.clocks.clone();
        let flush_interval_sec = self.flush_interval_sec;
        let shard_path = self.shard_path.clone();
        let (flush_tx, flush_rx) = oneshot::channel();
        self.flush_worker = Some(self.runtime_handle.spawn(UpdateWorkers::flush_worker_fn(
            segments,
            wal,
            wal_keep_from,
            clocks,
            flush_interval_sec,
            flush_rx,
            shard_path,
        )));

        self.flush_stop = Some(flush_tx);
    }

    pub fn stop_flush_worker(&mut self) {
        if let Some(flush_stop) = self.flush_stop.take()
            && let Err(()) = flush_stop.send(())
        {
            log::warn!("Failed to stop flush worker as it is already stopped.");
        }
    }

    /// Notify optimization handles to stop *without* waiting
    ///
    /// Blocking operation
    pub fn notify_optimization_handles_to_stop(&self) {
        log::trace!("notify optimization handles to stop");
        let opt_handles_guard = self.optimization_handles.blocking_lock();
        for handle in opt_handles_guard.iter() {
            handle.ask_to_stop();
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

        for handle in opt_handles_guard.iter() {
            handle.ask_to_stop();
        }

        // If the await fails, we would still keep the rest of handles.
        while let Some(handle) = opt_handles_guard.pop() {
            if let Some(join_handle) = handle.stop() {
                join_handle.await?;
            }
        }
        Ok(())
    }

    /// Checks whether all update-related workers have stopped.
    pub fn is_stopped(&self) -> bool {
        self.update_worker.is_none()
            && self.optimizer_worker.is_none()
            && self.flush_worker.is_none()
            && self.optimization_handles.blocking_lock().is_empty()
    }

    /// Checks the optimizer conditions.
    ///
    /// This function returns a tuple of two booleans:
    /// - The first indicates if any optimizers have been triggered since startup.
    /// - The second indicates if there are any pending/suboptimal optimizers.
    pub(crate) fn check_optimizer_conditions(&self) -> (bool, bool) {
        // Check if Qdrant triggered any optimizations since starting at all
        let has_triggered_any_optimizers = self.has_triggered_optimizers.load(Ordering::Relaxed);

        let segments = self.segments.read();
        let mut planner = OptimizationPlanner::new(
            segments.running_optimizations.count(),
            segments.iter_original(),
        );
        let has_suboptimal_optimizers = self.optimizers.iter().any(|optimizer| {
            optimizer.plan_optimizations(&mut planner);
            !planner.scheduled().is_empty()
        });

        (has_triggered_any_optimizers, has_suboptimal_optimizers)
    }

    pub async fn store_clocks_if_changed(&self) -> CollectionResult<()> {
        let clocks = self.clocks.clone();
        let segments = self.segments.clone();
        let shard_path = self.shard_path.clone();

        self.runtime_handle
            .spawn_blocking(move || {
                if let Err(err) = clocks.store_if_changed(&shard_path) {
                    log::warn!("Failed to store clock maps to disk: {err}");
                    segments.write().report_optimizer_error(err);
                }
            })
            .await?;

        Ok(())
    }
}
