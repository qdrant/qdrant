pub mod clock_map;
pub mod disk_usage_watcher;
pub(super) mod facet;
pub(super) mod query;
pub(super) mod scroll;
pub(super) mod search;
pub(super) mod shard_ops;

use std::collections::{BTreeSet, HashMap};
use std::mem::size_of;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use common::cpu::CpuBudget;
use common::rate_limiting::RateLimiter;
use common::types::TelemetryDetail;
use common::{panic, tar_ext};
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use parking_lot::{Mutex as ParkingMutex, RwLock};
use segment::data_types::vectors::VectorElementType;
use segment::entry::entry_point::SegmentEntry as _;
use segment::index::field_index::CardinalityEstimation;
use segment::segment::Segment;
use segment::segment_constructor::{build_segment, load_segment};
use segment::types::{
    CompressionRatio, Filter, PayloadIndexInfo, PayloadKeyType, PointIdType, QuantizationConfig,
    SegmentConfig, SegmentType, SnapshotFormat,
};
use segment::utils::mem::Mem;
use segment::vector_storage::common::get_async_scorer;
use tokio::fs::{create_dir_all, remove_dir_all, remove_file};
use tokio::runtime::Handle;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock as TokioRwLock};
use wal::{Wal, WalOptions};

use self::clock_map::{ClockMap, RecoveryPoint};
use self::disk_usage_watcher::DiskUsageWatcher;
use super::update_tracker::UpdateTracker;
use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentHolder,
};
use crate::collection_manager::optimizers::TrackerLog;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::common::file_utils::{move_dir, move_file};
use crate::config::CollectionConfigInternal;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{
    check_sparse_compatible_with_segment_config, CollectionError, CollectionResult,
    OptimizersStatus, ShardInfoInternal, ShardStatus,
};
use crate::operations::OperationWithClockTag;
use crate::optimizers_builder::{build_optimizers, clear_temp_segments, OptimizersConfig};
use crate::save_on_disk::SaveOnDisk;
use crate::shards::shard::ShardId;
use crate::shards::shard_config::ShardConfig;
use crate::shards::telemetry::{LocalShardTelemetry, OptimizerTelemetry};
use crate::shards::CollectionId;
use crate::update_handler::{Optimizer, UpdateHandler, UpdateSignal};
use crate::wal::SerdeWal;
use crate::wal_delta::{LockedWal, RecoverableWal};

/// If rendering WAL load progression in basic text form, report progression every 60 seconds.
const WAL_LOAD_REPORT_EVERY: Duration = Duration::from_secs(60);

const WAL_PATH: &str = "wal";

const SEGMENTS_PATH: &str = "segments";

/// LocalShard
///
/// LocalShard is an entity that can be moved between peers and contains some part of one collections data.
///
/// Holds all object, required for collection functioning
pub struct LocalShard {
    pub(super) segments: LockedSegmentHolder,
    pub(super) collection_config: Arc<TokioRwLock<CollectionConfigInternal>>,
    pub(super) shared_storage_config: Arc<SharedStorageConfig>,
    pub(crate) payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
    pub(super) wal: RecoverableWal,
    pub(super) update_handler: Arc<Mutex<UpdateHandler>>,
    pub(super) update_sender: ArcSwap<Sender<UpdateSignal>>,
    pub(super) update_tracker: UpdateTracker,
    pub(super) path: PathBuf,
    pub(super) optimizers: Arc<Vec<Arc<Optimizer>>>,
    pub(super) optimizers_log: Arc<ParkingMutex<TrackerLog>>,
    pub(super) total_optimized_points: Arc<AtomicUsize>,
    update_runtime: Handle,
    pub(super) search_runtime: Handle,
    disk_usage_watcher: DiskUsageWatcher,
    read_rate_limiter: ParkingMutex<Option<RateLimiter>>,
    write_rate_limiter: ParkingMutex<Option<RateLimiter>>,
}

/// Shard holds information about segments and WAL.
impl LocalShard {
    pub async fn move_data(from: &Path, to: &Path) -> CollectionResult<()> {
        let wal_from = Self::wal_path(from);
        let wal_to = Self::wal_path(to);
        let segments_from = Self::segments_path(from);
        let segments_to = Self::segments_path(to);

        move_dir(wal_from, wal_to).await?;
        move_dir(segments_from, segments_to).await?;

        LocalShardClocks::move_data(from, to).await?;

        Ok(())
    }

    /// Checks if path have local shard data present
    pub fn check_data(shard_path: &Path) -> bool {
        let wal_path = Self::wal_path(shard_path);
        let segments_path = Self::segments_path(shard_path);
        wal_path.exists() && segments_path.exists()
    }

    /// Clear local shard related data.
    ///
    /// Do NOT remove config file.
    pub async fn clear(shard_path: &Path) -> CollectionResult<()> {
        // Delete WAL
        let wal_path = Self::wal_path(shard_path);
        if wal_path.exists() {
            remove_dir_all(wal_path).await?;
        }

        // Delete segments
        let segments_path = Self::segments_path(shard_path);
        if segments_path.exists() {
            remove_dir_all(segments_path).await?;
        }

        LocalShardClocks::delete_data(shard_path).await?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        segment_holder: SegmentHolder,
        collection_config: Arc<TokioRwLock<CollectionConfigInternal>>,
        shared_storage_config: Arc<SharedStorageConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        wal: SerdeWal<OperationWithClockTag>,
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        optimizer_cpu_budget: CpuBudget,
        shard_path: &Path,
        clocks: LocalShardClocks,
        update_runtime: Handle,
        search_runtime: Handle,
    ) -> Self {
        let segment_holder = Arc::new(RwLock::new(segment_holder));
        let config = collection_config.read().await;
        let locked_wal = Arc::new(ParkingMutex::new(wal));
        let optimizers_log = Arc::new(ParkingMutex::new(Default::default()));
        let total_optimized_points = Arc::new(AtomicUsize::new(0));

        // default to 2x the WAL capacity
        let disk_buffer_threshold_mb =
            2 * (collection_config.read().await.wal_config.wal_capacity_mb);

        let disk_usage_watcher = disk_usage_watcher::DiskUsageWatcher::new(
            shard_path.to_owned(),
            disk_buffer_threshold_mb,
        )
        .await;

        let mut update_handler = UpdateHandler::new(
            shared_storage_config.clone(),
            payload_index_schema.clone(),
            optimizers.clone(),
            optimizers_log.clone(),
            total_optimized_points.clone(),
            optimizer_cpu_budget.clone(),
            update_runtime.clone(),
            segment_holder.clone(),
            locked_wal.clone(),
            config.optimizer_config.flush_interval_sec,
            config.optimizer_config.max_optimization_threads,
            clocks.clone(),
            shard_path.into(),
        );

        let (update_sender, update_receiver) =
            mpsc::channel(shared_storage_config.update_queue_size);
        update_handler.run_workers(update_receiver);

        let update_tracker = segment_holder.read().update_tracker();

        let read_rate_limiter = config.strict_mode_config.as_ref().and_then(|strict_mode| {
            strict_mode
                .read_rate_limit_per_sec
                .map(RateLimiter::with_rate_per_sec)
        });
        let read_rate_limiter = ParkingMutex::new(read_rate_limiter);

        let write_rate_limiter = config.strict_mode_config.as_ref().and_then(|strict_mode| {
            strict_mode
                .write_rate_limit_per_sec
                .map(RateLimiter::with_rate_per_sec)
        });
        let write_rate_limiter = ParkingMutex::new(write_rate_limiter);

        drop(config); // release `shared_config` from borrow checker

        Self {
            segments: segment_holder,
            collection_config,
            shared_storage_config,
            payload_index_schema,
            wal: RecoverableWal::new(locked_wal, clocks.newest_clocks, clocks.oldest_clocks),
            update_handler: Arc::new(Mutex::new(update_handler)),
            update_sender: ArcSwap::from_pointee(update_sender),
            update_tracker,
            path: shard_path.to_owned(),
            update_runtime,
            search_runtime,
            optimizers,
            optimizers_log,
            total_optimized_points,
            disk_usage_watcher,
            read_rate_limiter,
            write_rate_limiter,
        }
    }

    pub(super) fn segments(&self) -> &RwLock<SegmentHolder> {
        self.segments.deref()
    }

    /// Recovers shard from disk.
    #[allow(clippy::too_many_arguments)]
    pub async fn load(
        id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        collection_config: Arc<TokioRwLock<CollectionConfigInternal>>,
        effective_optimizers_config: OptimizersConfig,
        shared_storage_config: Arc<SharedStorageConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        update_runtime: Handle,
        search_runtime: Handle,
        optimizer_cpu_budget: CpuBudget,
    ) -> CollectionResult<LocalShard> {
        let collection_config_read = collection_config.read().await;

        let wal_path = Self::wal_path(shard_path);
        let segments_path = Self::segments_path(shard_path);

        let wal: SerdeWal<OperationWithClockTag> = SerdeWal::new(
            wal_path.to_str().unwrap(),
            (&collection_config_read.wal_config).into(),
        )
        .map_err(|e| CollectionError::service_error(format!("Wal error: {e}")))?;

        let segment_dirs = std::fs::read_dir(&segments_path).map_err(|err| {
            CollectionError::service_error(format!(
                "Can't read segments directory due to {}\nat {}",
                err,
                segments_path.to_str().unwrap()
            ))
        })?;

        let mut load_handlers = vec![];

        // This semaphore is used to limit the number of threads that load segments concurrently.
        // Uncomment it if you need to debug segment loading.
        // let semaphore = Arc::new(parking_lot::Mutex::new(()));

        for entry in segment_dirs {
            let segments_path = entry.unwrap().path();
            let payload_index_schema = payload_index_schema.clone();
            // let semaphore_clone = semaphore.clone();
            load_handlers.push(
                thread::Builder::new()
                    .name(format!("shard-load-{collection_id}-{id}"))
                    .spawn(move || {
                        // let _guard = semaphore_clone.lock();
                        let mut res = load_segment(&segments_path, &AtomicBool::new(false))?;
                        if let Some(segment) = &mut res {
                            segment.check_consistency_and_repair()?;
                            segment.update_all_field_indices(
                                &payload_index_schema.read().schema.clone(),
                            )?;
                        } else {
                            std::fs::remove_dir_all(&segments_path).map_err(|err| {
                                CollectionError::service_error(format!(
                                    "Can't remove leftover segment {}, due to {err}",
                                    segments_path.to_str().unwrap(),
                                ))
                            })?;
                        }
                        Ok::<_, CollectionError>(res)
                    })?,
            );
        }

        let mut segment_holder = SegmentHolder::default();

        for handler in load_handlers {
            let segment = handler.join().map_err(|err| {
                CollectionError::service_error(format!(
                    "Can't join segment load thread: {:?}",
                    err.type_id()
                ))
            })??;

            let Some(segment) = segment else {
                continue;
            };

            collection_config_read
                .params
                .vectors
                .check_compatible_with_segment_config(&segment.config().vector_data, true)?;
            collection_config_read
                .params
                .sparse_vectors
                .as_ref()
                .map(|sparse_vectors| {
                    check_sparse_compatible_with_segment_config(
                        sparse_vectors,
                        &segment.config().sparse_vector_data,
                        true,
                    )
                })
                .unwrap_or(Ok(()))?;

            segment_holder.add_new(segment);
        }

        let res = segment_holder.deduplicate_points().await?;
        if res > 0 {
            log::debug!("Deduplicated {} points", res);
        }

        clear_temp_segments(shard_path);
        let optimizers = build_optimizers(
            shard_path,
            &collection_config_read.params,
            &effective_optimizers_config,
            &collection_config_read.hnsw_config,
            &collection_config_read.quantization_config,
        );

        drop(collection_config_read); // release `shared_config` from borrow checker

        let clocks = LocalShardClocks::load(shard_path)?;

        // Always make sure we have any appendable segments, needed for update operations
        if !segment_holder.has_appendable_segment() {
            debug_assert!(
                false,
                "Shard has no appendable segments, this should never happen",
            );
            log::warn!("Shard has no appendable segments, this should never happen. Creating new appendable segment now");
            let segments_path = LocalShard::segments_path(shard_path);
            let collection_params = collection_config.read().await.params.clone();
            let payload_index_schema = payload_index_schema.read();
            segment_holder.create_appendable_segment(
                &segments_path,
                &collection_params,
                &payload_index_schema,
            )?;
        }

        let local_shard = LocalShard::new(
            segment_holder,
            collection_config,
            shared_storage_config,
            payload_index_schema,
            wal,
            optimizers,
            optimizer_cpu_budget,
            shard_path,
            clocks,
            update_runtime,
            search_runtime,
        )
        .await;

        // Apply outstanding operations from WAL
        local_shard.load_from_wal(collection_id).await?;

        let available_memory_bytes = Mem::new().available_memory_bytes() as usize;
        let vectors_size_bytes = local_shard.estimate_vector_data_size().await;

        // Simple heuristic to exclude mmap prefaulting for collections that won't benefit from it.
        //
        // We assume that mmap prefaulting is beneficial if we can put significant part of data
        // into RAM in advance. However, if we can see that the data is too big to fit into RAM,
        // it is better to avoid prefaulting, because it will only cause extra disk IO.
        //
        // This heuristic is not perfect, but it exclude cases when we don't have enough RAM
        // even to store half of the vector data.
        let do_mmap_prefault = available_memory_bytes * 2 > vectors_size_bytes;

        if do_mmap_prefault {
            for (_, segment) in local_shard.segments.read().iter() {
                if let LockedSegment::Original(segment) = segment {
                    segment.read().prefault_mmap_pages();
                }
            }
        }

        Ok(local_shard)
    }

    pub fn shard_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn wal_path(shard_path: &Path) -> PathBuf {
        shard_path.join(WAL_PATH)
    }

    pub fn segments_path(shard_path: &Path) -> PathBuf {
        shard_path.join(SEGMENTS_PATH)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn build_local(
        id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        collection_config: Arc<TokioRwLock<CollectionConfigInternal>>,
        shared_storage_config: Arc<SharedStorageConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        update_runtime: Handle,
        search_runtime: Handle,
        optimizer_cpu_budget: CpuBudget,
        effective_optimizers_config: OptimizersConfig,
    ) -> CollectionResult<LocalShard> {
        // initialize local shard config file
        let local_shard_config = ShardConfig::new_replica_set();
        let shard = Self::build(
            id,
            collection_id,
            shard_path,
            collection_config,
            shared_storage_config,
            payload_index_schema,
            update_runtime,
            search_runtime,
            optimizer_cpu_budget,
            effective_optimizers_config,
        )
        .await?;
        local_shard_config.save(shard_path)?;
        Ok(shard)
    }

    /// Creates new empty shard with given configuration, initializing all storages, optimizers and directories.
    #[allow(clippy::too_many_arguments)]
    pub async fn build(
        id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        collection_config: Arc<TokioRwLock<CollectionConfigInternal>>,
        shared_storage_config: Arc<SharedStorageConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        update_runtime: Handle,
        search_runtime: Handle,
        optimizer_cpu_budget: CpuBudget,
        effective_optimizers_config: OptimizersConfig,
    ) -> CollectionResult<LocalShard> {
        let config = collection_config.read().await;

        let wal_path = Self::wal_path(shard_path);

        create_dir_all(&wal_path).await.map_err(|err| {
            CollectionError::service_error(format!(
                "Can't create shard wal directory. Error: {err}"
            ))
        })?;

        let segments_path = Self::segments_path(shard_path);

        create_dir_all(&segments_path).await.map_err(|err| {
            CollectionError::service_error(format!(
                "Can't create shard segments directory. Error: {err}"
            ))
        })?;

        let mut segment_holder = SegmentHolder::default();
        let mut build_handlers = vec![];

        let vector_params = config.params.to_base_vector_data()?;
        let sparse_vector_params = config.params.to_sparse_vector_data()?;
        let segment_number = config.optimizer_config.get_number_segments();

        for _sid in 0..segment_number {
            let path_clone = segments_path.clone();
            let segment_config = SegmentConfig {
                vector_data: vector_params.clone(),
                sparse_vector_data: sparse_vector_params.clone(),
                payload_storage_type: config.params.payload_storage_type(),
            };
            let segment = thread::Builder::new()
                .name(format!("shard-build-{collection_id}-{id}"))
                .spawn(move || build_segment(&path_clone, &segment_config, true))
                .unwrap();
            build_handlers.push(segment);
        }

        let join_results = build_handlers
            .into_iter()
            .map(|handler| handler.join())
            .collect_vec();

        for join_result in join_results {
            let segment = join_result.map_err(|err| {
                let message = panic::downcast_str(&err).unwrap_or("");
                let separator = if !message.is_empty() { "with:\n" } else { "" };

                CollectionError::service_error(format!(
                    "Segment DB create panicked{separator}{message}",
                ))
            })??;

            segment_holder.add_new(segment);
        }

        let wal: SerdeWal<OperationWithClockTag> =
            SerdeWal::new(wal_path.to_str().unwrap(), (&config.wal_config).into())?;

        let optimizers = build_optimizers(
            shard_path,
            &config.params,
            &effective_optimizers_config,
            &config.hnsw_config,
            &config.quantization_config,
        );

        drop(config); // release `shared_config` from borrow checker

        let collection = LocalShard::new(
            segment_holder,
            collection_config,
            shared_storage_config,
            payload_index_schema,
            wal,
            optimizers,
            optimizer_cpu_budget,
            shard_path,
            LocalShardClocks::default(),
            update_runtime,
            search_runtime,
        )
        .await;

        Ok(collection)
    }

    pub async fn stop_flush_worker(&self) {
        let mut update_handler = self.update_handler.lock().await;
        update_handler.stop_flush_worker()
    }

    pub async fn wait_update_workers_stop(&self) -> CollectionResult<()> {
        let mut update_handler = self.update_handler.lock().await;
        update_handler.wait_workers_stops().await
    }

    /// Loads latest collection operations from WAL
    pub async fn load_from_wal(&self, collection_id: CollectionId) -> CollectionResult<()> {
        let mut newest_clocks = self.wal.newest_clocks.lock().await;
        let wal = self.wal.wal.lock();
        let bar = ProgressBar::new(wal.len(false));

        let progress_style = ProgressStyle::default_bar()
            .template("{msg} [{elapsed_precise}] {wide_bar} {pos}/{len} (eta:{eta})")
            .expect("Failed to create progress style");
        bar.set_style(progress_style);

        log::debug!(
            "Recovering shard {:?} starting reading WAL from {}",
            &self.path,
            wal.first_index()
        );

        bar.set_message(format!("Recovering collection {collection_id}"));
        let segments = self.segments();

        // Fall back to basic text output if the progress bar is hidden (e.g. not a tty)
        let show_progress_bar = !bar.is_hidden();
        let mut last_progress_report = Instant::now();
        if !show_progress_bar {
            log::info!(
                "Recovering collection {collection_id}: 0/{} (0%)",
                wal.len(false),
            );
        }

        // When `Segment`s are flushed, WAL is truncated up to the index of the last operation
        // that has been applied and flushed.
        //
        // `SerdeWal` wrapper persists/keeps track of this index (in addition to any handling
        // in the `wal` crate itself).
        //
        // `SerdeWal::read_all` starts reading WAL from the first "un-truncated" index,
        // so no additional handling required to "skip" any potentially applied entries.
        //
        // Note, that it's not guaranteed that some operation won't be re-applied to the storage.
        // (`SerdeWal::read_all` may even start reading WAL from some already truncated
        // index *occasionally*), but the storage can handle it.

        for (op_num, update) in wal.read_all(false) {
            if let Some(clock_tag) = update.clock_tag {
                newest_clocks.advance_clock(clock_tag);
            }

            // Propagate `CollectionError::ServiceError`, but skip other error types.
            match &CollectionUpdater::update(segments, op_num, update.operation) {
                Err(err @ CollectionError::ServiceError { error, backtrace }) => {
                    let path = self.path.display();

                    log::error!(
                        "Can't apply WAL operation: {error}, \
                         collection: {collection_id}, \
                         shard: {path}, \
                         op_num: {op_num}"
                    );

                    if let Some(backtrace) = &backtrace {
                        log::error!("Backtrace: {}", backtrace);
                    }

                    return Err(err.clone());
                }
                Err(err @ CollectionError::OutOfMemory { .. }) => {
                    log::error!("{err}");
                    return Err(err.clone());
                }
                Err(err @ CollectionError::NotFound { .. }) => log::warn!("{err}"),
                Err(err) => log::error!("{err}"),
                Ok(_) => (),
            }

            // Update progress bar or show text progress every WAL_LOAD_REPORT_EVERY
            bar.inc(1);
            if !show_progress_bar && last_progress_report.elapsed() >= WAL_LOAD_REPORT_EVERY {
                let progress = bar.position();
                log::info!(
                    "{progress}/{} ({}%)",
                    wal.len(false),
                    (progress as f32 / wal.len(false) as f32 * 100.0) as usize,
                );
                last_progress_report = Instant::now();
            }
        }

        {
            let segments = self.segments.read();

            // It is possible, that after recovery, if WAL flush was not enforced.
            // We could be left with some un-versioned points.
            // To maintain consistency, we can either remove them or try to recover.
            for (_idx, segment) in segments.iter() {
                match segment {
                    LockedSegment::Original(raw_segment) => {
                        raw_segment.write().cleanup_versions()?;
                    }
                    LockedSegment::Proxy(_) => {
                        debug_assert!(false, "Proxy segment found in load_from_wal");
                    }
                }
            }

            // Force a flush after re-applying WAL operations, to ensure we maintain on-disk data
            // consistency, if we happened to only apply *past* operations to a segment with newer
            // version.
            segments.flush_all(true, true)?;
        }

        bar.finish();
        if !show_progress_bar {
            log::info!(
                "Recovered collection {collection_id}: {0}/{0} (100%)",
                wal.len(false),
            );
        }

        // The storage is expected to be consistent after WAL recovery
        #[cfg(feature = "data-consistency-check")]
        self.check_data_consistency()?;

        Ok(())
    }

    /// Check data consistency for all segments
    ///
    /// Returns an error at the first inconsistent segment
    pub fn check_data_consistency(&self) -> CollectionResult<()> {
        log::info!("Checking data consistency for shard {:?}", self.path);
        let segments = self.segments.read();
        for (_idx, segment) in segments.iter() {
            match segment {
                LockedSegment::Original(raw_segment) => {
                    let segment_guard = raw_segment.read();
                    if let Err(err) = segment_guard.check_data_consistency() {
                        log::error!(
                            "Segment {:?} is inconsistent: {}",
                            segment_guard.current_path,
                            err
                        );
                        return Err(err.into());
                    }
                }
                LockedSegment::Proxy(_) => {
                    return Err(CollectionError::service_error(
                        "Proxy segment found in check_data_consistency",
                    ));
                }
            }
        }
        Ok(())
    }

    pub async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        let config = self.collection_config.read().await;
        let mut update_handler = self.update_handler.lock().await;

        let (update_sender, update_receiver) =
            mpsc::channel(self.shared_storage_config.update_queue_size);
        // makes sure that the Stop signal is the last one in this channel
        let old_sender = self.update_sender.swap(Arc::new(update_sender));
        old_sender.send(UpdateSignal::Stop).await?;
        update_handler.stop_flush_worker();

        update_handler.wait_workers_stops().await?;
        let new_optimizers = build_optimizers(
            &self.path,
            &config.params,
            &config.optimizer_config,
            &config.hnsw_config,
            &config.quantization_config,
        );
        update_handler.optimizers = new_optimizers;
        update_handler.flush_interval_sec = config.optimizer_config.flush_interval_sec;
        update_handler.max_optimization_threads = config.optimizer_config.max_optimization_threads;
        update_handler.run_workers(update_receiver);

        self.update_sender.load().send(UpdateSignal::Nop).await?;

        Ok(())
    }

    /// Apply shard's strict mode configuration update
    /// - Update read and write rate limiters
    pub async fn on_strict_mode_config_update(&self) {
        let config = self.collection_config.read().await;

        if let Some(strict_mode_config) = &config.strict_mode_config {
            // Update read rate limiter
            if let Some(read_rate_limit_per_sec) = strict_mode_config.read_rate_limit_per_sec {
                let mut read_rate_limiter_guard = self.read_rate_limiter.lock();
                read_rate_limiter_guard
                    .replace(RateLimiter::with_rate_per_sec(read_rate_limit_per_sec));
            }

            // update write rate limiter
            if let Some(write_rate_limit_per_sec) = strict_mode_config.write_rate_limit_per_sec {
                let mut write_rate_limiter_guard = self.write_rate_limiter.lock();
                write_rate_limiter_guard
                    .replace(RateLimiter::with_rate_per_sec(write_rate_limit_per_sec));
            }
        }
    }

    pub fn trigger_optimizers(&self) {
        // Send a trigger signal and ignore errors because all error cases are acceptable:
        // - If receiver is already dead - we do not care
        // - If channel is full - optimization will be triggered by some other signal
        let _ = self.update_sender.load().try_send(UpdateSignal::Nop);
    }

    /// Finishes ongoing update tasks
    pub async fn stop_gracefully(&self) {
        if let Err(err) = self.update_sender.load().send(UpdateSignal::Stop).await {
            log::warn!("Error sending stop signal to update handler: {}", err);
        }

        self.stop_flush_worker().await;

        if let Err(err) = self.wait_update_workers_stop().await {
            log::warn!("Update workers failed with: {}", err);
        }
    }

    pub fn restore_snapshot(snapshot_path: &Path) -> CollectionResult<()> {
        // Read dir first as the directory contents would change during restore.
        let entries = std::fs::read_dir(LocalShard::segments_path(snapshot_path))?
            .collect::<Result<Vec<_>, _>>()?;

        for entry in entries {
            Segment::restore_snapshot_in_place(&entry.path())?;
        }

        Ok(())
    }

    /// Create snapshot for local shard into `target_path`
    pub async fn create_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        save_wal: bool,
    ) -> CollectionResult<()> {
        let segments = self.segments.clone();
        let wal = self.wal.wal.clone();

        if !save_wal {
            // If we are not saving WAL, we still need to make sure that all submitted by this point
            // updates have made it to the segments. So we use the Plunger to achieve that.
            // It will notify us when all submitted updates so far have been processed.
            let (tx, rx) = oneshot::channel();
            let plunger = UpdateSignal::Plunger(tx);
            self.update_sender.load().send(plunger).await?;
            rx.await?;
        }

        let segments_path = Self::segments_path(&self.path);
        let collection_params = self.collection_config.read().await.params.clone();
        let temp_path = temp_path.to_owned();
        let payload_index_schema = self.payload_index_schema.clone();

        let tar_c = tar.clone();
        tokio::task::spawn_blocking(move || {
            // Do not change segments while snapshotting
            SegmentHolder::snapshot_all_segments(
                segments.clone(),
                &segments_path,
                Some(&collection_params),
                &payload_index_schema.read().clone(),
                &temp_path,
                &tar_c.descend(Path::new(SEGMENTS_PATH))?,
                format,
            )?;

            if save_wal {
                // snapshot all shard's WAL
                Self::snapshot_wal(wal, &tar_c)
            } else {
                Self::snapshot_empty_wal(wal, &temp_path, &tar_c)
            }
        })
        .await??;

        LocalShardClocks::archive_data(&self.path, tar).await?;

        Ok(())
    }

    /// Create empty WAL which is compatible with currently stored data
    pub fn snapshot_empty_wal(
        wal: LockedWal,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
    ) -> CollectionResult<()> {
        let (segment_capacity, latest_op_num) = {
            let wal_guard = wal.lock();
            (wal_guard.segment_capacity(), wal_guard.last_index())
        };

        let temp_dir = tempfile::tempdir_in(temp_path).map_err(|err| {
            CollectionError::service_error(format!(
                "Can not create temporary directory for WAL: {err}",
            ))
        })?;

        Wal::generate_empty_wal_starting_at_index(
            temp_dir.path(),
            &WalOptions {
                segment_capacity,
                segment_queue_len: 0,
            },
            latest_op_num,
        )
        .map_err(|err| {
            CollectionError::service_error(format!("Error while create empty WAL: {err}"))
        })?;

        tar.blocking_append_dir_all(temp_dir.path(), Path::new(WAL_PATH))
            .map_err(|err| {
                CollectionError::service_error(format!("Error while archiving WAL: {err}"))
            })
    }

    /// snapshot WAL
    pub fn snapshot_wal(wal: LockedWal, tar: &tar_ext::BuilderExt) -> CollectionResult<()> {
        // lock wal during snapshot
        let mut wal_guard = wal.lock();
        wal_guard.flush()?;
        let source_wal_path = wal_guard.path();

        let tar = tar.descend(Path::new(WAL_PATH))?;
        for entry in std::fs::read_dir(source_wal_path).map_err(|err| {
            CollectionError::service_error(format!("Can't read WAL directory: {err}",))
        })? {
            let entry = entry.map_err(|err| {
                CollectionError::service_error(format!("Can't read WAL directory: {err}",))
            })?;

            if entry.file_name() == ".wal" {
                // This sentinel file is used for WAL locking. Trying to archive
                // or open it will cause the following error on Windows:
                // > The process cannot access the file because another process
                // > has locked a portion of the file. (os error 33)
                // https://github.com/qdrant/wal/blob/7c9202d0874/src/lib.rs#L125-L145
                continue;
            }

            tar.blocking_append_file(&entry.path(), Path::new(&entry.file_name()))
                .map_err(|err| {
                    CollectionError::service_error(format!("Error while archiving WAL: {err}"))
                })?;
        }
        Ok(())
    }

    pub fn estimate_cardinality<'a>(
        &'a self,
        filter: Option<&'a Filter>,
    ) -> CollectionResult<CardinalityEstimation> {
        let segments = self.segments().read();
        let cardinality = segments
            .iter()
            .map(|(_id, segment)| segment.get().read().estimate_point_count(filter))
            .fold(CardinalityEstimation::exact(0), |acc, x| {
                CardinalityEstimation {
                    primary_clauses: vec![],
                    min: acc.min + x.min,
                    exp: acc.exp + x.exp,
                    max: acc.max + x.max,
                }
            });
        Ok(cardinality)
    }

    pub async fn read_filtered<'a>(
        &'a self,
        filter: Option<&'a Filter>,
        runtime_handle: &Handle,
    ) -> CollectionResult<BTreeSet<PointIdType>> {
        let segments = self.segments.clone();
        SegmentsSearcher::read_filtered(segments, filter, runtime_handle).await
    }

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> LocalShardTelemetry {
        let segments_read_guard = self.segments.read();
        let segments: Vec<_> = segments_read_guard
            .iter()
            .map(|(_id, segment)| segment.get().read().get_telemetry_data(detail))
            .collect();

        let optimizer_status = match &segments_read_guard.optimizer_errors {
            None => OptimizersStatus::Ok,
            Some(error) => OptimizersStatus::Error(error.to_string()),
        };
        drop(segments_read_guard);
        let optimizations = self
            .optimizers
            .iter()
            .map(|optimizer| {
                optimizer
                    .get_telemetry_counter()
                    .lock()
                    .get_statistics(detail)
            })
            .fold(Default::default(), |acc, x| acc + x);

        let total_optimized_points = self.total_optimized_points.load(Ordering::Relaxed);

        LocalShardTelemetry {
            variant_name: None,
            status: None,
            total_optimized_points,
            segments,
            optimizations: OptimizerTelemetry {
                status: optimizer_status,
                optimizations,
                log: self.optimizers_log.lock().to_telemetry(),
            },
            async_scorer: Some(get_async_scorer()),
        }
    }

    /// Returns estimated size of vector data in bytes
    async fn estimate_vector_data_size(&self) -> usize {
        let info = self.local_shard_info().await;

        let vector_size: usize = info
            .config
            .params
            .vectors
            .params_iter()
            .map(|(_, value)| {
                let vector_size = value.size.get() as usize;

                let quantization_config = value
                    .quantization_config
                    .as_ref()
                    .or(info.config.quantization_config.as_ref());

                let quantized_size_bytes = match quantization_config {
                    None => 0,
                    Some(QuantizationConfig::Scalar(_)) => vector_size,
                    Some(QuantizationConfig::Product(pq)) => match pq.product.compression {
                        CompressionRatio::X4 => vector_size,
                        CompressionRatio::X8 => vector_size / 2,
                        CompressionRatio::X16 => vector_size / 4,
                        CompressionRatio::X32 => vector_size / 8,
                        CompressionRatio::X64 => vector_size / 16,
                    },
                    Some(QuantizationConfig::Binary(_)) => vector_size / 8,
                };

                vector_size * size_of::<VectorElementType>() + quantized_size_bytes
            })
            .sum();

        vector_size * info.points_count
    }

    pub async fn local_shard_status(&self) -> (ShardStatus, OptimizersStatus) {
        {
            let segments = self.segments().read();

            // Red status on failed operation or optimizer error
            if !segments.failed_operation.is_empty() || segments.optimizer_errors.is_some() {
                let optimizer_status = segments
                    .optimizer_errors
                    .as_ref()
                    .map_or(OptimizersStatus::Ok, |err| {
                        OptimizersStatus::Error(err.to_string())
                    });
                return (ShardStatus::Red, optimizer_status);
            }

            // Yellow status if we have a special segment, indicates a proxy segment used during optimization
            // TODO: snapshotting also creates temp proxy segments. should differentiate.
            let has_special_segment = segments
                .iter()
                .map(|(_, segment)| segment.get().read().info().segment_type)
                .any(|segment_type| segment_type == SegmentType::Special);
            if has_special_segment {
                return (ShardStatus::Yellow, OptimizersStatus::Ok);
            }
        }

        // Yellow or grey status if there are pending optimizations
        // Grey if optimizers were not triggered yet after restart,
        // we don't automatically trigger them to prevent a crash loop
        let (has_triggered_any_optimizers, has_suboptimal_optimizers) = self
            .update_handler
            .lock()
            .await
            .check_optimizer_conditions();
        if has_suboptimal_optimizers {
            let status = if has_triggered_any_optimizers {
                ShardStatus::Yellow
            } else {
                ShardStatus::Grey
            };
            return (status, OptimizersStatus::Ok);
        }

        // Green status because everything is fine
        (ShardStatus::Green, OptimizersStatus::Ok)
    }

    pub async fn local_shard_info(&self) -> ShardInfoInternal {
        let collection_config = self.collection_config.read().await.clone();
        let mut vectors_count = 0;
        let mut indexed_vectors_count = 0;
        let mut points_count = 0;
        let mut segments_count = 0;
        let mut schema: HashMap<PayloadKeyType, PayloadIndexInfo> = Default::default();

        {
            let segments = self.segments().read();
            for (_idx, segment) in segments.iter() {
                segments_count += 1;

                let segment_info = segment.get().read().info();

                vectors_count += segment_info.num_vectors;
                indexed_vectors_count += segment_info.num_indexed_vectors;
                points_count += segment_info.num_points;
                for (key, val) in segment_info.index_schema {
                    schema
                        .entry(key)
                        .and_modify(|entry| entry.points += val.points)
                        .or_insert(val);
                }
            }
        }

        let (status, optimizer_status) = self.local_shard_status().await;

        ShardInfoInternal {
            status,
            optimizer_status,
            vectors_count,
            indexed_vectors_count,
            points_count,
            segments_count,
            config: collection_config,
            payload_schema: schema,
        }
    }

    pub fn update_tracker(&self) -> &UpdateTracker {
        &self.update_tracker
    }

    /// Get the recovery point for the current shard
    ///
    /// This is sourced from the last seen clocks from other nodes that we know about.
    pub async fn recovery_point(&self) -> RecoveryPoint {
        self.wal.recovery_point().await
    }

    /// Update the cutoff point on the current shard
    ///
    /// This also updates the highest seen clocks.
    pub async fn update_cutoff(&self, cutoff: &RecoveryPoint) {
        self.wal.update_cutoff(cutoff).await
    }

    /// Check if the write rate limiter allows the operation to proceed
    ///
    /// Returns an error if the rate limit is exceeded.
    fn check_write_rate_limiter(&self) -> CollectionResult<()> {
        if let Some(rate_limiter) = self.write_rate_limiter.lock().as_mut() {
            if !rate_limiter.check() {
                return Err(CollectionError::RateLimitExceeded {
                    description: "Write rate limit exceeded, retry later".to_string(),
                });
            }
        }
        Ok(())
    }

    /// Check if the read rate limiter allows the operation to proceed
    ///
    /// Returns an error if the rate limit is exceeded.
    fn check_read_rate_limiter(&self) -> CollectionResult<()> {
        if let Some(rate_limiter) = self.read_rate_limiter.lock().as_mut() {
            if !rate_limiter.check() {
                return Err(CollectionError::RateLimitExceeded {
                    description: "Read rate limit exceeded, retry later".to_string(),
                });
            }
        }
        Ok(())
    }
}

impl Drop for LocalShard {
    fn drop(&mut self) {
        thread::scope(|s| {
            let handle = thread::Builder::new()
                .name("drop-shard".to_string())
                .spawn_scoped(s, || {
                    // Needs dedicated thread to avoid `Cannot start a runtime from within a runtime` error.
                    self.update_runtime
                        .block_on(async { self.stop_gracefully().await })
                });
            handle.expect("Failed to create thread for shard drop");
        })
    }
}

const NEWEST_CLOCKS_PATH: &str = "newest_clocks.json";

const OLDEST_CLOCKS_PATH: &str = "oldest_clocks.json";

/// Convenience struct for combining clock maps belonging to a shard
///
/// Holds a clock map for tracking the highest clocks and the cutoff clocks.
#[derive(Clone, Debug, Default)]
pub struct LocalShardClocks {
    newest_clocks: Arc<Mutex<ClockMap>>,
    oldest_clocks: Arc<Mutex<ClockMap>>,
}

impl LocalShardClocks {
    fn new(newest_clocks: ClockMap, oldest_clocks: ClockMap) -> Self {
        Self {
            newest_clocks: Arc::new(Mutex::new(newest_clocks)),
            oldest_clocks: Arc::new(Mutex::new(oldest_clocks)),
        }
    }

    // Load clock maps from disk
    pub fn load(shard_path: &Path) -> CollectionResult<Self> {
        let newest_clocks = ClockMap::load_or_default(&Self::newest_clocks_path(shard_path))?;

        let oldest_clocks = ClockMap::load_or_default(&Self::oldest_clocks_path(shard_path))?;

        Ok(Self::new(newest_clocks, oldest_clocks))
    }

    /// Persist clock maps to disk
    pub async fn store_if_changed(&self, shard_path: &Path) -> CollectionResult<()> {
        self.oldest_clocks
            .lock()
            .await
            .store_if_changed(&Self::oldest_clocks_path(shard_path))?;

        self.newest_clocks
            .lock()
            .await
            .store_if_changed(&Self::newest_clocks_path(shard_path))?;

        Ok(())
    }

    /// Put clock data from the disk into an archive.
    pub async fn archive_data(from: &Path, tar: &tar_ext::BuilderExt) -> CollectionResult<()> {
        let newest_clocks_from = Self::newest_clocks_path(from);
        let oldest_clocks_from = Self::oldest_clocks_path(from);

        if newest_clocks_from.exists() {
            tar.append_file(&newest_clocks_from, Path::new(NEWEST_CLOCKS_PATH))
                .await?;
        }

        if oldest_clocks_from.exists() {
            tar.append_file(&oldest_clocks_from, Path::new(OLDEST_CLOCKS_PATH))
                .await?;
        }

        Ok(())
    }

    /// Move clock data on disk from one shard path to another.
    pub async fn move_data(from: &Path, to: &Path) -> CollectionResult<()> {
        let newest_clocks_from = Self::newest_clocks_path(from);
        let oldest_clocks_from = Self::oldest_clocks_path(from);

        if newest_clocks_from.exists() {
            let newest_clocks_to = Self::newest_clocks_path(to);
            move_file(newest_clocks_from, newest_clocks_to).await?;
        }

        if oldest_clocks_from.exists() {
            let oldest_clocks_to = Self::oldest_clocks_path(to);
            move_file(oldest_clocks_from, oldest_clocks_to).await?;
        }

        Ok(())
    }

    /// Delete clock data from disk at the given shard path.
    pub async fn delete_data(shard_path: &Path) -> CollectionResult<()> {
        let newest_clocks_path = Self::newest_clocks_path(shard_path);
        let oldest_clocks_path = Self::oldest_clocks_path(shard_path);

        if newest_clocks_path.exists() {
            remove_file(newest_clocks_path).await?;
        }

        if oldest_clocks_path.exists() {
            remove_file(oldest_clocks_path).await?;
        }

        Ok(())
    }

    fn newest_clocks_path(shard_path: &Path) -> PathBuf {
        shard_path.join(NEWEST_CLOCKS_PATH)
    }

    fn oldest_clocks_path(shard_path: &Path) -> PathBuf {
        shard_path.join(OLDEST_CLOCKS_PATH)
    }
}
