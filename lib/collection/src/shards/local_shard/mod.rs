pub mod clock_map;
pub mod disk_usage_watcher;
pub(super) mod facet;
pub(super) mod formula_rescore;
pub(super) mod query;
pub(super) mod scroll;
pub(super) mod search;
pub(super) mod shard_ops;
mod telemetry;

use std::collections::{BTreeSet, HashMap};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::thread;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::rate_limiting::RateLimiter;
use common::{panic, tar_ext};
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use parking_lot::{Mutex as ParkingMutex, RwLock};
use segment::data_types::segment_manifest::SegmentManifests;
use segment::entry::entry_point::SegmentEntry as _;
use segment::index::field_index::CardinalityEstimation;
use segment::segment::Segment;
use segment::segment_constructor::{build_segment, load_segment};
use segment::types::{
    Filter, PayloadIndexInfo, PayloadKeyType, PointIdType, SegmentConfig, SegmentType,
    SnapshotFormat,
};
use tokio::fs::{create_dir_all, remove_dir_all, remove_file};
use tokio::runtime::Handle;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, RwLock as TokioRwLock, mpsc, oneshot};
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
use crate::operations::OperationWithClockTag;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{
    CollectionError, CollectionResult, OptimizersStatus, ShardInfoInternal, ShardStatus,
    check_sparse_compatible_with_segment_config,
};
use crate::optimizers_builder::{OptimizersConfig, build_optimizers, clear_temp_segments};
use crate::save_on_disk::SaveOnDisk;
use crate::shards::CollectionId;
use crate::shards::shard::ShardId;
use crate::shards::shard_config::ShardConfig;
use crate::update_handler::{Optimizer, UpdateHandler, UpdateSignal};
use crate::wal::SerdeWal;
use crate::wal_delta::{LockedWal, RecoverableWal};

/// If rendering WAL load progression in basic text form, report progression every 60 seconds.
const WAL_LOAD_REPORT_EVERY: Duration = Duration::from_secs(60);

const WAL_PATH: &str = "wal";

const SEGMENTS_PATH: &str = "segments";

const NEWEST_CLOCKS_PATH: &str = "newest_clocks.json";

const OLDEST_CLOCKS_PATH: &str = "oldest_clocks.json";

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
    read_rate_limiter: Option<ParkingMutex<RateLimiter>>,
}

/// Shard holds information about segments and WAL.
impl LocalShard {
    /// Moves `wal`, `segments` and `clocks` data from one path to another.
    pub async fn move_data(from: &Path, to: &Path) -> CollectionResult<()> {
        log::debug!(
            "Moving local shard from {} to {}",
            from.display(),
            to.display()
        );

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
        optimizer_resource_budget: ResourceBudget,
        shard_path: &Path,
        clocks: LocalShardClocks,
        update_runtime: Handle,
        search_runtime: Handle,
    ) -> Self {
        let segment_holder = Arc::new(RwLock::new(segment_holder));
        let config = collection_config.read().await;
        let locked_wal = Arc::new(Mutex::new(wal));
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
            optimizer_resource_budget.clone(),
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
                .read_rate_limit
                .map(RateLimiter::new_per_minute)
                .map(ParkingMutex::new)
        });

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
        optimizer_resource_budget: ResourceBudget,
    ) -> CollectionResult<LocalShard> {
        let collection_config_read = collection_config.read().await;

        let wal_path = Self::wal_path(shard_path);
        let segments_path = Self::segments_path(shard_path);

        let wal: SerdeWal<OperationWithClockTag> = SerdeWal::new(
            wal_path.to_str().unwrap(),
            (&collection_config_read.wal_config).into(),
        )
        .map_err(|e| CollectionError::service_error(format!("Wal error: {e}")))?;

        // Walk over segments directory and collect all directory entries now
        // Collect now and error early to prevent errors while we've already spawned load threads
        let segment_paths = std::fs::read_dir(&segments_path)
            .map_err(|err| {
                CollectionError::service_error(format!(
                    "Can't read segments directory due to {err}\nat {}",
                    segments_path.display(),
                ))
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                CollectionError::service_error(format!(
                    "Failed to read segment path in segment directory: {err}",
                ))
            })?;

        // Grab segment paths, filter out hidden entries and non-directories
        let segment_paths = segment_paths
            .into_iter()
            .filter(|entry| {
                let is_hidden = entry
                    .file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with('.'));
                if is_hidden {
                    log::debug!(
                        "Segments path entry prefixed with a period, ignoring: {}",
                        entry.path().display(),
                    );
                }
                !is_hidden
            })
            .filter(|entry| {
                let is_dir = entry.path().is_dir();
                if !is_dir {
                    log::warn!(
                        "Segments path entry is not a directory, skipping: {}",
                        entry.path().display(),
                    );
                }
                is_dir
            })
            .map(|entry| entry.path());

        let mut load_handlers = vec![];

        // This semaphore is used to limit the number of threads that load segments concurrently.
        // Uncomment it if you need to debug segment loading.
        // let semaphore = Arc::new(parking_lot::Mutex::new(()));

        for segment_path in segment_paths {
            let payload_index_schema = payload_index_schema.clone();
            // let semaphore_clone = semaphore.clone();
            load_handlers.push(
                thread::Builder::new()
                    .name(format!("shard-load-{collection_id}-{id}"))
                    .spawn(move || {
                        // let _guard = semaphore_clone.lock();
                        let mut res = load_segment(&segment_path, &AtomicBool::new(false))?;
                        if let Some(segment) = &mut res {
                            segment.check_consistency_and_repair()?;
                            segment.update_all_field_indices(
                                &payload_index_schema.read().schema.clone(),
                            )?;
                        } else {
                            std::fs::remove_dir_all(&segment_path).map_err(|err| {
                                CollectionError::service_error(format!(
                                    "Can't remove leftover segment {}, due to {err}",
                                    segment_path.to_str().unwrap(),
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
            log::debug!("Deduplicated {res} points");
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
            log::warn!(
                "Shard has no appendable segments, this should never happen. Creating new appendable segment now",
            );
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
            optimizer_resource_budget,
            shard_path,
            clocks,
            update_runtime,
            search_runtime,
        )
        .await;

        // Apply outstanding operations from WAL
        local_shard.load_from_wal(collection_id).await?;

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
        optimizer_resource_budget: ResourceBudget,
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
            optimizer_resource_budget,
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
        optimizer_resource_budget: ResourceBudget,
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
            optimizer_resource_budget,
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
        let wal = self.wal.wal.lock().await;
        let bar = ProgressBar::new(wal.len(false));

        let progress_style = ProgressStyle::default_bar()
            .template("{msg} [{elapsed_precise}] {wide_bar} {pos}/{len} (eta:{eta})")
            .expect("Failed to create progress style");
        bar.set_style(progress_style);

        log::debug!(
            "Recovering shard {} starting reading WAL from {}",
            self.path.display(),
            wal.first_index(),
        );

        bar.set_message(format!("Recovering collection {collection_id}"));
        let segments = self.segments();

        // Fall back to basic text output if the progress bar is hidden (e.g. not a tty)
        let show_progress_bar = !bar.is_hidden();
        let mut last_progress_report = Instant::now();
        if !show_progress_bar {
            log::info!(
                "Recovering shard {}: 0/{} (0%)",
                self.path.display(),
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
            match &CollectionUpdater::update(
                segments,
                op_num,
                update.operation,
                &HardwareCounterCell::disposable(), // Internal operation, no measurement needed.
            ) {
                Err(err @ CollectionError::ServiceError { error, backtrace }) => {
                    let path = self.path.display();

                    log::error!(
                        "Can't apply WAL operation: {error}, \
                         collection: {collection_id}, \
                         shard: {path}, \
                         op_num: {op_num}"
                    );

                    if let Some(backtrace) = &backtrace {
                        log::error!("Backtrace: {backtrace}");
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
    /// - Update read rate limiter
    pub async fn on_strict_mode_config_update(&mut self) {
        let config = self.collection_config.read().await;

        if let Some(strict_mode_config) = &config.strict_mode_config {
            if strict_mode_config.enabled == Some(true) {
                // update read rate limiter
                if let Some(read_rate_limit_per_min) = strict_mode_config.read_rate_limit {
                    let new_read_rate_limiter =
                        RateLimiter::new_per_minute(read_rate_limit_per_min);
                    self.read_rate_limiter
                        .replace(parking_lot::Mutex::new(new_read_rate_limiter));
                    return;
                }
            }
        }
        // remove read rate limiter for all other situations
        self.read_rate_limiter.take();
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
            log::warn!("Error sending stop signal to update handler: {err}");
        }

        self.stop_flush_worker().await;

        if let Err(err) = self.wait_update_workers_stop().await {
            log::warn!("Update workers failed with: {err}");
        }
    }

    pub fn restore_snapshot(snapshot_path: &Path) -> CollectionResult<()> {
        log::info!("Restoring shard snapshot {}", snapshot_path.display());
        // Read dir first as the directory contents would change during restore
        let entries = std::fs::read_dir(LocalShard::segments_path(snapshot_path))?
            .collect::<Result<Vec<_>, _>>()?;

        // Filter out hidden entries
        let entries = entries.into_iter().filter(|entry| {
            let is_hidden = entry
                .file_name()
                .to_str()
                .is_some_and(|s| s.starts_with('.'));
            if is_hidden {
                log::debug!(
                    "Ignoring hidden segment in local shard during snapshot recovery: {}",
                    entry.path().display(),
                );
            }
            !is_hidden
        });

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
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    pub fn snapshot_empty_wal(
        wal: LockedWal,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
    ) -> CollectionResult<()> {
        let (segment_capacity, latest_op_num) = {
            let wal_guard = wal.blocking_lock();
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
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    pub fn snapshot_wal(wal: LockedWal, tar: &tar_ext::BuilderExt) -> CollectionResult<()> {
        // lock wal during snapshot
        let mut wal_guard = wal.blocking_lock();
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

    pub fn segment_manifests(&self) -> CollectionResult<SegmentManifests> {
        self.segments()
            .read()
            .segment_manifests()
            .map_err(CollectionError::from)
    }

    pub fn estimate_cardinality<'a>(
        &'a self,
        filter: Option<&'a Filter>,
        hw_counter: &HardwareCounterCell,
    ) -> CollectionResult<CardinalityEstimation> {
        let segments = self.segments().read();
        let cardinality = segments
            .iter()
            .map(|(_id, segment)| {
                segment
                    .get()
                    .read()
                    .estimate_point_count(filter, hw_counter)
            })
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
        hw_counter: HwMeasurementAcc,
    ) -> CollectionResult<BTreeSet<PointIdType>> {
        let segments = self.segments.clone();
        SegmentsSearcher::read_filtered(segments, filter, runtime_handle, hw_counter).await
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

    /// Check if the read rate limiter allows the operation to proceed
    /// - hw_measurement_acc: the current hardware measurement accumulator
    /// - context: the context of the operation to add on the error message
    /// - cost_fn: the cost of the operation called lazily
    ///
    /// Returns an error if the rate limit is exceeded.
    fn check_read_rate_limiter<F>(
        &self,
        hw_measurement_acc: &HwMeasurementAcc,
        context: &str,
        cost_fn: F,
    ) -> CollectionResult<()>
    where
        F: FnOnce() -> usize,
    {
        // Do not rate limit internal operation tagged with disposable measurement
        if hw_measurement_acc.is_disposable() {
            return Ok(());
        }
        if let Some(rate_limiter) = &self.read_rate_limiter {
            let cost = cost_fn();
            rate_limiter
                .lock()
                .try_consume(cost as f64)
                .map_err(|err| {
                    log::debug!("Read rate limit error on {context} with {err:?}");
                    CollectionError::rate_limit_error(err, cost, false)
                })?;
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
