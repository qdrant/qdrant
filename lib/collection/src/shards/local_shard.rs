use std::collections::{BTreeSet, HashMap};
use std::mem::size_of;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use arc_swap::ArcSwap;
use common::panic;
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use parking_lot::{Mutex as ParkingMutex, RwLock};
use segment::data_types::vectors::VectorElementType;
use segment::entry::entry_point::SegmentEntry as _;
use segment::index::field_index::CardinalityEstimation;
use segment::segment::Segment;
use segment::segment_constructor::{build_segment, load_segment};
use segment::types::{
    CompressionRatio, Filter, PayloadIndexInfo, PayloadKeyType, PayloadStorageType, PointIdType,
    QuantizationConfig, SegmentConfig, SegmentType,
};
use segment::utils::mem::Mem;
use tokio::fs::{copy, create_dir_all, remove_dir_all};
use tokio::runtime::Handle;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock as TokioRwLock};
use wal::{Wal, WalOptions};

use super::update_tracker::UpdateTracker;
use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};
use crate::collection_manager::optimizers::TrackerLog;
use crate::common::file_utils::move_dir;
use crate::config::CollectionConfig;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{
    check_sparse_compatible_with_segment_config, CollectionError, CollectionInfoInternal,
    CollectionResult, CollectionStatus, OptimizersStatus,
};
use crate::operations::CollectionUpdateOperations;
use crate::optimizers_builder::{build_optimizers, clear_temp_segments};
use crate::shards::shard::ShardId;
use crate::shards::shard_config::{ShardConfig, SHARD_CONFIG_FILE};
use crate::shards::telemetry::{LocalShardTelemetry, OptimizerTelemetry};
use crate::shards::CollectionId;
use crate::update_handler::{Optimizer, UpdateHandler, UpdateSignal};
use crate::wal::SerdeWal;

pub type LockedWal = Arc<ParkingMutex<SerdeWal<CollectionUpdateOperations>>>;

/// LocalShard
///
/// LocalShard is an entity that can be moved between peers and contains some part of one collections data.
///
/// Holds all object, required for collection functioning
pub struct LocalShard {
    pub(super) segments: Arc<RwLock<SegmentHolder>>,
    pub(super) collection_config: Arc<TokioRwLock<CollectionConfig>>,
    pub(super) shared_storage_config: Arc<SharedStorageConfig>,
    pub(super) wal: LockedWal,
    pub(super) update_handler: Arc<Mutex<UpdateHandler>>,
    pub(super) update_sender: ArcSwap<Sender<UpdateSignal>>,
    pub(super) update_tracker: UpdateTracker,
    pub(super) path: PathBuf,
    pub(super) optimizers: Arc<Vec<Arc<Optimizer>>>,
    pub(super) optimizers_log: Arc<ParkingMutex<TrackerLog>>,
    update_runtime: Handle,
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

        Ok(())
    }

    pub async fn new(
        segment_holder: SegmentHolder,
        collection_config: Arc<TokioRwLock<CollectionConfig>>,
        shared_storage_config: Arc<SharedStorageConfig>,
        wal: SerdeWal<CollectionUpdateOperations>,
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        shard_path: &Path,
        update_runtime: Handle,
    ) -> Self {
        let segment_holder = Arc::new(RwLock::new(segment_holder));
        let config = collection_config.read().await;
        let locked_wal = Arc::new(ParkingMutex::new(wal));
        let optimizers_log = Arc::new(ParkingMutex::new(Default::default()));

        let mut update_handler = UpdateHandler::new(
            shared_storage_config.clone(),
            optimizers.clone(),
            optimizers_log.clone(),
            update_runtime.clone(),
            segment_holder.clone(),
            locked_wal.clone(),
            config.optimizer_config.flush_interval_sec,
            config.optimizer_config.max_optimization_threads,
        );

        let (update_sender, update_receiver) =
            mpsc::channel(shared_storage_config.update_queue_size);
        update_handler.run_workers(update_receiver);

        let update_tracker = segment_holder.read().update_tracker();

        drop(config); // release `shared_config` from borrow checker

        Self {
            segments: segment_holder,
            collection_config,
            shared_storage_config,
            wal: locked_wal,
            update_handler: Arc::new(Mutex::new(update_handler)),
            update_sender: ArcSwap::from_pointee(update_sender),
            update_tracker,
            path: shard_path.to_owned(),
            update_runtime,
            optimizers,
            optimizers_log,
        }
    }

    pub(super) fn segments(&self) -> &RwLock<SegmentHolder> {
        self.segments.deref()
    }

    /// Recovers shard from disk.
    pub async fn load(
        id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        collection_config: Arc<TokioRwLock<CollectionConfig>>,
        shared_storage_config: Arc<SharedStorageConfig>,
        update_runtime: Handle,
    ) -> CollectionResult<LocalShard> {
        let collection_config_read = collection_config.read().await;

        let wal_path = Self::wal_path(shard_path);
        let segments_path = Self::segments_path(shard_path);
        let mut segment_holder = SegmentHolder::default();

        let wal: SerdeWal<CollectionUpdateOperations> = SerdeWal::new(
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

        for entry in segment_dirs {
            let segments_path = entry.unwrap().path();
            load_handlers.push(
                thread::Builder::new()
                    .name(format!("shard-load-{collection_id}-{id}"))
                    .spawn(move || {
                        let mut res = load_segment(&segments_path)?;
                        if let Some(segment) = &mut res {
                            segment.check_consistency_and_repair()?;
                        } else {
                            std::fs::remove_dir_all(&segments_path).map_err(|err| {
                                CollectionError::service_error(format!(
                                    "Can't remove leftover segment {}, due to {}",
                                    segments_path.to_str().unwrap(),
                                    err
                                ))
                            })?;
                        }
                        Ok::<_, CollectionError>(res)
                    })?,
            );
        }

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

            segment_holder.add(segment);
        }

        let res = segment_holder.deduplicate_points()?;
        if res > 0 {
            log::debug!("Deduplicated {} points", res);
        }

        clear_temp_segments(shard_path);
        let optimizers = build_optimizers(
            shard_path,
            &collection_config_read.params,
            &collection_config_read.optimizer_config,
            &collection_config_read.hnsw_config,
            &collection_config_read.quantization_config,
        );

        drop(collection_config_read); // release `shared_config` from borrow checker

        let collection = LocalShard::new(
            segment_holder,
            collection_config,
            shared_storage_config,
            wal,
            optimizers,
            shard_path,
            update_runtime,
        )
        .await;

        collection.load_from_wal(collection_id)?;

        let available_memory_bytes = Mem::new().available_memory_bytes() as usize;
        let vectors_size_bytes = collection.estimate_vector_data_size().await;

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
            for (_, segment) in collection.segments.read().iter() {
                if let LockedSegment::Original(segment) = segment {
                    segment.read().prefault_mmap_pages();
                }
            }
        }

        Ok(collection)
    }

    pub fn shard_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn wal_path(shard_path: &Path) -> PathBuf {
        shard_path.join("wal")
    }

    pub fn segments_path(shard_path: &Path) -> PathBuf {
        shard_path.join("segments")
    }

    pub async fn build_local(
        id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        collection_config: Arc<TokioRwLock<CollectionConfig>>,
        shared_storage_config: Arc<SharedStorageConfig>,
        update_runtime: Handle,
    ) -> CollectionResult<LocalShard> {
        // initialize local shard config file
        let local_shard_config = ShardConfig::new_replica_set();
        let shard = Self::build(
            id,
            collection_id,
            shard_path,
            collection_config,
            shared_storage_config,
            update_runtime,
        )
        .await?;
        local_shard_config.save(shard_path)?;
        Ok(shard)
    }

    /// Creates new empty shard with given configuration, initializing all storages, optimizers and directories.
    pub async fn build(
        id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        collection_config: Arc<TokioRwLock<CollectionConfig>>,
        shared_storage_config: Arc<SharedStorageConfig>,
        update_runtime: Handle,
    ) -> CollectionResult<LocalShard> {
        let config = collection_config.read().await;

        let wal_path = shard_path.join("wal");

        create_dir_all(&wal_path).await.map_err(|err| {
            CollectionError::service_error(format!(
                "Can't create shard wal directory. Error: {err}"
            ))
        })?;

        let segments_path = shard_path.join("segments");

        create_dir_all(&segments_path).await.map_err(|err| {
            CollectionError::service_error(format!(
                "Can't create shard segments directory. Error: {err}"
            ))
        })?;

        let mut segment_holder = SegmentHolder::default();
        let mut build_handlers = vec![];

        let vector_params = config.params.into_base_vector_data()?;
        let sparse_vector_params = config.params.into_sparse_vector_data()?;
        let segment_number = config.optimizer_config.get_number_segments();

        for _sid in 0..segment_number {
            let path_clone = segments_path.clone();
            let segment_config = SegmentConfig {
                vector_data: vector_params.clone(),
                sparse_vector_data: sparse_vector_params.clone(),
                payload_storage_type: if config.params.on_disk_payload {
                    PayloadStorageType::OnDisk
                } else {
                    PayloadStorageType::InMemory
                },
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

            segment_holder.add(segment);
        }

        let wal: SerdeWal<CollectionUpdateOperations> =
            SerdeWal::new(wal_path.to_str().unwrap(), (&config.wal_config).into())?;

        let optimizers = build_optimizers(
            shard_path,
            &config.params,
            &config.optimizer_config,
            &config.hnsw_config,
            &config.quantization_config,
        );

        drop(config); // release `shared_config` from borrow checker

        let collection = LocalShard::new(
            segment_holder,
            collection_config,
            shared_storage_config,
            wal,
            optimizers,
            shard_path,
            update_runtime,
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
    pub fn load_from_wal(&self, collection_id: CollectionId) -> CollectionResult<()> {
        let wal = self.wal.lock();
        let bar = ProgressBar::new(wal.len());

        let progress_style = ProgressStyle::default_bar()
            .template("{msg} [{elapsed_precise}] {wide_bar} {pos}/{len} (eta:{eta})")
            .expect("Failed to create progress style");
        bar.set_style(progress_style);

        bar.set_message(format!("Recovering collection {collection_id}"));
        let segments = self.segments();

        // Fall back to basic text output if the progress bar is hidden (e.g. not a tty)
        let show_progress_bar = !bar.is_hidden();
        if !show_progress_bar {
            eprintln!(
                "Recovering collection {collection_id}: 0/{} (0%)",
                wal.len(),
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

        for (i, (op_num, update)) in wal.read_all().enumerate() {
            // Propagate `CollectionError::ServiceError`, but skip other error types.
            match &CollectionUpdater::update(segments, op_num, update) {
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

            // Update progress bar or show text progress every 100 operations
            let progress = i as u64 + 1;
            bar.set_position(progress);
            if !show_progress_bar && progress % 100 == 0 {
                eprintln!(
                    "{}/{} ({}%)",
                    progress,
                    wal.len(),
                    (progress as f32 / wal.len() as f32 * 100.0) as usize,
                );
            }
        }

        self.segments.read().flush_all(true)?;

        bar.finish();
        if !show_progress_bar {
            eprintln!(
                "Recovered collection {collection_id}: {0}/{0} (100%)",
                wal.len(),
            );
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
        update_handler.run_workers(update_receiver);
        self.update_sender.load().send(UpdateSignal::Nop).await?;

        Ok(())
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
        // recover segments
        let segments_path = LocalShard::segments_path(snapshot_path);
        // iterate over segments directory and recover each segment
        for entry in std::fs::read_dir(segments_path)? {
            let entry_path = entry?.path();
            if entry_path.extension().map(|s| s == "tar").unwrap_or(false) {
                let segment_id_opt = entry_path
                    .file_stem()
                    .map(|s| s.to_str().unwrap().to_owned());
                if segment_id_opt.is_none() {
                    return Err(CollectionError::service_error(
                        "Segment ID is empty".to_string(),
                    ));
                }
                let segment_id = segment_id_opt.unwrap();
                Segment::restore_snapshot(&entry_path, &segment_id)?;
                std::fs::remove_file(&entry_path)?;
            }
        }
        Ok(())
    }

    /// Create snapshot for local shard into `target_path`
    pub async fn create_snapshot(
        &self,
        temp_path: &Path,
        target_path: &Path,
        save_wal: bool,
    ) -> CollectionResult<()> {
        let snapshot_shard_path = target_path;

        // snapshot all shard's segment
        let snapshot_segments_shard_path = snapshot_shard_path.join("segments");
        create_dir_all(&snapshot_segments_shard_path).await?;

        let segments = self.segments.clone();
        let wal = self.wal.clone();
        let snapshot_shard_path_owned = snapshot_shard_path.to_owned();

        if !save_wal {
            // If we are not saving WAL, we still need to make sure that all submitted by this point
            // updates have made it to the segments. So we use the Plunger to achieve that.
            // It will notify us when all submitted updates so far have been processed.
            let (tx, rx) = oneshot::channel();
            let plunger = UpdateSignal::Plunger(tx);
            self.update_sender.load().send(plunger).await?;
            rx.await?;
        }

        let temp_path = temp_path.to_owned();

        tokio::task::spawn_blocking(move || {
            let segments_read = segments.read();

            // Do not change segments while snapshotting
            segments_read.snapshot_all_segments(&temp_path, &snapshot_segments_shard_path)?;

            if save_wal {
                // snapshot all shard's WAL
                Self::snapshot_wal(wal, &snapshot_shard_path_owned)
            } else {
                Self::snapshot_empty_wal(wal, &snapshot_shard_path_owned)
            }
        })
        .await??;

        // copy shard's config
        let shard_config_path = ShardConfig::get_config_path(&self.path);
        let target_shard_config_path = snapshot_shard_path.join(SHARD_CONFIG_FILE);
        copy(&shard_config_path, &target_shard_config_path).await?;
        Ok(())
    }

    /// Create empty WAL which is compatible with currently stored data
    pub fn snapshot_empty_wal(wal: LockedWal, snapshot_shard_path: &Path) -> CollectionResult<()> {
        let (segment_capacity, latest_op_num) = {
            let wal_guard = wal.lock();
            (wal_guard.segment_capacity(), wal_guard.last_index())
        };

        let target_path = Self::wal_path(snapshot_shard_path);

        // Create directory if it does not exist
        std::fs::create_dir_all(&target_path).map_err(|err| {
            CollectionError::service_error(format!(
                "Can not crate directory {}: {}",
                target_path.display(),
                err
            ))
        })?;

        Wal::generate_empty_wal_starting_at_index(
            target_path,
            &WalOptions {
                segment_capacity,
                segment_queue_len: 0,
            },
            latest_op_num,
        )
        .map_err(|err| {
            CollectionError::service_error(format!("Error while create empty WAL: {err}"))
        })
    }

    /// snapshot WAL
    ///
    /// copies all WAL files into `snapshot_shard_path/wal`
    pub fn snapshot_wal(wal: LockedWal, snapshot_shard_path: &Path) -> CollectionResult<()> {
        // lock wal during snapshot
        let mut wal_guard = wal.lock();
        wal_guard.flush()?;
        let source_wal_path = wal_guard.path();
        let options = fs_extra::dir::CopyOptions::new();
        fs_extra::dir::copy(source_wal_path, snapshot_shard_path, &options).map_err(|err| {
            CollectionError::service_error(format!(
                "Error while copy WAL {snapshot_shard_path:?} {err}"
            ))
        })?;
        Ok(())
    }

    pub fn estimate_cardinality<'a>(
        &'a self,
        filter: Option<&'a Filter>,
    ) -> CollectionResult<CardinalityEstimation> {
        let segments = self.segments().read();
        let some_segment = segments.iter().next();

        if some_segment.is_none() {
            return Ok(CardinalityEstimation::exact(0));
        }
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

    pub fn read_filtered<'a>(
        &'a self,
        filter: Option<&'a Filter>,
    ) -> CollectionResult<BTreeSet<PointIdType>> {
        let segments = self.segments().read();
        let some_segment = segments.iter().next();

        if some_segment.is_none() {
            return Ok(Default::default());
        }
        let all_points: BTreeSet<_> = segments
            .iter()
            .flat_map(|(_id, segment)| segment.get().read().read_filtered(None, None, filter))
            .collect();
        Ok(all_points)
    }

    pub fn get_telemetry_data(&self) -> LocalShardTelemetry {
        let segments_read_guard = self.segments.read();
        let segments: Vec<_> = segments_read_guard
            .iter()
            .map(|(_id, segment)| segment.get().read().get_telemetry_data())
            .collect();

        let optimizer_status = match &segments_read_guard.optimizer_errors {
            None => OptimizersStatus::Ok,
            Some(error) => OptimizersStatus::Error(error.to_string()),
        };
        drop(segments_read_guard);
        let optimizations = self
            .optimizers
            .iter()
            .map(|optimizer| optimizer.get_telemetry_data())
            .fold(Default::default(), |acc, x| acc + x);

        LocalShardTelemetry {
            variant_name: None,
            segments,
            optimizations: OptimizerTelemetry {
                status: optimizer_status,
                optimizations,
                log: self.optimizers_log.lock().to_telemetry(),
            },
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

    pub async fn local_shard_info(&self) -> CollectionInfoInternal {
        let collection_config = self.collection_config.read().await.clone();
        let segments = self.segments().read();
        let mut vectors_count = 0;
        let mut indexed_vectors_count = 0;
        let mut points_count = 0;
        let mut segments_count = 0;
        let mut status = CollectionStatus::Green;
        let mut schema: HashMap<PayloadKeyType, PayloadIndexInfo> = Default::default();
        for (_idx, segment) in segments.iter() {
            segments_count += 1;

            let segment_info = segment.get().read().info();

            if segment_info.segment_type == SegmentType::Special {
                status = CollectionStatus::Yellow;
            }
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
        if !segments.failed_operation.is_empty() || segments.optimizer_errors.is_some() {
            status = CollectionStatus::Red;
        }

        let optimizer_status = match &segments.optimizer_errors {
            None => OptimizersStatus::Ok,
            Some(error) => OptimizersStatus::Error(error.to_string()),
        };

        CollectionInfoInternal {
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
