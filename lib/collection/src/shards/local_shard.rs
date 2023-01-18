use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use arc_swap::ArcSwap;
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use parking_lot::{Mutex as ParkingMutex, RwLock};
use segment::entry::entry_point::SegmentEntry;
use segment::index::field_index::CardinalityEstimation;
use segment::segment::Segment;
use segment::segment_constructor::{build_segment, load_segment};
use segment::types::{
    Filter, PayloadIndexInfo, PayloadKeyType, PayloadStorageType, PointIdType, SegmentConfig,
    SegmentType,
};
use tokio::fs::{copy, create_dir_all, remove_dir_all};
use tokio::runtime::{self, Runtime};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex, RwLock as TokioRwLock};

use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};
use crate::config::CollectionConfig;
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CollectionStatus, OptimizersStatus,
};
use crate::operations::CollectionUpdateOperations;
use crate::optimizers_builder::build_optimizers;
use crate::shards::shard::ShardId;
use crate::shards::shard_config::{ShardConfig, SHARD_CONFIG_FILE};
use crate::shards::telemetry::{LocalShardTelemetry, OptimizerTelemetry};
use crate::shards::CollectionId;
use crate::update_handler::{Optimizer, UpdateHandler, UpdateSignal, UPDATE_QUEUE_SIZE};
use crate::wal::SerdeWal;

/// LocalShard
///
/// LocalShard is an entity that can be moved between peers and contains some part of one collections data.
///
/// Holds all object, required for collection functioning
pub struct LocalShard {
    pub(super) segments: Arc<RwLock<SegmentHolder>>,
    pub(super) config: Arc<TokioRwLock<CollectionConfig>>,
    pub(super) wal: Arc<ParkingMutex<SerdeWal<CollectionUpdateOperations>>>,
    pub(super) update_handler: Arc<Mutex<UpdateHandler>>,
    pub(super) runtime_handle: Option<Runtime>,
    pub(super) update_sender: ArcSwap<Sender<UpdateSignal>>,
    pub(super) path: PathBuf,
    before_drop_called: bool,
    pub(super) optimizers: Arc<Vec<Arc<Optimizer>>>,
}

/// Shard holds information about segments and WAL.
impl LocalShard {
    pub async fn move_data(from: &Path, to: &Path) -> CollectionResult<()> {
        let wal_from = Self::wal_path(from);
        let wal_to = Self::wal_path(to);
        let segments_from = Self::segments_path(from);
        let segments_to = Self::segments_path(to);
        tokio::fs::rename(wal_from, wal_to).await?;
        tokio::fs::rename(segments_from, segments_to).await?;
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

    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        id: ShardId,
        collection_id: CollectionId,
        segment_holder: SegmentHolder,
        shared_config: Arc<TokioRwLock<CollectionConfig>>,
        wal: SerdeWal<CollectionUpdateOperations>,
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        shard_path: &Path,
    ) -> Self {
        let segment_holder = Arc::new(RwLock::new(segment_holder));
        let config = shared_config.read().await;
        let mut optimize_runtime_builder = runtime::Builder::new_multi_thread();

        optimize_runtime_builder
            .worker_threads(3)
            .enable_time()
            .thread_name_fn(move || {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let optimizer_id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("collection-{collection_id}-shard-{id}-optimizer-{optimizer_id}")
            });

        if config.optimizer_config.max_optimization_threads > 0 {
            // panics if val is not larger than 0.
            optimize_runtime_builder
                .max_blocking_threads(config.optimizer_config.max_optimization_threads);
        }

        let optimize_runtime = optimize_runtime_builder.build().unwrap();

        let locked_wal = Arc::new(ParkingMutex::new(wal));

        let mut update_handler = UpdateHandler::new(
            optimizers.clone(),
            optimize_runtime.handle().clone(),
            segment_holder.clone(),
            locked_wal.clone(),
            config.optimizer_config.flush_interval_sec,
            config.optimizer_config.max_optimization_threads,
        );

        let (update_sender, update_receiver) = mpsc::channel(UPDATE_QUEUE_SIZE);
        update_handler.run_workers(update_receiver);

        drop(config); // release `shared_config` from borrow checker

        Self {
            segments: segment_holder,
            config: shared_config,
            wal: locked_wal,
            update_handler: Arc::new(Mutex::new(update_handler)),
            runtime_handle: Some(optimize_runtime),
            update_sender: ArcSwap::from_pointee(update_sender),
            path: shard_path.to_owned(),
            before_drop_called: false,
            optimizers,
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
        shared_config: Arc<TokioRwLock<CollectionConfig>>,
    ) -> CollectionResult<LocalShard> {
        let collection_config = shared_config.read().await;

        let wal_path = Self::wal_path(shard_path);
        let segments_path = Self::segments_path(shard_path);
        let mut segment_holder = SegmentHolder::default();

        let wal: SerdeWal<CollectionUpdateOperations> = SerdeWal::new(
            wal_path.to_str().unwrap(),
            &(&collection_config.wal_config).into(),
        )
        .map_err(|e| CollectionError::service_error(format!("Wal error: {}", e)))?;

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
            if segments_path.ends_with("deleted") {
                remove_dir_all(&segments_path).await.map_err(|_| {
                    CollectionError::service_error(format!(
                        "Can't remove marked-for-remove segment {}",
                        segments_path.to_str().unwrap()
                    ))
                })?;
                continue;
            }
            load_handlers.push(
                thread::Builder::new()
                    .name(format!("shard-load-{}-{}", collection_id, id))
                    .spawn(move || {
                        let mut res = load_segment(&segments_path)?;
                        if let Some(segment) = &mut res {
                            segment.check_consistency_and_repair()?;
                        }
                        Ok::<_, CollectionError>(res)
                    })?,
            );
        }

        for handler in load_handlers {
            let segment_opt = handler.join().map_err(|err| {
                CollectionError::service_error(format!(
                    "Can't join segment load thread: {:?}",
                    err.type_id()
                ))
            })??;
            if let Some(segment) = segment_opt {
                segment_holder.add(segment);
            }
        }

        let optimizers = build_optimizers(
            shard_path,
            &collection_config.params,
            &collection_config.optimizer_config,
            &collection_config.hnsw_config,
        );

        drop(collection_config); // release `shared_config` from borrow checker

        let collection = LocalShard::new(
            id,
            collection_id.clone(),
            segment_holder,
            shared_config,
            wal,
            optimizers,
            shard_path,
        )
        .await;

        collection.load_from_wal(collection_id);

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
        shared_config: Arc<TokioRwLock<CollectionConfig>>,
    ) -> CollectionResult<LocalShard> {
        // initialize local shard config file
        let local_shard_config = ShardConfig::new_local();
        let shard = Self::build(id, collection_id, shard_path, shared_config).await?;
        local_shard_config.save(shard_path)?;
        Ok(shard)
    }

    /// Creates new empty shard with given configuration, initializing all storages, optimizers and directories.
    pub async fn build(
        id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        shared_config: Arc<TokioRwLock<CollectionConfig>>,
    ) -> CollectionResult<LocalShard> {
        let config = shared_config.read().await;

        let wal_path = shard_path.join("wal");

        create_dir_all(&wal_path).await.map_err(|err| {
            CollectionError::service_error(format!(
                "Can't create shard wal directory. Error: {}",
                err
            ))
        })?;

        let segments_path = shard_path.join("segments");

        create_dir_all(&segments_path).await.map_err(|err| {
            CollectionError::service_error(format!(
                "Can't create shard segments directory. Error: {}",
                err
            ))
        })?;

        let mut segment_holder = SegmentHolder::default();
        let mut build_handlers = vec![];

        let vector_params = config.params.get_all_vector_params()?;
        let segment_number = config.optimizer_config.get_number_segments();

        for _sid in 0..segment_number {
            let path_clone = segments_path.clone();
            let segment_config = SegmentConfig {
                vector_data: vector_params.clone(),
                index: Default::default(),
                storage_type: Default::default(),
                payload_storage_type: match config.params.on_disk_payload {
                    true => PayloadStorageType::OnDisk,
                    false => PayloadStorageType::InMemory,
                },
            };
            let segment = thread::Builder::new()
                .name(format!("shard-build-{}-{}", collection_id, id))
                .spawn(move || build_segment(&path_clone, &segment_config))
                .unwrap();
            build_handlers.push(segment);
        }

        let join_results = build_handlers
            .into_iter()
            .map(|handler| handler.join())
            .collect_vec();

        for join_result in join_results {
            let segment = join_result.map_err(|e| {
                let error_msg = if let Some(s) = e.downcast_ref::<&str>() {
                    format!("Segment DB create panicked with:\n{}", s)
                } else if let Some(s) = e.downcast_ref::<String>() {
                    format!("Segment DB create panicked with:\n{}", s)
                } else {
                    "Segment DB create failed with unknown reason".to_string()
                };
                CollectionError::service_error(error_msg)
            })??;
            segment_holder.add(segment);
        }

        let wal: SerdeWal<CollectionUpdateOperations> =
            SerdeWal::new(wal_path.to_str().unwrap(), &(&config.wal_config).into())?;

        let optimizers = build_optimizers(
            shard_path,
            &config.params,
            &config.optimizer_config,
            &config.hnsw_config,
        );

        drop(config); // release `shared_config` from borrow checker

        let collection = LocalShard::new(
            id,
            collection_id,
            segment_holder,
            shared_config,
            wal,
            optimizers,
            shard_path,
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
    pub fn load_from_wal(&self, collection_id: CollectionId) {
        let wal = self.wal.lock();
        let bar = ProgressBar::new(wal.len());

        let progress_style = ProgressStyle::default_bar()
            .template("{msg} [{elapsed_precise}] {wide_bar} {pos}/{len} (eta:{eta})")
            .expect("Failed to create progress style");
        bar.set_style(progress_style);

        bar.set_message(format!("Recovering collection {}", collection_id));
        let segments = self.segments();
        // ToDo: Start from minimal applied version
        for (op_num, update) in wal.read_all() {
            // Panic only in case of internal error. If wrong formatting - skip
            if let Err(CollectionError::ServiceError { error, .. }) =
                CollectionUpdater::update(segments, op_num, update)
            {
                panic!("Can't apply WAL operation: {}", error)
            }
            bar.inc(1);
        }

        self.segments.read().flush_all(true).unwrap();
        bar.finish();
    }

    pub async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        let config = self.config.read().await;
        let mut update_handler = self.update_handler.lock().await;

        let (update_sender, update_receiver) = mpsc::channel(UPDATE_QUEUE_SIZE);
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
        );
        update_handler.optimizers = new_optimizers;
        update_handler.flush_interval_sec = config.optimizer_config.flush_interval_sec;
        update_handler.run_workers(update_receiver);
        self.update_sender.load().send(UpdateSignal::Nop).await?;

        Ok(())
    }

    pub async fn before_drop(&mut self) {
        // Finishes update tasks right before destructor stuck to do so with runtime
        if let Err(err) = self.update_sender.load().send(UpdateSignal::Stop).await {
            log::warn!("Error sending stop signal to update handler: {}", err);
        }

        self.stop_flush_worker().await;

        if let Err(err) = self.wait_update_workers_stop().await {
            log::warn!("Update workers failed with: {}", err);
        }

        match self.runtime_handle.take() {
            None => {}
            Some(handle) => {
                // The drop could be called from the tokio context, e.g. from perform_collection_operation method.
                // Calling remove from there would lead to the following error in a new version of tokio:
                // "Cannot drop a runtime in a context where blocking is not allowed. This happens when a runtime is dropped from within an asynchronous context."
                // So the workaround for move out the runtime handler and drop it in the separate thread.
                // The proper solution is to reconsider the collection to be an owner of the runtime

                let thread_handler = thread::Builder::new()
                    .name("collection_drop".to_string())
                    .spawn(move || drop(handle))
                    .unwrap();
                thread_handler.join().unwrap();
            }
        }

        self.before_drop_called = true;
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

    /// create snapshot for local shard into `target_path`
    pub async fn create_snapshot(&self, target_path: &Path) -> CollectionResult<()> {
        let snapshot_shard_path = target_path;

        // snapshot all shard's segment
        let snapshot_segments_shard_path = snapshot_shard_path.join("segments");
        create_dir_all(&snapshot_segments_shard_path).await?;
        self.segments
            .read()
            .snapshot_all_segments(&snapshot_segments_shard_path)?;

        // snapshot all shard's WAL
        self.snapshot_wal(snapshot_shard_path)?;

        // copy shard's config
        let shard_config_path = ShardConfig::get_config_path(&self.path);
        let target_shard_config_path = snapshot_shard_path.join(SHARD_CONFIG_FILE);
        copy(&shard_config_path, &target_shard_config_path).await?;
        Ok(())
    }

    /// snapshot WAL
    ///
    /// copies all WAL files into `snapshot_shard_path/wal`
    pub fn snapshot_wal(&self, snapshot_shard_path: &Path) -> CollectionResult<()> {
        // lock wal during snapshot
        let _wal_guard = self.wal.lock();
        let source_wal_path = self.path.join("wal");
        let options = fs_extra::dir::CopyOptions::new();
        fs_extra::dir::copy(source_wal_path, snapshot_shard_path, &options).map_err(|err| {
            CollectionError::service_error(format!(
                "Error while copy WAL {:?} {}",
                snapshot_shard_path, err
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
            .map(|(_id, segment)| segment.get().read().estimate_points_count(filter))
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
            },
        }
    }

    fn assert_before_drop_called(&self) {
        if !self.before_drop_called {
            // Panic is used to get fast feedback in unit and integration tests
            // in cases where `before_drop` was not added.
            if cfg!(test) {
                panic!("Collection `before_drop` was not called.")
            } else {
                log::error!("Collection `before_drop` was not called.")
            }
        }
    }

    pub async fn local_shard_info(&self) -> CollectionInfo {
        let collection_config = self.config.read().await.clone();
        let segments = self.segments().read();
        let mut vectors_count = 0;
        let mut indexed_vectors_count = 0;
        let mut points_count = 0;
        let mut segments_count = 0;
        let mut status = CollectionStatus::Green;
        let mut schema: HashMap<PayloadKeyType, PayloadIndexInfo> = Default::default();
        for (_idx, segment) in segments.iter() {
            segments_count += 1;

            let segment_info = match segment {
                LockedSegment::Original(original_segment) => {
                    let info = original_segment.read().info();
                    if info.segment_type == SegmentType::Indexed {
                        indexed_vectors_count += info.num_vectors;
                    }
                    info
                }
                LockedSegment::Proxy(proxy_segment) => {
                    let proxy_segment_lock = proxy_segment.read();
                    let proxy_segment_info = proxy_segment_lock.info();

                    let wrapped_info = proxy_segment_lock.wrapped_segment.get().read().info();
                    if wrapped_info.segment_type == SegmentType::Indexed {
                        indexed_vectors_count += wrapped_info.num_vectors;
                    }
                    proxy_segment_info
                }
            };

            if segment_info.segment_type == SegmentType::Special {
                status = CollectionStatus::Yellow;
            }
            vectors_count += segment_info.num_vectors;
            points_count += segment_info.num_points;
            for (key, val) in segment_info.index_schema {
                match schema.entry(key) {
                    Entry::Occupied(o) => {
                        o.into_mut().points += val.points;
                    }
                    Entry::Vacant(v) => {
                        v.insert(val);
                    }
                }
            }
        }
        if !segments.failed_operation.is_empty() || segments.optimizer_errors.is_some() {
            status = CollectionStatus::Red;
        }

        let optimizer_status = match &segments.optimizer_errors {
            None => OptimizersStatus::Ok,
            Some(error) => OptimizersStatus::Error(error.to_string()),
        };

        CollectionInfo {
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
}

pub async fn drop_and_delete_from_disk(shard: LocalShard) -> CollectionResult<()> {
    let path = shard.shard_path();
    drop(shard);
    remove_dir_all(path).await?;
    Ok(())
}

impl Drop for LocalShard {
    fn drop(&mut self) {
        self.assert_before_drop_called()
    }
}
