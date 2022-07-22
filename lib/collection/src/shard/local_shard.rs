use std::cmp::max;
use std::collections::BTreeSet;
use std::fs::remove_file;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use arc_swap::ArcSwap;
use indicatif::ProgressBar;
use itertools::Itertools;
use parking_lot::RwLock;
use segment::index::field_index::CardinalityEstimation;
use segment::segment::Segment;
use segment::segment_constructor::{build_segment, load_segment};
use segment::types::{Filter, PayloadStorageType, PointIdType, SegmentConfig};
use tokio::fs::{copy, create_dir_all};
use tokio::runtime::{self, Runtime};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, Mutex, RwLock as TokioRwLock};

use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::config::CollectionConfig;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::CollectionUpdateOperations;
use crate::optimizers_builder::build_optimizers;
use crate::shard::shard_config::{ShardConfig, SHARD_CONFIG_FILE};
use crate::shard::{CollectionId, ShardId};
use crate::telemetry::ShardTelemetry;
use crate::update_handler::{Optimizer, UpdateHandler, UpdateSignal};
use crate::wal::SerdeWal;

/// LocalShard
///
/// LocalShard is an entity that can be moved between peers and contains some part of one collections data.
///
/// Holds all object, required for collection functioning
pub struct LocalShard {
    pub(super) segments: Arc<RwLock<SegmentHolder>>,
    pub(super) config: Arc<TokioRwLock<CollectionConfig>>,
    pub(super) wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    pub(super) update_handler: Arc<Mutex<UpdateHandler>>,
    pub(super) runtime_handle: Option<Runtime>,
    pub(super) update_sender: ArcSwap<UnboundedSender<UpdateSignal>>,
    pub(super) path: PathBuf,
    pub(super) before_drop_called: bool,
}

/// Shard holds information about segments and WAL.
impl LocalShard {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        id: ShardId,
        collection_id: CollectionId,
        segment_holder: SegmentHolder,
        shared_config: Arc<TokioRwLock<CollectionConfig>>,
        wal: SerdeWal<CollectionUpdateOperations>,
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        collection_path: &Path,
    ) -> Self {
        let segment_holder = Arc::new(RwLock::new(segment_holder));
        let config = shared_config.read().await;
        let blocking_threads = if config.optimizer_config.max_optimization_threads == 0 {
            max(num_cpus::get() - 1, 1)
        } else {
            config.optimizer_config.max_optimization_threads
        };
        let optimize_runtime = runtime::Builder::new_multi_thread()
            .worker_threads(3)
            .enable_time()
            .thread_name_fn(move || {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let optimizer_id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("collection-{collection_id}-shard-{id}-optimizer-{optimizer_id}")
            })
            .max_blocking_threads(blocking_threads)
            .build()
            .unwrap();

        let locked_wal = Arc::new(Mutex::new(wal));

        let mut update_handler = UpdateHandler::new(
            optimizers,
            optimize_runtime.handle().clone(),
            segment_holder.clone(),
            locked_wal.clone(),
            config.optimizer_config.flush_interval_sec,
            config.optimizer_config.max_optimization_threads,
        );

        let (update_sender, update_receiver) = mpsc::unbounded_channel();
        update_handler.run_workers(update_receiver);

        drop(config); // release `shared_config` from borrow checker

        Self {
            segments: segment_holder,
            config: shared_config,
            wal: locked_wal,
            update_handler: Arc::new(tokio::sync::Mutex::new(update_handler)),
            runtime_handle: Some(optimize_runtime),
            update_sender: ArcSwap::from_pointee(update_sender),
            path: collection_path.to_owned(),
            before_drop_called: false,
        }
    }

    pub(super) fn segments(&self) -> &RwLock<SegmentHolder> {
        self.segments.deref()
    }

    pub async fn load(
        id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        shared_config: Arc<TokioRwLock<CollectionConfig>>,
    ) -> LocalShard {
        let collection_config = shared_config.read().await;

        let wal_path = Self::wal_path(shard_path);
        let segments_path = Self::segments_path(shard_path);
        let mut segment_holder = SegmentHolder::default();

        let wal: SerdeWal<CollectionUpdateOperations> = SerdeWal::new(
            wal_path.to_str().unwrap(),
            &(&collection_config.wal_config).into(),
        )
        .expect("Can't read WAL");

        let segment_dirs = std::fs::read_dir(&segments_path).unwrap_or_else(|err| {
            panic!(
                "Can't read segments directory due to {}\nat {}",
                err,
                segments_path.to_str().unwrap()
            )
        });

        let mut load_handlers = vec![];

        for entry in segment_dirs {
            let segments_path = entry.unwrap().path();
            if segments_path.ends_with("deleted") {
                std::fs::remove_dir_all(&segments_path).unwrap_or_else(|_| {
                    panic!(
                        "Can't remove marked-for-remove segment {}",
                        segments_path.to_str().unwrap()
                    )
                });
                continue;
            }
            load_handlers.push(thread::spawn(move || load_segment(&segments_path)));
        }

        for handler in load_handlers {
            let res = handler.join();
            if let Err(err) = res {
                panic!("Can't load segment {:?}", err);
            }
            let res = res.unwrap();
            if let Err(res) = res {
                panic!("Can't load segment {:?}", res);
            }
            let segment = res.unwrap();
            segment_holder.add(segment);
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
            collection_id,
            segment_holder,
            shared_config,
            wal,
            optimizers,
            shard_path,
        )
        .await;

        collection.load_from_wal().await;

        collection
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

    pub async fn build_temp(
        id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        shared_config: Arc<TokioRwLock<CollectionConfig>>,
    ) -> CollectionResult<LocalShard> {
        // initialize temporary shard config file
        let temp_shard_config = ShardConfig::new_temp();
        Self::_build(
            id,
            collection_id,
            shard_path,
            shared_config,
            temp_shard_config,
        )
        .await
    }

    pub async fn build(
        id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        shared_config: Arc<TokioRwLock<CollectionConfig>>,
    ) -> CollectionResult<LocalShard> {
        // initialize local shard config file
        let local_shard_config = ShardConfig::new_local();
        Self::_build(
            id,
            collection_id,
            shard_path,
            shared_config,
            local_shard_config,
        )
        .await
    }

    /// Creates new empty shard with given configuration, initializing all storages, optimizers and directories.
    async fn _build(
        id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        shared_config: Arc<TokioRwLock<CollectionConfig>>,
        config: ShardConfig,
    ) -> CollectionResult<LocalShard> {
        config.save(shard_path)?;

        let config = shared_config.read().await;

        let wal_path = shard_path.join("wal");

        create_dir_all(&wal_path)
            .await
            .map_err(|err| CollectionError::ServiceError {
                error: format!("Can't create shard wal directory. Error: {}", err),
            })?;

        let segments_path = shard_path.join("segments");

        create_dir_all(&segments_path)
            .await
            .map_err(|err| CollectionError::ServiceError {
                error: format!("Can't create shard segments directory. Error: {}", err),
            })?;

        let mut segment_holder = SegmentHolder::default();
        let mut build_handlers = vec![];

        let vector_size = config.params.vector_size;
        let distance = config.params.distance;
        let segment_number = config.optimizer_config.get_number_segments();

        for _sid in 0..segment_number {
            let path_clone = segments_path.clone();
            let segment_config = SegmentConfig {
                vector_size,
                distance,
                index: Default::default(),
                storage_type: Default::default(),
                payload_storage_type: match config.params.on_disk_payload {
                    true => PayloadStorageType::OnDisk,
                    false => PayloadStorageType::InMemory,
                },
            };
            let segment = thread::spawn(move || build_segment(&path_clone, &segment_config));
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
                CollectionError::ServiceError { error: error_msg }
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
    pub async fn load_from_wal(&self) {
        let wal = self.wal.lock().await;
        let bar = ProgressBar::new(wal.len());
        bar.set_message("Recovering collection");
        let segments = self.segments();
        // ToDo: Start from minimal applied version
        for (op_num, update) in wal.read_all() {
            // Panic only in case of internal error. If wrong formatting - skip
            if let Err(CollectionError::ServiceError { error }) =
                CollectionUpdater::update(segments, op_num, update)
            {
                panic!("Can't apply WAL operation: {}", error)
            }
            bar.inc(1);
        }

        self.segments.read().flush_all().unwrap();
        bar.finish();
    }

    pub async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        let config = self.config.read().await;
        let mut update_handler = self.update_handler.lock().await;

        let (update_sender, update_receiver) = mpsc::unbounded_channel();
        // makes sure that the Stop signal is the last one in this channel
        let old_sender = self.update_sender.swap(Arc::new(update_sender));
        old_sender.send(UpdateSignal::Stop)?;
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
        self.update_sender.load().send(UpdateSignal::Nop)?;

        Ok(())
    }

    pub async fn before_drop(&mut self) {
        // Finishes update tasks right before destructor stuck to do so with runtime
        self.update_sender.load().send(UpdateSignal::Stop).unwrap();

        self.stop_flush_worker().await;

        self.wait_update_workers_stop().await.unwrap();

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
                    return Err(CollectionError::ServiceError {
                        error: "Segment ID is empty".to_string(),
                    });
                }
                let segment_id = segment_id_opt.unwrap();
                Segment::restore_snapshot(&entry_path, &segment_id)?;
                remove_file(&entry_path)?;
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
        self.snapshot_wal(snapshot_shard_path).await?;

        // copy shard's config
        let shard_config_path = ShardConfig::get_config_path(&self.path);
        let target_shard_config_path = snapshot_shard_path.join(SHARD_CONFIG_FILE);
        copy(&shard_config_path, &target_shard_config_path).await?;
        Ok(())
    }

    /// snapshot WAL
    ///
    /// copies all WAL files into `snapshot_shard_path/wal`
    pub async fn snapshot_wal(&self, snapshot_shard_path: &Path) -> CollectionResult<()> {
        // lock wal during snapshot
        let _wal_guard = self.wal.lock().await;
        let source_wal_path = self.path.join("wal");
        let options = fs_extra::dir::CopyOptions::new();
        fs_extra::dir::copy(&source_wal_path, snapshot_shard_path, &options).map_err(|err| {
            CollectionError::service_error(format!(
                "Error while copy WAL {:?} {}",
                snapshot_shard_path, err
            ))
        })?;
        Ok(())
    }

    pub async fn estimate_cardinality<'a>(
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

    pub async fn read_filtered<'a>(
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
            .flat_map(|(_id, segment)| segment.get().read().read_filtered(None, usize::MAX, filter))
            .collect();
        Ok(all_points)
    }

    pub fn get_telemetry_data(&self) -> ShardTelemetry {
        let segments = self
            .segments()
            .read()
            .iter()
            .map(|(_id, segment)| segment.get().read().get_telemetry_data())
            .collect();
        ShardTelemetry::Local { segments }
    }
}

impl Drop for LocalShard {
    fn drop(&mut self) {
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
}
