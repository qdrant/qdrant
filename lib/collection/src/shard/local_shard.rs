use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use async_trait::async_trait;
use indicatif::ProgressBar;
use itertools::Itertools;
use parking_lot::RwLock;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use std::cmp::max;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::fs::create_dir_all;
use tokio::runtime::{self, Handle, Runtime};
use tokio::sync::{mpsc, mpsc::UnboundedSender, oneshot, Mutex, RwLock as TokioRwLock};

use segment::types::{
    ExtendedPointId, Filter, PayloadIndexInfo, PayloadKeyType, ScoredPoint, SegmentType,
    WithPayload, WithPayloadInterface,
};

use crate::collection_manager::collection_managers::CollectionSearcher;
use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::config::CollectionConfig;
use crate::operations::config_diff::{DiffConfig, OptimizersConfigDiff};
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CollectionStatus, OptimizersStatus, Record,
    UpdateResult, UpdateStatus,
};
use crate::operations::CollectionUpdateOperations;
use crate::optimizers_builder::{build_optimizers, OptimizersConfig};
use crate::shard::ShardOperation;
use crate::update_handler::{OperationData, Optimizer, UpdateHandler, UpdateSignal};
use crate::wal::SerdeWal;
use crate::{CollectionId, PointRequest, SearchRequest, ShardId};
use segment::segment_constructor::load_segment;
use std::fs::{read_dir, remove_dir_all};

/// LocalShard
///
/// LocalShard is an entity that can be moved between peers and contains some part of one collections data.
///
/// Holds all object, required for collection functioning
pub struct LocalShard {
    segments: Arc<RwLock<SegmentHolder>>,
    config: Arc<TokioRwLock<CollectionConfig>>,
    wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    update_handler: Arc<Mutex<UpdateHandler>>,
    runtime_handle: Option<Runtime>,
    update_sender: ArcSwap<UnboundedSender<UpdateSignal>>,
    path: PathBuf,
    before_drop_called: bool,
}

/// Shard holds information about segments and WAL.
impl LocalShard {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: ShardId,
        collection_id: CollectionId,
        segment_holder: SegmentHolder,
        config: CollectionConfig,
        wal: SerdeWal<CollectionUpdateOperations>,
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        collection_path: &Path,
    ) -> Self {
        let segment_holder = Arc::new(RwLock::new(segment_holder));

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
        );

        let (update_sender, update_receiver) = mpsc::unbounded_channel();
        update_handler.run_workers(update_receiver);

        Self {
            segments: segment_holder,
            config: Arc::new(TokioRwLock::new(config)),
            wal: locked_wal,
            update_handler: Arc::new(tokio::sync::Mutex::new(update_handler)),
            runtime_handle: Some(optimize_runtime),
            update_sender: ArcSwap::from_pointee(update_sender),
            path: collection_path.to_owned(),
            before_drop_called: false,
        }
    }

    fn segments(&self) -> &RwLock<SegmentHolder> {
        self.segments.deref()
    }

    pub async fn load(
        id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        collection_config: &CollectionConfig,
    ) -> LocalShard {
        let wal_path = Self::wal_path(shard_path);
        let segments_path = Self::segments_path(shard_path);
        let mut segment_holder = SegmentHolder::default();

        let wal: SerdeWal<CollectionUpdateOperations> = SerdeWal::new(
            wal_path.to_str().unwrap(),
            &(&collection_config.wal_config).into(),
        )
        .expect("Can't read WAL");

        let segment_dirs = read_dir(&segments_path).unwrap_or_else(|err| {
            panic!(
                "Can't read segments directory due to {}\nat {}",
                err,
                segments_path.to_str().unwrap()
            )
        });

        for entry in segment_dirs {
            let segments_path = entry.unwrap().path();
            if segments_path.ends_with("deleted") {
                remove_dir_all(&segments_path).unwrap_or_else(|_| {
                    panic!(
                        "Can't remove marked-for-remove segment {}",
                        segments_path.to_str().unwrap()
                    )
                });
                continue;
            }
            let segment = match load_segment(&segments_path) {
                Ok(x) => x,
                Err(err) => panic!(
                    "Can't load segments from {}, error: {}",
                    segments_path.to_str().unwrap(),
                    err
                ),
            };
            segment_holder.add(segment);
        }

        let optimizers = build_optimizers(
            shard_path,
            &collection_config.params,
            &collection_config.optimizer_config,
            &collection_config.hnsw_config,
        );

        let collection = LocalShard::new(
            id,
            collection_id,
            segment_holder,
            collection_config.clone(),
            wal,
            optimizers,
            shard_path,
        );

        collection.load_from_wal().await;

        collection
    }

    pub fn wal_path(shard_path: &Path) -> PathBuf {
        shard_path.join("wal")
    }

    pub fn segments_path(shard_path: &Path) -> PathBuf {
        shard_path.join("segments")
    }

    /// Creates new empty shard with given configuration, initializing all storages, optimizers and directories.
    pub async fn build(
        id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        config: &CollectionConfig,
    ) -> CollectionResult<LocalShard> {
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
        for _sid in 0..config.optimizer_config.default_segment_number {
            let path_clone = segments_path.clone();
            let segment =
                thread::spawn(move || build_simple_segment(&path_clone, vector_size, distance));
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

        let collection = LocalShard::new(
            id,
            collection_id,
            segment_holder,
            config.clone(),
            wal,
            optimizers,
            shard_path,
        );

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

    /// Updates shard optimization params:
    /// - Saves new params on disk
    /// - Stops existing optimization loop
    /// - Runs new optimizers with new params
    pub async fn update_optimizer_with_diff(
        &self,
        optimizer_config_diff: OptimizersConfigDiff,
    ) -> CollectionResult<()> {
        log::debug!("Updating optimizer params");
        {
            let mut config = self.config.write().await;
            config.optimizer_config = optimizer_config_diff.update(&config.optimizer_config)?;
            config.save(&self.path)?;
        }
        self.on_optimizer_config_update().await
    }

    /// Updates shard optimization params:
    /// - Saves new params on disk
    /// - Stops existing optimization loop
    /// - Runs new optimizers with new params
    pub async fn update_optimizer_with_config(
        &self,
        optimizer_config: OptimizersConfig,
    ) -> CollectionResult<()> {
        log::debug!("Updating optimizer params");
        {
            let mut config = self.config.write().await;
            config.optimizer_config = optimizer_config;
            config.save(&self.path)?;
        }
        self.on_optimizer_config_update().await
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
}

#[async_trait]
#[allow(unused_variables)]
impl ShardOperation for &LocalShard {
    /// Imply interior mutability.
    /// Performs update operation on this collection asynchronously.
    /// Explicitly waits for result to be updated.
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let (callback_sender, callback_receiver) = if wait {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let operation_id = {
            let mut wal_lock = self.wal.lock().await;
            let operation_id = wal_lock.write(&operation)?;
            self.update_sender
                .load()
                .send(UpdateSignal::Operation(OperationData {
                    op_num: operation_id,
                    operation,
                    sender: callback_sender,
                }))?;
            operation_id
        };

        if let Some(receiver) = callback_receiver {
            let _res = receiver.await??;
            Ok(UpdateResult {
                operation_id,
                status: UpdateStatus::Completed,
            })
        } else {
            Ok(UpdateResult {
                operation_id,
                status: UpdateStatus::Acknowledged,
            })
        }
    }

    async fn scroll_by(
        &self,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: bool,
        filter: Option<&Filter>,
    ) -> CollectionResult<Vec<Record>> {
        // ToDo: Make faster points selection with a set
        let segments = self.segments();
        let point_ids = segments
            .read()
            .iter()
            .flat_map(|(_, segment)| segment.get().read().read_filtered(offset, limit, filter))
            .sorted()
            .dedup()
            .take(limit)
            .collect_vec();

        let with_payload = WithPayload::from(with_payload_interface);
        let mut points = segment_searcher
            .retrieve(segments, &point_ids, &with_payload, with_vector)
            .await?;
        points.sort_by_key(|point| point.id);

        Ok(points)
    }

    /// Collect overview information about the shard
    async fn info(&self) -> CollectionResult<CollectionInfo> {
        let collection_config = self.config.read().await.clone();
        let segments = self.segments.read();
        let mut vectors_count = 0;
        let mut segments_count = 0;
        let mut ram_size = 0;
        let mut disk_size = 0;
        let mut status = CollectionStatus::Green;
        let mut schema: HashMap<PayloadKeyType, PayloadIndexInfo> = Default::default();
        for (_idx, segment) in segments.iter() {
            segments_count += 1;
            let segment_info = segment.get().read().info();
            if segment_info.segment_type == SegmentType::Special {
                status = CollectionStatus::Yellow;
            }
            vectors_count += segment_info.num_vectors;
            disk_size += segment_info.disk_usage_bytes;
            ram_size += segment_info.ram_usage_bytes;
            for (key, val) in segment_info.index_schema {
                schema.insert(key, val);
            }
        }
        if !segments.failed_operation.is_empty() || segments.optimizer_errors.is_some() {
            status = CollectionStatus::Red;
        }

        let optimizer_status = match &segments.optimizer_errors {
            None => OptimizersStatus::Ok,
            Some(error) => OptimizersStatus::Error(error.to_string()),
        };

        Ok(CollectionInfo {
            status,
            optimizer_status,
            vectors_count,
            segments_count,
            disk_data_size: disk_size,
            ram_data_size: ram_size,
            config: collection_config,
            payload_schema: schema,
        })
    }

    async fn search(
        &self,
        request: Arc<SearchRequest>,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let res = segment_searcher
            .search(self.segments(), request.clone(), search_runtime_handle)
            .await?;
        let distance = self.config.read().await.params.distance;
        let processed_res = res.into_iter().map(|mut scored_point| {
            scored_point.score = distance.postprocess_score(scored_point.score);
            scored_point
        });

        let top_result = if let Some(threshold) = request.score_threshold {
            processed_res
                .take_while(|scored_point| distance.check_threshold(scored_point.score, threshold))
                .collect()
        } else {
            processed_res.collect()
        };
        Ok(top_result)
    }

    async fn retrieve(
        &self,
        request: Arc<PointRequest>,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        with_payload: &WithPayload,
        with_vector: bool,
    ) -> CollectionResult<Vec<Record>> {
        segment_searcher
            .retrieve(self.segments(), &request.ids, with_payload, with_vector)
            .await
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
