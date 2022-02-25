use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use indicatif::ProgressBar;
use itertools::Itertools;
use parking_lot::RwLock;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use std::cmp::max;
use std::fs::create_dir_all;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::runtime::{self, Handle, Runtime};
use tokio::sync::{mpsc, mpsc::UnboundedSender, oneshot, Mutex, RwLock as TokioRwLock};

use segment::types::{
    Condition, Filter, HasIdCondition, PayloadKeyType, PayloadSchemaInfo, PointIdType, ScoredPoint,
    SegmentType, VectorElementType, WithPayload,
};

use crate::collection_manager::collection_managers::CollectionSearcher;
use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::config::CollectionConfig;
use crate::operations::config_diff::{DiffConfig, OptimizersConfigDiff};
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CollectionStatus, OptimizersStatus,
    RecommendRequest, ScrollRequest, ScrollResult, SearchRequest, UpdateResult, UpdateStatus,
};
use crate::operations::CollectionUpdateOperations;
use crate::optimizers_builder::build_optimizers;
use crate::update_handler::{OperationData, Optimizer, UpdateHandler, UpdateSignal};
use crate::wal::SerdeWal;
use crate::CollectionId;
use futures::executor::block_on;
use segment::payload_storage::schema_storage::SchemaStorage;
use segment::segment_constructor::load_segment;
use std::fs::{read_dir, remove_dir_all};

pub type ShardId = u32;

/// Shard
///
/// Shard is an entity that can be moved between peers and contains some part of one collections data.
///
/// Holds all object, required for collection functioning
pub struct Shard {
    segments: Arc<RwLock<SegmentHolder>>,
    // TODO: Move config into Raft global state
    config: Arc<TokioRwLock<CollectionConfig>>,
    wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    update_handler: Arc<Mutex<UpdateHandler>>,
    runtime_handle: Option<Runtime>,
    update_sender: ArcSwap<UnboundedSender<UpdateSignal>>,
    path: PathBuf,
    schema_store: Arc<SchemaStorage>,
}

/// Shard holds information about segments and WAL.
impl Shard {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: ShardId,
        collection_id: CollectionId,
        segment_holder: SegmentHolder,
        config: CollectionConfig,
        wal: SerdeWal<CollectionUpdateOperations>,
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        collection_path: &Path,
        schema_store: Arc<SchemaStorage>,
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
            schema_store,
        }
    }

    pub fn load(id: ShardId, collection_id: CollectionId, collection_path: &Path) -> Shard {
        let wal_path = collection_path.join("wal");
        let segments_path = collection_path.join("segments");
        let mut segment_holder = SegmentHolder::default();

        let collection_config = CollectionConfig::load(collection_path).unwrap_or_else(|err| {
            panic!(
                "Can't read collection config due to {}\nat {}",
                err,
                collection_path.to_str().unwrap()
            )
        });

        let wal: SerdeWal<CollectionUpdateOperations> = SerdeWal::new(
            wal_path.to_str().unwrap(),
            &(&collection_config.wal_config).into(),
        )
        .expect("Can't read WAL");

        let schema_storage = Arc::new(SchemaStorage::new());

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
            let segment = match load_segment(&segments_path, schema_storage.clone()) {
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
            collection_path,
            &collection_config.params,
            &collection_config.optimizer_config,
            &collection_config.hnsw_config,
            schema_storage.clone(),
        );

        let collection = Shard::new(
            id,
            collection_id,
            segment_holder,
            collection_config,
            wal,
            optimizers,
            collection_path,
            schema_storage,
        );

        block_on(collection.load_from_wal());

        collection
    }

    /// Creates new empty shard with given configuration, initializing all storages, optimizers and directories.
    pub fn build(
        id: ShardId,
        collection_id: CollectionId,
        collection_path: &Path,
        config: &CollectionConfig,
    ) -> CollectionResult<Shard> {
        let wal_path = collection_path.join("wal");

        create_dir_all(&wal_path).map_err(|err| CollectionError::ServiceError {
            error: format!("Can't create collection directory. Error: {}", err),
        })?;

        let segments_path = collection_path.join("segments");

        create_dir_all(&segments_path).map_err(|err| CollectionError::ServiceError {
            error: format!("Can't create collection directory. Error: {}", err),
        })?;

        let mut segment_holder = SegmentHolder::default();

        let schema_storage = Arc::new(SchemaStorage::new());

        for _sid in 0..config.optimizer_config.default_segment_number {
            let segment = build_simple_segment(
                &segments_path,
                config.params.vector_size,
                config.params.distance,
                schema_storage.clone(),
            )?;
            segment_holder.add(segment);
        }

        let wal: SerdeWal<CollectionUpdateOperations> =
            SerdeWal::new(wal_path.to_str().unwrap(), &(&config.wal_config).into())?;

        let collection_config = CollectionConfig {
            params: config.params.clone(),
            hnsw_config: config.hnsw_config,
            optimizer_config: config.optimizer_config.clone(),
            wal_config: config.wal_config.clone(),
        };

        collection_config.save(collection_path)?;

        let optimizers = build_optimizers(
            collection_path,
            &config.params,
            &config.optimizer_config,
            &collection_config.hnsw_config,
            schema_storage.clone(),
        );

        let collection = Shard::new(
            id,
            collection_id,
            segment_holder,
            collection_config,
            wal,
            optimizers,
            collection_path,
            schema_storage,
        );

        Ok(collection)
    }

    pub fn segments(&self) -> &RwLock<SegmentHolder> {
        self.segments.deref()
    }

    /// Imply interior mutability.
    /// Performs update operation on this collection asynchronously.
    /// Explicitly waits for result to be updated.
    pub async fn update(
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

    pub async fn scroll_by(
        &self,
        request: ScrollRequest,
        segment_searcher: &(dyn CollectionSearcher),
    ) -> CollectionResult<ScrollResult> {
        let default_request = ScrollRequest::default();

        let offset = request.offset;
        let limit = request
            .limit
            .unwrap_or_else(|| default_request.limit.unwrap());
        let with_payload_interface = &request
            .with_payload
            .clone()
            .unwrap_or_else(|| default_request.with_payload.clone().unwrap());
        let with_vector = request
            .with_vector
            .unwrap_or_else(|| default_request.with_vector.unwrap());

        if limit == 0 {
            return Err(CollectionError::BadRequest {
                description: "Limit cannot be 0".to_string(),
            });
        }

        // Retrieve 1 extra point to determine proper `next_page_offset`
        let retrieve_limit = limit + 1;

        // ToDo: Make faster points selection with a set
        let segments = self.segments();
        let mut point_ids = segments
            .read()
            .iter()
            .flat_map(|(_, segment)| {
                segment
                    .get()
                    .read()
                    .read_filtered(offset, retrieve_limit, request.filter.as_ref())
            })
            .sorted()
            .dedup()
            .take(retrieve_limit)
            .collect_vec();

        let next_page_offset = if point_ids.len() < retrieve_limit {
            // This was the last page
            None
        } else {
            // remove extra point, it would be a first point of the next page
            Some(point_ids.pop().unwrap())
        };

        let with_payload = WithPayload::from(with_payload_interface);
        let mut points = segment_searcher
            .retrieve(segments, &point_ids, &with_payload, with_vector)
            .await?;
        points.sort_by_key(|point| point.id);

        Ok(ScrollResult {
            points,
            next_page_offset,
        })
    }

    pub async fn recommend_by(
        &self,
        request: RecommendRequest,
        segment_searcher: &(dyn CollectionSearcher),
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let segments = self.segments();
        if request.positive.is_empty() {
            return Err(CollectionError::BadRequest {
                description: "At least one positive vector ID required".to_owned(),
            });
        }

        let reference_vectors_ids = request
            .positive
            .iter()
            .chain(&request.negative)
            .cloned()
            .collect_vec();

        let vectors = segment_searcher
            .retrieve(
                segments,
                &reference_vectors_ids,
                &WithPayload::from(true),
                true,
            )
            .await?;
        let vectors_map: HashMap<PointIdType, Vec<VectorElementType>> = vectors
            .into_iter()
            .map(|rec| (rec.id, rec.vector.unwrap()))
            .collect();

        for &point_id in &reference_vectors_ids {
            if !vectors_map.contains_key(&point_id) {
                return Err(CollectionError::NotFound {
                    missed_point_id: point_id,
                });
            }
        }

        let avg_positive = crate::avg_vectors(
            request
                .positive
                .iter()
                .map(|vid| vectors_map.get(vid).unwrap()),
        );

        let search_vector = if request.negative.is_empty() {
            avg_positive
        } else {
            let avg_negative = crate::avg_vectors(
                request
                    .negative
                    .iter()
                    .map(|vid| vectors_map.get(vid).unwrap()),
            );

            avg_positive
                .iter()
                .cloned()
                .zip(avg_negative.iter().cloned())
                .map(|(pos, neg)| pos + pos - neg)
                .collect()
        };

        let search_request = SearchRequest {
            vector: search_vector,
            filter: Some(Filter {
                should: None,
                must: request
                    .filter
                    .clone()
                    .map(|filter| vec![Condition::Filter(filter)]),
                must_not: Some(vec![Condition::HasId(HasIdCondition {
                    has_id: reference_vectors_ids.iter().cloned().collect(),
                })]),
            }),
            with_payload: None,
            with_vector: None,
            params: request.params,
            top: request.top,
        };

        segment_searcher
            .search(segments, Arc::new(search_request), search_runtime_handle)
            .await
    }

    /// Collect overview information about the collection
    pub async fn info(&self) -> CollectionResult<CollectionInfo> {
        let collection_config = self.config.read().await.clone();
        let segments = self.segments.read();
        let mut vectors_count = 0;
        let mut segments_count = 0;
        let mut ram_size = 0;
        let mut disk_size = 0;
        let mut status = CollectionStatus::Green;
        let mut schema: HashMap<PayloadKeyType, PayloadSchemaInfo> = Default::default();
        for (_idx, segment) in segments.iter() {
            segments_count += 1;
            let segment_info = segment.get().read().info();
            if segment_info.segment_type == SegmentType::Special {
                status = CollectionStatus::Yellow;
            }
            vectors_count += segment_info.num_vectors;
            disk_size += segment_info.disk_usage_bytes;
            ram_size += segment_info.ram_usage_bytes;
            for (key, val) in segment_info.schema {
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

    /// Updates shard optimization params:
    /// - Saves new params on disk
    /// - Stops existing optimization loop
    /// - Runs new optimizers with new params
    pub async fn update_optimizer_params(
        &self,
        optimizer_config_diff: OptimizersConfigDiff,
    ) -> CollectionResult<()> {
        log::debug!("Updating optimizer params");
        {
            let mut config = self.config.write().await;
            config.optimizer_config = optimizer_config_diff.update(&config.optimizer_config)?;
            config.save(&self.path)?;
        }
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
            self.schema_store.clone(),
        );
        update_handler.optimizers = new_optimizers;
        update_handler.flush_interval_sec = config.optimizer_config.flush_interval_sec;
        update_handler.run_workers(update_receiver);
        self.update_sender.load().send(UpdateSignal::Nop)?;

        Ok(())
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
}

impl Drop for Shard {
    fn drop(&mut self) {
        // Finishes update tasks right before destructor stuck to do so with runtime
        self.update_sender.load().send(UpdateSignal::Stop).unwrap();
        block_on(self.stop_flush_worker());

        block_on(self.wait_update_workers_stop()).unwrap();

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
    }
}
