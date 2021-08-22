use std::collections::HashMap;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use crossbeam_channel::Sender;
use indicatif::ProgressBar;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use tokio::runtime::{Handle, Runtime};

use segment::types::{
    Condition, Filter, HasIdCondition, PayloadKeyType, PayloadSchemaInfo, PointIdType, ScoredPoint,
    SegmentType, VectorElementType,
};

use crate::collection_builder::optimizers_builder::build_optimizers;
use crate::collection_manager::collection_managers::{CollectionSearcher, CollectionUpdater};
use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::config::CollectionConfig;
use crate::operations::config_diff::{DiffConfig, OptimizersConfigDiff};
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CollectionStatus, RecommendRequest,
    ScrollRequest, ScrollResult, SearchRequest, UpdateResult, UpdateStatus,
};
use crate::operations::CollectionUpdateOperations;
use crate::update_handler::{UpdateHandler, UpdateSignal};
use crate::wal::SerdeWal;

pub struct Collection {
    segments: Arc<RwLock<SegmentHolder>>,
    config: Arc<RwLock<CollectionConfig>>,
    wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    update_handler: Arc<Mutex<UpdateHandler>>,
    runtime_handle: Option<Runtime>,
    update_sender: Sender<UpdateSignal>,
    path: PathBuf,
}

/// Collection holds information about segments and WAL.
impl Collection {
    pub fn new(
        segments: Arc<RwLock<SegmentHolder>>,
        config: CollectionConfig,
        wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
        update_handler: UpdateHandler,
        runtime_handle: Runtime,
        update_sender: Sender<UpdateSignal>,
        path: PathBuf,
    ) -> Self {
        Self {
            segments,
            config: Arc::new(RwLock::new(config)),
            wal,
            update_handler: Arc::new(Mutex::new(update_handler)),
            runtime_handle: Some(runtime_handle),
            update_sender,
            path,
        }
    }

    pub fn segments(&self) -> &RwLock<SegmentHolder> {
        self.segments.deref()
    }

    /// Imply interior mutability.
    /// Performs update operation on this collection asynchronously.
    /// Explicitly waits for result to be updated.
    pub async fn update_by(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
        segment_updater: Arc<dyn CollectionUpdater + Send + Sync>,
    ) -> CollectionResult<UpdateResult> {
        let operation_id = self.wal.lock().write(&operation)?;

        let sndr = self.update_sender.clone();
        let segments = self.segments.clone();
        let update_future = async move {
            let res = segment_updater.update(segments.deref(), operation_id, operation);
            sndr.send(UpdateSignal::Operation(operation_id))?;
            res
        };

        if let Some(handle) = self.runtime_handle.as_ref() {
            let update_handler = handle.spawn(update_future);
            if !wait {
                return Ok(UpdateResult {
                    operation_id,
                    status: UpdateStatus::Acknowledged,
                });
            }

            let _res: usize = update_handler.await??;
            Ok(UpdateResult {
                operation_id,
                status: UpdateStatus::Completed,
            })
        } else {
            // The above error could only happen if update is called in parallel with
            // drop. That means the drop method has already shut down the update runtime
            // If you see this error, most likely you have synchronization issue
            // with updating collection and dropping it at the same time.
            // It is almost not possible to achieve in safe rust as the caller of update you
            // have a reference to update handler, so the drop is not called yet.
            Err(CollectionError::ServiceError {
                error: "Calling update on removed collection".to_owned(),
            })
        }
    }

    pub async fn scroll_by(
        &self,
        request: ScrollRequest,
        segment_searcher: &(dyn CollectionSearcher),
    ) -> CollectionResult<ScrollResult> {
        let default_request = ScrollRequest::default();

        let offset = request
            .offset
            .unwrap_or_else(|| default_request.offset.unwrap());
        let limit = request
            .limit
            .unwrap_or_else(|| default_request.limit.unwrap());
        let with_payload = request
            .with_payload
            .unwrap_or_else(|| default_request.with_payload.unwrap());
        let with_vector = request
            .with_vector
            .unwrap_or_else(|| default_request.with_vector.unwrap());

        // ToDo: Make faster points selection with a set
        let segments = self.segments();
        let point_ids = segments
            .read()
            .iter()
            .map(|(_, segment)| {
                segment
                    .get()
                    .read()
                    .read_filtered(offset, limit, request.filter.as_ref())
                    .into_iter()
            })
            .flatten()
            .sorted()
            .dedup()
            .take(limit)
            .collect_vec();

        let mut points = segment_searcher
            .retrieve(segments, &point_ids, with_payload, with_vector)
            .await?;
        points.sort_by_key(|point| point.id);

        let next_page_offset = if point_ids.len() < limit {
            None
        } else {
            Some(point_ids.last().unwrap() + 1)
        };

        Ok(ScrollResult {
            points,
            next_page_offset,
        })
    }

    pub async fn recommend_by(
        &self,
        request: Arc<RecommendRequest>,
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
            .chain(request.negative.iter())
            .cloned()
            .collect_vec();

        let vectors = segment_searcher
            .retrieve(segments, &reference_vectors_ids, false, true)
            .await?;
        let vectors_map: HashMap<PointIdType, Vec<VectorElementType>> = vectors
            .into_iter()
            .map(|rec| (rec.id, rec.vector.unwrap()))
            .collect();

        for point_id in reference_vectors_ids.iter().cloned() {
            if !vectors_map.contains_key(&point_id) {
                return Err(CollectionError::NotFound {
                    missed_point_id: point_id,
                });
            }
        }

        let avg_positive = Collection::avg_vectors(
            request
                .positive
                .iter()
                .map(|vid| vectors_map.get(vid).unwrap()),
        );

        let search_vector = if request.negative.is_empty() {
            avg_positive
        } else {
            let avg_negative = Collection::avg_vectors(
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
            params: request.params,
            top: request.top,
        };

        segment_searcher
            .search(segments, Arc::new(search_request), search_runtime_handle)
            .await
    }

    pub fn info(&self) -> CollectionResult<CollectionInfo> {
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
            for (key, val) in segment_info.schema.into_iter() {
                schema.insert(key, val);
            }
        }
        Ok(CollectionInfo {
            status,
            vectors_count,
            segments_count,
            disk_data_size: disk_size,
            ram_data_size: ram_size,
            config: self.config.read().clone(),
            payload_schema: schema,
        })
    }

    pub fn stop(&self) -> CollectionResult<()> {
        self.update_sender.send(UpdateSignal::Stop)?;
        Ok(())
    }

    pub fn avg_vectors<'a>(
        vectors: impl Iterator<Item = &'a Vec<VectorElementType>>,
    ) -> Vec<VectorElementType> {
        let mut count: usize = 0;
        let mut avg_vector: Vec<VectorElementType> = vec![];
        for vector in vectors {
            count += 1;
            for i in 0..vector.len() {
                if i >= avg_vector.len() {
                    avg_vector.push(vector[i])
                } else {
                    avg_vector[i] += vector[i];
                }
            }
        }

        for item in &mut avg_vector {
            *item /= count as VectorElementType;
        }

        avg_vector
    }

    /// Updates collection optimization params:
    /// - Saves new params on disk
    /// - Stops existing optimization loop
    /// - Runs new optimizers with new params
    pub async fn update_optimizer_params(
        &self,
        optimizer_config_diff: OptimizersConfigDiff,
    ) -> CollectionResult<()> {
        {
            let mut config = self.config.write();
            config.optimizer_config = optimizer_config_diff.update(&config.optimizer_config)?;
            config.save(self.path.as_path())?;
        }
        let config = self.config.read();
        let mut update_handler = self.update_handler.lock();
        self.stop()?;
        update_handler.wait_worker_stops().await?;
        let new_optimizers = build_optimizers(
            self.path.as_path(),
            &config.params,
            &config.optimizer_config,
            &config.hnsw_config,
        );
        update_handler.optimizers = new_optimizers;
        update_handler.flush_timeout_sec = config.optimizer_config.flush_interval_sec;
        update_handler.run_worker();
        self.update_sender.send(UpdateSignal::Nop)?;

        Ok(())
    }

    pub async fn wait_update_worker_stops(&self) -> CollectionResult<()> {
        let mut update_handler = self.update_handler.lock();
        update_handler.wait_worker_stops().await
    }

    pub fn load_from_wal(&self, segment_updater: Arc<dyn CollectionUpdater>) {
        let wal = self.wal.lock();
        let bar = ProgressBar::new(wal.len());
        bar.set_message("Recovering collection");
        let segments = self.segments();
        for (op_num, update) in wal.read_all() {
            // Panic only in case of internal error. If wrong formatting - skip
            if let Err(CollectionError::ServiceError { error }) =
                segment_updater.update(segments, op_num, update)
            {
                panic!("Can't apply WAL operation: {}", error)
            }
            bar.inc(1);
        }

        self.segments.read().flush_all().unwrap();
        bar.finish();
    }
}

impl Drop for Collection {
    fn drop(&mut self) {
        self.stop().unwrap(); // Finishes update tasks right before destructor stuck to do so with runtime

        // The drop could be called from the tokio context, e.g. from perform_collection_operation method.
        // Calling remove from there would lead to the following error in a new version of tokio:
        // "Cannot drop a runtime in a context where blocking is not allowed. This happens when a runtime is dropped from within an asynchronous context."
        // So the workaround for move out the runtime handler and drop it in the separate thread.
        // The proper solution is to reconsider the collection to be an owner of the runtime
        let handle = self.runtime_handle.take();
        let thread_handler = thread::spawn(move || {
            drop(handle);
        });
        thread_handler.join().unwrap();
    }
}
