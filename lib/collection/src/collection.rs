use std::collections::HashMap;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use indicatif::ProgressBar;
use itertools::Itertools;
use parking_lot::RwLock;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{mpsc::UnboundedSender, oneshot, Mutex};

use segment::types::{
    Condition, Filter, HasIdCondition, PayloadKeyType, PayloadSchemaInfo, PointIdType, ScoredPoint,
    SegmentType, VectorElementType, WithPayload,
};

use crate::collection_builder::optimizers_builder::build_optimizers;
use crate::collection_manager::collection_managers::CollectionSearcher;
use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::config::CollectionConfig;
use crate::operations::config_diff::{DiffConfig, OptimizersConfigDiff};
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CollectionStatus, RecommendRequest,
    ScrollRequest, ScrollResult, SearchRequest, UpdateResult, UpdateStatus,
};
use crate::operations::CollectionUpdateOperations;
use crate::update_handler::{OperationData, UpdateHandler, UpdateSignal};
use crate::wal::SerdeWal;
use futures::executor::block_on;

/// Collection
///
/// Holds all object, required for collection functioning
pub struct Collection {
    segments: Arc<RwLock<SegmentHolder>>,
    config: Arc<tokio::sync::RwLock<CollectionConfig>>,
    wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    update_handler: Arc<Mutex<UpdateHandler>>,
    runtime_handle: Option<Runtime>,
    update_sender: UnboundedSender<UpdateSignal>,
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
        update_sender: UnboundedSender<UpdateSignal>,
        path: PathBuf,
    ) -> Self {
        Self {
            segments,
            config: Arc::new(tokio::sync::RwLock::new(config)),
            wal,
            update_handler: Arc::new(tokio::sync::Mutex::new(update_handler)),
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
    pub async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let sndr = self.update_sender.clone();
        let (callback_sender, callback_receiver) = if wait {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let operation_id = {
            let mut wal_lock = self.wal.lock().await;
            let operation_id = wal_lock.write(&operation)?;
            sndr.send(UpdateSignal::Operation(OperationData {
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
        if !segments.failed_operation.is_empty() {
            status = CollectionStatus::Red;
        }
        Ok(CollectionInfo {
            status,
            vectors_count,
            segments_count,
            disk_data_size: disk_size,
            ram_data_size: ram_size,
            config: collection_config,
            payload_schema: schema,
        })
    }

    pub async fn stop(&self) -> CollectionResult<()> {
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
            let mut config = self.config.write().await;
            config.optimizer_config = optimizer_config_diff.update(&config.optimizer_config)?;
            config.save(&self.path)?;
        }
        let config = self.config.read().await;
        let mut update_handler = self.update_handler.lock().await;
        self.stop().await?;
        update_handler.wait_workers_stops().await?;
        let new_optimizers = build_optimizers(
            &self.path,
            &config.params,
            &config.optimizer_config,
            &config.hnsw_config,
        );
        update_handler.optimizers = new_optimizers;
        update_handler.flush_timeout_sec = config.optimizer_config.flush_interval_sec;
        update_handler.run_workers();
        self.update_sender.send(UpdateSignal::Nop)?;

        Ok(())
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

impl Drop for Collection {
    fn drop(&mut self) {
        // Finishes update tasks right before destructor stuck to do so with runtime
        block_on(self.stop()).unwrap();

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
