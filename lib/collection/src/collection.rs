use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crossbeam_channel::Sender;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use tokio::runtime::Runtime;

use segment::types::Condition;
use segment::types::Filter;
use segment::types::{
    HasIdCondition, PayloadKeyType, PayloadSchemaInfo, PointIdType, ScoredPoint, SegmentType,
    VectorElementType,
};

use crate::collection_builder::optimizers_builder::build_optimizers;
use crate::config::CollectionConfig;
use crate::operations::config_diff::{DiffConfig, OptimizersConfigDiff};
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CollectionStatus, RecommendRequest, Record,
    ScrollRequest, ScrollResult, SearchRequest, UpdateResult, UpdateStatus,
};
use crate::operations::CollectionUpdateOperations;
use crate::segment_manager::holders::segment_holder::SegmentHolder;
use crate::segment_manager::segment_managers::{SegmentSearcher, SegmentUpdater};
use crate::update_handler::update_handler::{UpdateHandler, UpdateSignal};
use crate::wal::SerdeWal;

pub struct Collection {
    pub segments: Arc<RwLock<SegmentHolder>>,
    pub config: Arc<RwLock<CollectionConfig>>,
    pub wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    pub searcher: Arc<dyn SegmentSearcher + Sync + Send>,
    pub update_handler: Arc<Mutex<UpdateHandler>>,
    pub updater: Arc<dyn SegmentUpdater + Sync + Send>,
    pub runtime_handle: Arc<Runtime>,
    pub update_sender: Sender<UpdateSignal>,
    pub path: PathBuf,
}

/// Collection holds information about segments and WAL.
impl Collection {
    /// Imply interior mutability.
    /// Performs update operation on this collection asynchronously.
    /// Explicitly waits for result to be updated.
    pub fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let operation_id = self.wal.lock().write(&operation)?;

        let upd = self.updater.clone();
        let sndr = self.update_sender.clone();
        let update_future = async move {
            let res = upd.update(operation_id, operation);
            sndr.send(UpdateSignal::Operation(operation_id))?;
            res
        };

        let update_handler = self.runtime_handle.spawn(update_future);

        if !wait {
            return Ok(UpdateResult {
                operation_id,
                status: UpdateStatus::Acknowledged,
            });
        }

        let _res: usize = self.runtime_handle.block_on(update_handler)??;
        Ok(UpdateResult {
            operation_id,
            status: UpdateStatus::Completed,
        })
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

    pub fn search(&self, request: Arc<SearchRequest>) -> CollectionResult<Vec<ScoredPoint>> {
        return self.searcher.search(request);
    }

    pub fn scroll(&self, request: Arc<ScrollRequest>) -> CollectionResult<ScrollResult> {
        let default_request = ScrollRequest::default();

        let offset = request.offset.unwrap_or(default_request.offset.unwrap());
        let limit = request.limit.unwrap_or(default_request.limit.unwrap());
        let with_payload = request
            .with_payload
            .unwrap_or(default_request.with_payload.unwrap());
        let with_vector = request
            .with_vector
            .unwrap_or(default_request.with_vector.unwrap());

        // ToDo: Make faster points selection with a set
        let point_ids = self
            .segments
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

        let mut points = self.retrieve(&point_ids, with_payload, with_vector)?;
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

    pub fn retrieve(
        &self,
        points: &Vec<PointIdType>,
        with_payload: bool,
        with_vector: bool,
    ) -> CollectionResult<Vec<Record>> {
        return self.searcher.retrieve(points, with_payload, with_vector);
    }

    pub fn stop(&self) -> CollectionResult<()> {
        self.update_sender.send(UpdateSignal::Stop)?;
        Ok(())
    }

    pub fn flush_all(&self) -> CollectionResult<()> {
        self.segments.read().flush_all()?;
        Ok(())
    }

    fn avg_vectors<'a>(
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
                    avg_vector[i] = avg_vector[i] + vector[i];
                }
            }
        }

        for i in 0..avg_vector.len() {
            avg_vector[i] = avg_vector[i] / (count as VectorElementType);
        }

        avg_vector
    }

    pub fn recommend(&self, request: Arc<RecommendRequest>) -> CollectionResult<Vec<ScoredPoint>> {
        if request.positive.is_empty() {
            return Err(CollectionError::BadRequest {
                description: format!("At least one positive vector ID required"),
            });
        }

        let reference_vectors_ids = request
            .positive
            .iter()
            .chain(request.negative.iter())
            .cloned()
            .collect_vec();

        let vectors = self.retrieve(&reference_vectors_ids, false, true)?;
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
                must: match request.filter.clone() {
                    None => None,
                    Some(filter) => Some(vec![Condition::Filter(filter)]),
                },
                must_not: Some(vec![Condition::HasId(HasIdCondition {
                    has_id: reference_vectors_ids.iter().cloned().collect(),
                })]),
            }),
            params: request.params.clone(),
            top: request.top,
        };

        self.search(Arc::new(search_request))
    }

    /// Updates collection optimization params:
    /// - Saves new params on disk
    /// - Stops existing optimization loop
    /// - Runs new optimizers with new params
    pub fn update_optimizer_params(
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
        update_handler.wait_worker_stops()?;
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
}

impl Drop for Collection {
    fn drop(&mut self) {
        self.stop().unwrap(); // Finishes update tasks right before destructor stucks to do so with runtime
    }
}
