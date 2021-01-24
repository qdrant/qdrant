use thiserror::Error;
use crate::operations::CollectionUpdateOperations;
use segment::types::{PointIdType, ScoredPoint, SegmentConfig, VectorElementType};
use std::result;
use crate::operations::types::{Record, CollectionInfo, UpdateResult, UpdateStatus, SearchRequest, RecommendRequest};
use std::sync::Arc;
use crate::wal::{SerdeWal, WalError};
use crate::segment_manager::segment_managers::{SegmentSearcher, SegmentUpdater};
use segment::entry::entry_point::OperationError;
use tokio::task::JoinError;
use crossbeam_channel::{Sender, SendError};
use crate::update_handler::update_handler::{UpdateHandler, UpdateSignal};
use parking_lot::{Mutex, RwLock};
use crate::segment_manager::holders::segment_holder::SegmentHolder;
use tokio::runtime::Runtime;
use itertools::Itertools;
use std::collections::HashMap;
use segment::types::Filter;
use segment::types::Condition;


#[derive(Error, Debug, Clone)]
#[error("{0}")]
pub enum CollectionError {
    #[error("Wrong input: {description}")]
    BadInput { description: String },
    #[error("No point with id {missed_point_id} found")]
    NotFound { missed_point_id: PointIdType },
    #[error("Service internal error: {error}")]
    ServiceError { error: String },
    #[error("Bad request: {description}")]
    BadRequest { description: String },
}

impl From<OperationError> for CollectionError {
    fn from(err: OperationError) -> Self {
        match err {
            OperationError::WrongVector { .. } => Self::BadInput { description: format!("{}", err) },
            OperationError::PointIdError { missed_point_id } => Self::NotFound { missed_point_id },
            OperationError::ServiceError { description } => Self::ServiceError { error: description }
        }
    }
}

impl From<JoinError> for CollectionError {
    fn from(err: JoinError) -> Self {
        Self::ServiceError { error: format!("{}", err) }
    }
}

impl From<WalError> for CollectionError {
    fn from(err: WalError) -> Self {
        Self::ServiceError { error: format!("{}", err) }
    }
}

impl<T> From<SendError<T>> for CollectionError {
    fn from(_err: SendError<T>) -> Self {
        Self::ServiceError { error: format!("Can't reach one of the workers") }
    }
}

pub type CollectionResult<T> = result::Result<T, CollectionError>;

pub struct Collection {
    pub segments: Arc<RwLock<SegmentHolder>>,
    pub config: SegmentConfig,
    pub wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    pub searcher: Arc<dyn SegmentSearcher + Sync + Send>,
    pub update_handler: Arc<UpdateHandler>,
    pub updater: Arc<dyn SegmentUpdater + Sync + Send>,
    pub runtime_handle: Arc<Runtime>,
    pub update_sender: Sender<UpdateSignal>,
}


/// Collection holds information about segments and WAL.
impl Collection {
    /// Imply interior mutability.
    /// Performs update operation on this collection asynchronously.
    /// Explicitly waits for result to be updated.
    pub fn update(&self, operation: CollectionUpdateOperations, wait: bool) -> CollectionResult<UpdateResult> {
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
            return Ok(UpdateResult { operation_id, status: UpdateStatus::Acknowledged });
        }

        let _res: usize = self.runtime_handle.block_on(update_handler)??;
        Ok(UpdateResult { operation_id, status: UpdateStatus::Completed })
    }

    pub fn info(&self) -> CollectionResult<CollectionInfo> {
        let segments = self.segments.read();
        let mut vectors_count = 0;
        let mut segments_count = 0;
        let mut ram_size = 0;
        let mut disk_size = 0;
        for (_idx, segment) in segments.iter() {
            segments_count += 1;
            let segment_info = segment.get().read().info();
            vectors_count += segment_info.num_vectors;
            disk_size += segment_info.disk_usage_bytes;
            ram_size += segment_info.ram_usage_bytes;
        }
        Ok(CollectionInfo {
            vectors_count,
            segments_count,
            disk_data_size: disk_size,
            ram_data_size: ram_size,
            config: self.config.clone(),
        })
    }

    pub fn search(&self, request: Arc<SearchRequest>) -> CollectionResult<Vec<ScoredPoint>> {
        return self.searcher.search(request);
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

    fn avg_vectors<'a>(vectors: impl Iterator<Item=&'a Vec<VectorElementType>>) -> Vec<VectorElementType> {
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
                description: format!("At least one positive vector ID required")
            });
        }

        let reference_vectors_ids = request.positive
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
                    missed_point_id: point_id
                });
            }
        }

        let avg_positive = Collection::avg_vectors(request.positive
            .iter()
            .map(|vid| vectors_map.get(vid).unwrap()));

        let search_vector = if request.negative.is_empty() {
            avg_positive
        } else {
            let avg_negative = Collection::avg_vectors(request.negative
                .iter()
                .map(|vid| vectors_map.get(vid).unwrap()));

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
                    Some(filter) => Some(vec![Condition::Filter(filter)])
                },
                must_not: Some(vec![Condition::HasId(reference_vectors_ids.iter().cloned().collect())]),
            }),
            params: request.params.clone(),
            top: request.top,
        };

        self.search(Arc::new(search_request))
    }
}

impl Drop for Collection {
    fn drop(&mut self) {
        self.stop().unwrap(); // Finishes update tasks right before destructor stucks to do so with runtime
    }
}