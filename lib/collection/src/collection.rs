use thiserror::Error;
use crate::operations::CollectionUpdateOperations;
use segment::types::{VectorElementType, Filter, PointIdType, ScoreType, SearchParams, Distance, SeqNumberType, ScoredPoint};
use serde::{Deserialize, Serialize};
use std::result;
use crate::operations::index_def::Indexes;
use crate::operations::types::{Record, CollectionInfo, UpdateResult, UpdateStatus};
use std::sync::{Arc, RwLock, Mutex, Condvar};
use crate::wal::SerdeWal;
use crossbeam_channel::Sender;
use crate::segment_manager::segment_managers::SegmentSearcher;


#[derive(Error, Debug)]
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


pub type OperationResult<T> = result::Result<T, CollectionError>;

pub struct Collection {
    wal: Arc<RwLock<SerdeWal<CollectionUpdateOperations>>>,
    /// Channel to notify updater on new operations
    notify_operation: Sender<SeqNumberType>,
    /// Condition value, used for waiting for operation processing
    processed_operation: Arc<(Mutex<SeqNumberType>, Condvar)>,
    searcher: Arc<Box<dyn SegmentSearcher>>,
}


/// Collection holds information about segments and WAL.
impl Collection {
    /// Imply interior mutability.
    /// Performs update operation on this collection asynchronously.
    /// Explicitly waits for result to be updated.
    pub fn update(&self, operation: CollectionUpdateOperations, wait: bool) -> OperationResult<UpdateResult> {
        let operation_id = match self.wal.write().unwrap().write(&operation) {
            Ok(operation_id) => operation_id,
            Err(err) => return Err(
                CollectionError::ServiceError {
                    error: format!("{}", err)
                }),
        };
        self.notify_operation.send(operation_id).unwrap();
        if !wait {
            return Ok(UpdateResult { operation_id, status: UpdateStatus::Acknowledged });
        }

        let (applied_operation, condvar) = &*self.processed_operation.clone();

        condvar.wait_while(applied_operation.lock().unwrap(), |x| *x < operation_id);

        Ok(UpdateResult { operation_id, status: UpdateStatus::Completed })
    }

    pub fn info(&self) -> OperationResult<CollectionInfo> {
        return self.searcher.info();
    }

    pub fn search(&self,
                  vector: &Vec<VectorElementType>,
                  filter: Option<&Filter>,
                  top: usize,
                  params: Option<&SearchParams>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        return self.searcher.search(vector, filter, top, params);
    }

    pub fn retrieve(
        &self,
        points: &Vec<PointIdType>,
        with_payload: bool,
        with_vector: bool,
    ) -> OperationResult<Vec<Record>> {
        return self.searcher.retrieve(points, with_payload, with_vector);
    }
}

