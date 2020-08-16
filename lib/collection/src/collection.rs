use thiserror::Error;
use crate::operations::CollectionUpdateOperations;
use segment::types::{PointIdType, ScoredPoint, SeqNumberType};
use std::result;
use crate::operations::types::{Record, CollectionInfo, UpdateResult, UpdateStatus, SearchRequest};
use std::sync::{Arc, RwLock, PoisonError};
use crate::wal::{SerdeWal, WalError};
use crate::segment_manager::segment_managers::{SegmentSearcher, SegmentUpdater};
use segment::entry::entry_point::OperationError;
use tokio::task::JoinError;
use tokio::runtime::Handle;
use crossbeam_channel::{Sender, SendError};
use crate::update_handler::update_handler::UpdateHandler;


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
            OperationError::WrongVector { .. } => Self::BadInput { description: format!("{}", err)},
            OperationError::PointIdError { missed_point_id } => Self::NotFound { missed_point_id },
        }
    }
}

impl From<JoinError> for CollectionError {
    fn from(err: JoinError) -> Self {
        Self::ServiceError { error: format!("{}", err) }
    }
}

impl<T> From<PoisonError<T>> for CollectionError {
    fn from(err: PoisonError<T>) -> Self {
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

pub type OperationResult<T> = result::Result<T, CollectionError>;

pub struct Collection {
    pub wal: Arc<RwLock<SerdeWal<CollectionUpdateOperations>>>,
    pub searcher: Arc<dyn SegmentSearcher>,
    pub update_handler: Arc<UpdateHandler>,
    pub updater: Arc<dyn SegmentUpdater + Sync + Send>,
    pub runtime_handle: Handle,
    pub update_sender: Sender<SeqNumberType>
}


/// Collection holds information about segments and WAL.
impl Collection {
    /// Imply interior mutability.
    /// Performs update operation on this collection asynchronously.
    /// Explicitly waits for result to be updated.
    pub fn update(&self, operation: CollectionUpdateOperations, wait: bool) -> OperationResult<UpdateResult> {

        let operation_id = self.wal.write().unwrap().write(&operation)?;

        let upd = self.updater.clone();
        let sndr = self.update_sender.clone();
        let update_future = async move {
            let res = upd.update(operation_id, &operation);
            sndr.send(operation_id)?;
            res
        };
        let update_handler = self.runtime_handle.spawn(update_future);

        if !wait {
            return Ok(UpdateResult { operation_id, status: UpdateStatus::Acknowledged });
        }

        let _res: usize = self.runtime_handle.block_on(update_handler)??;
        Ok(UpdateResult { operation_id, status: UpdateStatus::Completed })
    }

    pub fn info(&self) -> OperationResult<CollectionInfo> {
        return self.searcher.info();
    }

    pub fn search(&self, request: Arc<SearchRequest>) -> OperationResult<Vec<ScoredPoint>> {
        return self.searcher.search(request);
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
