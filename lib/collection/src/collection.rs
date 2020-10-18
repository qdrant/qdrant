use thiserror::Error;
use crate::operations::CollectionUpdateOperations;
use segment::types::{PointIdType, ScoredPoint, SeqNumberType, SegmentConfig};
use std::result;
use crate::operations::types::{Record, CollectionInfo, UpdateResult, UpdateStatus, SearchRequest};
use std::sync::Arc;
use crate::wal::{SerdeWal, WalError};
use crate::segment_manager::segment_managers::{SegmentSearcher, SegmentUpdater};
use segment::entry::entry_point::OperationError;
use tokio::task::JoinError;
use tokio::runtime::Handle;
use crossbeam_channel::{Sender, SendError};
use crate::update_handler::update_handler::UpdateHandler;
use parking_lot::{Mutex, RwLock};
use crate::segment_manager::holders::segment_holder::SegmentHolder;


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

pub type OperationResult<T> = result::Result<T, CollectionError>;

pub struct Collection {
    pub segments: Arc<RwLock<SegmentHolder>>,
    pub config: SegmentConfig,
    pub wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    pub searcher: Arc<dyn SegmentSearcher + Sync + Send>,
    pub update_handler: Arc<UpdateHandler>,
    pub updater: Arc<dyn SegmentUpdater + Sync + Send>,
    pub runtime_handle: Handle,
    pub update_sender: Sender<SeqNumberType>,
}


/// Collection holds information about segments and WAL.
impl Collection {
    /// Imply interior mutability.
    /// Performs update operation on this collection asynchronously.
    /// Explicitly waits for result to be updated.
    pub fn update(&self, operation: CollectionUpdateOperations, wait: bool) -> OperationResult<UpdateResult> {
        let operation_id = self.wal.lock().write(&operation)?;

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
