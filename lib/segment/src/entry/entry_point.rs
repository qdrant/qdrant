use crate::types::{
    Filter, PayloadKeyType, PayloadKeyTypeRef, PayloadType, PointIdType, ScoredPoint, SearchParams,
    SegmentConfig, SegmentInfo, SegmentType, SeqNumberType, TheMap, VectorElementType, WithPayload,
};
use atomicwrites::Error as AtomicIoError;
use rocksdb::Error;
use std::io::Error as IoError;
use std::result;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
#[error("{0}")]
pub enum OperationError {
    #[error("Vector inserting error: expected dim: {expected_dim}, got {received_dim}")]
    WrongVector {
        expected_dim: usize,
        received_dim: usize,
    },
    #[error("No point with id {missed_point_id} found")]
    PointIdError { missed_point_id: PointIdType },
    #[error("Payload type does not match with previously given for field {field_name}. Expected: {expected_type}")]
    TypeError {
        field_name: PayloadKeyType,
        expected_type: String,
    },
    #[error("Service runtime error: {description}")]
    ServiceError { description: String },
}

/// Contains information regarding last operation error, which should be fixed before next operation could be processed
#[derive(Debug, Clone)]
pub struct SegmentFailedState {
    pub version: SeqNumberType,
    pub point_id: Option<PointIdType>,
    pub error: OperationError,
}

impl<E> From<AtomicIoError<E>> for OperationError {
    fn from(err: AtomicIoError<E>) -> Self {
        match err {
            AtomicIoError::Internal(io_err) => OperationError::from(io_err),
            AtomicIoError::User(_user_err) => OperationError::ServiceError {
                description: "Unknown atomic write error".to_owned(),
            },
        }
    }
}

impl From<IoError> for OperationError {
    fn from(err: IoError) -> Self {
        OperationError::ServiceError {
            description: format!("{}", err),
        }
    }
}

impl From<Error> for OperationError {
    fn from(err: Error) -> Self {
        OperationError::ServiceError {
            description: format!("persistence error: {}", err),
        }
    }
}

impl From<serde_json::Error> for OperationError {
    fn from(err: serde_json::Error) -> Self {
        OperationError::ServiceError {
            description: format!("Json error: {}", err),
        }
    }
}

pub type OperationResult<T> = result::Result<T, OperationError>;

pub fn get_service_error<T>(err: &OperationResult<T>) -> Option<OperationError> {
    match err {
        Ok(_) => None,
        Err(error) => match error {
            OperationError::ServiceError { .. } => Some(error.clone()),
            _ => None,
        },
    }
}

/// Define all operations which can be performed with Segment or Segment-like entity.
/// Assume, that all operations are idempotent - which means that
///     no matter how much time they will consequently executed - storage state will be the same.
pub trait SegmentEntry {
    /// Get current update version of the segment
    fn version(&self) -> SeqNumberType;

    /// Get version of specified point
    fn point_version(&self, point_id: PointIdType) -> Option<SeqNumberType>;

    fn search(
        &self,
        vector: &[VectorElementType],
        with_payload: &WithPayload,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> OperationResult<Vec<ScoredPoint>>;

    fn upsert_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vector: &[VectorElementType],
    ) -> OperationResult<bool>;

    fn delete_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool>;

    fn set_full_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        full_payload: TheMap<PayloadKeyType, PayloadType>,
    ) -> OperationResult<bool>;

    fn set_full_payload_with_json(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        full_payload: &str,
    ) -> OperationResult<bool>;

    fn set_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        key: PayloadKeyTypeRef,
        payload: PayloadType,
    ) -> OperationResult<bool>;

    fn delete_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<bool>;

    fn clear_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool>;

    fn vector(&self, point_id: PointIdType) -> OperationResult<Vec<VectorElementType>>;

    fn payload(
        &self,
        point_id: PointIdType,
    ) -> OperationResult<TheMap<PayloadKeyType, PayloadType>>;

    fn iter_points(&self) -> Box<dyn Iterator<Item = PointIdType> + '_>;

    /// Paginate over points which satisfies filtering condition starting with `offset` id including.
    fn read_filtered<'a>(
        &'a self,
        offset: PointIdType,
        limit: usize,
        filter: Option<&'a Filter>,
    ) -> Vec<PointIdType>;

    /// Check if there is point with `point_id` in this segment.
    fn has_point(&self, point_id: PointIdType) -> bool;

    /// Return number of vectors in this segment
    fn vectors_count(&self) -> usize;

    /// Number of vectors, marked as deleted
    fn deleted_count(&self) -> usize;

    /// Get segment type
    fn segment_type(&self) -> SegmentType;

    /// Get current stats of the segment
    fn info(&self) -> SegmentInfo;

    /// Get segment configuration
    fn config(&self) -> SegmentConfig;

    /// Get current stats of the segment
    fn is_appendable(&self) -> bool;

    /// Flushes current segment state into a persistent storage, if possible
    /// Returns maximum version number which is guaranteed to be persisted.
    fn flush(&self) -> OperationResult<SeqNumberType>;

    /// Removes all persisted data and forces to destroy segment
    fn drop_data(&mut self) -> OperationResult<()>;

    /// Delete field index, if exists
    fn delete_field_index(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<bool>;

    /// Create index for a payload field, if not exists
    fn create_field_index(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<bool>;

    /// Get indexed fields
    fn get_indexed_fields(&self) -> Vec<PayloadKeyType>;

    /// Checks if segment errored during last operations
    fn check_error(&self) -> Option<SegmentFailedState>;
}
