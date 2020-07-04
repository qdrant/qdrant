use thiserror::Error;
use std::path::Path;
use crate::types::{SeqNumberType, VectorElementType, Filter, PointIdType, ScoreType, PayloadKeyType, PayloadType};
use std::result;


/// Trait for versionable & saveable objects.
pub trait VersionedPersistable {
    fn persist(&self, directory: &Path) -> SeqNumberType;
    fn load(directory: &Path) -> Self;

    /// Save latest persisted version in memory, so the object will not be saved too much times
    fn ack_persistance(&mut self, version: SeqNumberType);
}


#[derive(Error, Debug)]
pub enum OperationError {
    #[error("Vector inserting error: expected dim: {expected_dim}, got {received_dim}")]
    WrongVector { expected_dim: usize, received_dim: usize },
    #[error("Wrong operation ordering: segment state:{SeqNumberType}, operation: {operation_num}")]
    SeqError { current_state: SeqNumberType, operation_num: SeqNumberType},
    #[error("No point with id {missed_point_id} found")]
    PointIdError { missed_point_id: PointIdType },
    #[error("Payload `{key}` type mismatch for point {point_id}: expected: {required_type}, got {received_type}")]
    PayloadError {
        point_id: PointIdType,
        key: PayloadKeyType,
        required_type: String,
        received_type: String
    },
}

pub type Result<T> = result::Result<T, OperationError>;


/// Define all operations which can be performed with Segment.
/// Assume, that all operations are idempotent - which means that
///     no matter how much time they will consequently executed - storage state will be the same.
pub trait SegmentEntry {
    /// Get current update version of the segement
    fn version(&self) -> SeqNumberType;

    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize) -> Vec<(PointIdType, ScoreType)>;

    fn upsert_point(&mut self, op_num: SeqNumberType, point_id: PointIdType, vector: &Vec<VectorElementType>) -> Result<bool>;

    fn delete_point(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> Result<bool>;

    fn set_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType, key: PayloadKeyType, payload: PayloadType) -> Result<bool>;

    fn delete_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType, key: PayloadKeyType) -> Result<bool>;

    fn clear_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> Result<bool>;

    fn wipe_payload(&mut self, op_num: SeqNumberType) -> Result<bool>;
}

