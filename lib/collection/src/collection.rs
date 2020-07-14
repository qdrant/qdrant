use thiserror::Error;
use crate::operations::CollectionUpdateOperations;
use segment::types::{VectorElementType, Filter, PointIdType, ScoreType, SearchParams, Distance};
use serde::{Deserialize, Serialize};
use std::result;
use crate::operations::index_def::Indexes;


#[derive(Error, Debug)]
pub enum UpdateError {
    #[error("Vector inserting error: expected dim: {expected_dim}, got {received_dim}")]
    BadInput { expected_dim: usize, received_dim: usize },
    #[error("No point with id {missed_point_id} found")]
    NotFound { missed_point_id: PointIdType },
    #[error("Service internal error: {error}")]
    ServiceError { error: String },
    #[error("Bad request: {description}")]
    BadRequest { description:  String }
}


pub type OperationResult<T> = result::Result<T, UpdateError>;


#[derive(Debug, Deserialize, Serialize)]
pub struct CollectionInfo {
    vectors_count: usize,
    segments_count: usize,
    data_size: usize,
    index: Indexes,
    distance: Distance
}

/// Collection holds information about segments and WAL.
pub trait Collection {

    /// Imply interior mutability.
    fn update(&self, operation: CollectionUpdateOperations) -> OperationResult<bool>;

    fn info(&self) -> OperationResult<CollectionInfo>;

    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize,
              params: Option<&SearchParams>
    ) -> Vec<(PointIdType, ScoreType)>;
}

