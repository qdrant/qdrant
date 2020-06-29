use super::common::CollectionUpdateInfo;
use serde::{Deserialize, Serialize};
use segment::types::{PointIdType};
use crate::operations::types::VectorType;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PointOps {
    /// Delete point if exists
    UpsertPoints { 
        collection: CollectionUpdateInfo,
        ids: Vec<PointIdType>,
        vectors: Vec<VectorType>,
    },
    /// Insert or update points
    DeletePoints {
        collection: CollectionUpdateInfo,
        ids: Vec<PointIdType>,
    },
}
