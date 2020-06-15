use super::common::CollectionUpdateInfo;
use crate::common::types::{PointIdType, VectorType};
use serde::{Deserialize, Serialize};

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
