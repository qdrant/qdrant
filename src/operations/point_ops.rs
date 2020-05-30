use serde::{Serialize, Deserialize};
use crate::common::types::{PointIdType, VectorType};
use super::common::CollectionUpdateInfo;


/// Insert or update points
#[derive(Debug, Deserialize, Serialize)]
struct UpsertPoints {
  collection: CollectionUpdateInfo,
  ids: Vec<PointIdType>,
  vectors: Vec<VectorType>
}

/// Delete point if exists
#[derive(Debug, Deserialize, Serialize)]
struct DeletePoints {
  collection: CollectionUpdateInfo,
  ids: Vec<PointIdType>,
}