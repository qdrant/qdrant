use super::common::CollectionUpdateInfo;
use crate::common::types::{PointIdType, VectorType};
use serde::{Deserialize, Serialize};

/// Insert or update points
#[derive(Debug, Deserialize, Serialize)]
pub struct UpsertPoints {
    pub collection: CollectionUpdateInfo,
    pub ids: Vec<PointIdType>,
    pub vectors: Vec<VectorType>,
}

/// Delete point if exists
#[derive(Debug, Deserialize, Serialize)]
pub struct DeletePoints {
    pub collection: CollectionUpdateInfo,
    pub ids: Vec<PointIdType>,
}
