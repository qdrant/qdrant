use serde::{Deserialize, Serialize};
use segment::types::{PointIdType, PayloadKeyType};
use crate::operations::types::VectorType;
use std::collections::HashMap;
use crate::operations::payload_ops::PayloadInterface;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PointStruct {
    pub id: PointIdType,
    pub vector: VectorType,
    pub payload: Option<HashMap<PayloadKeyType, PayloadInterface>>,
}


#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PointInsertOps {
    #[serde(rename = "batch")]
    BatchPoints {
        ids: Vec<PointIdType>,
        vectors: Vec<VectorType>,
        payloads: Option<Vec<Option<HashMap<PayloadKeyType, PayloadInterface>>>>,
    },
    #[serde(rename = "points")]
    PointsList(Vec<PointStruct>),
}


#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PointOps {
    /// Insert or update points
    UpsertPoints(PointInsertOps),
    /// Delete point if exists
    DeletePoints {
        ids: Vec<PointIdType>,
    },
}
