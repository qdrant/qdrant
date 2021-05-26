use serde::{Deserialize, Serialize};
use schemars::{JsonSchema};
use segment::types::{PointIdType, PayloadKeyType, PayloadInterface};
use crate::operations::types::VectorType;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct PointStruct {
    /// Point id
    pub id: PointIdType,
    /// Vector
    pub vector: VectorType,
    /// Payload values (optional)
    pub payload: Option<HashMap<PayloadKeyType, PayloadInterface>>,
}


#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PointInsertOperations {
    #[serde(rename = "batch")]
    /// Inset points from a batch.
    BatchPoints {
        ids: Vec<PointIdType>,
        vectors: Vec<VectorType>,
        payloads: Option<Vec<Option<HashMap<PayloadKeyType, PayloadInterface>>>>,
    },
    #[serde(rename = "points")]
    /// Insert points from a list
    PointsList(Vec<PointStruct>),
}


#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PointOperations {
    /// Insert or update points
    UpsertPoints(PointInsertOperations),
    /// Delete point if exists
    DeletePoints {
        ids: Vec<PointIdType>,
    },
}
