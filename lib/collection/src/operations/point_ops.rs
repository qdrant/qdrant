use crate::operations::types::VectorType;
use schemars::JsonSchema;
use segment::types::{PayloadInterface, PayloadKeyType, PointIdType};
use serde::{Deserialize, Serialize};
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
pub struct BatchPoints {
    pub ids: Vec<PointIdType>,
    pub vectors: Vec<VectorType>,
    pub payloads: Option<Vec<Option<HashMap<PayloadKeyType, PayloadInterface>>>>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct BatchInsertOperation {
    pub batch: BatchPoints,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct PointsList {
    pub points: Vec<PointStruct>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum PointInsertOperations {
    /// Inset points from a batch.
    BatchPoints(BatchInsertOperation),
    /// Insert points from a list
    PointsList(PointsList),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PointOperations {
    /// Insert or update points
    UpsertPoints(PointInsertOperations),
    /// Delete point if exists
    DeletePoints { ids: Vec<PointIdType> },
}

impl From<BatchPoints> for PointInsertOperations {
    fn from(batch: BatchPoints) -> Self {
        PointInsertOperations::BatchPoints(BatchInsertOperation { batch })
    }
}

impl From<Vec<PointStruct>> for PointInsertOperations {
    fn from(points: Vec<PointStruct>) -> Self {
        PointInsertOperations::PointsList(PointsList { points })
    }
}

impl From<BatchPoints> for PointOperations {
    fn from(batch: BatchPoints) -> Self {
        PointOperations::UpsertPoints(PointInsertOperations::BatchPoints(BatchInsertOperation {
            batch,
        }))
    }
}

impl From<Vec<PointStruct>> for PointOperations {
    fn from(points: Vec<PointStruct>) -> Self {
        PointOperations::UpsertPoints(PointInsertOperations::PointsList(PointsList { points }))
    }
}
