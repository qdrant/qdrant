use crate::operations::types::VectorType;
use schemars::JsonSchema;
use segment::types::{Filter, PayloadInterface, PayloadKeyType, PointIdType};
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
pub struct Batch {
    pub ids: Vec<PointIdType>,
    pub vectors: Vec<VectorType>,
    pub payloads: Option<Vec<Option<HashMap<PayloadKeyType, PayloadInterface>>>>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct PointsBatch {
    pub batch: Batch,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct PointsList {
    pub points: Vec<PointStruct>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct PointIdsList {
    pub points: Vec<PointIdType>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct FilterSelector {
    pub filter: Filter,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum PointsSelector {
    /// Select points by list of IDs
    PointIdsSelector(PointIdsList),
    /// Select points by filtering condition
    FilterSelector(FilterSelector),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum PointInsertOperations {
    /// Inset points from a batch.
    PointsBatch(PointsBatch),
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
    /// Delete points by given filter criteria
    DeletePointsByFilter(Filter),
}

impl From<Batch> for PointInsertOperations {
    fn from(batch: Batch) -> Self {
        PointInsertOperations::PointsBatch(PointsBatch { batch })
    }
}

impl From<Vec<PointStruct>> for PointInsertOperations {
    fn from(points: Vec<PointStruct>) -> Self {
        PointInsertOperations::PointsList(PointsList { points })
    }
}

impl From<Batch> for PointOperations {
    fn from(batch: Batch) -> Self {
        PointOperations::UpsertPoints(PointInsertOperations::PointsBatch(PointsBatch { batch }))
    }
}

impl From<Vec<PointStruct>> for PointOperations {
    fn from(points: Vec<PointStruct>) -> Self {
        PointOperations::UpsertPoints(PointInsertOperations::PointsList(PointsList { points }))
    }
}
