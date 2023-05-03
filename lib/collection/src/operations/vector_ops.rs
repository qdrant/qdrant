use std::collections::HashSet;

use schemars::JsonSchema;
use segment::data_types::vectors::VectorStruct;
use segment::types::PointIdType;
use serde::{Deserialize, Serialize};
use validator::Validate;

use super::point_ops::PointsSelector;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
pub struct UpdateVectors {
    /// Point id
    pub id: PointIdType,
    /// Vectors
    #[serde(alias = "vectors")]
    pub vector: VectorStruct,
}

type VectorNameList = HashSet<String>;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct DeleteVectors {
    /// Point selector
    pub point_selector: PointsSelector,
    /// Vectors
    // TODO: validate cannot be empty?
    #[serde(alias = "vectors")]
    pub vector: VectorNameList,
}
