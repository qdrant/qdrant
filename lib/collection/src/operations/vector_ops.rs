use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use schemars::JsonSchema;
use segment::data_types::vectors::VectorStruct;
use segment::types::{Filter, PointIdType};
use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError};

use super::point_ops::PointIdsList;
use super::{point_to_shard, split_iter_by_shard, OperationToShard, SplitByShard};
use crate::hash_ring::HashRing;
use crate::operations::shard_key_selector::ShardKeySelector;
use crate::shards::shard::ShardId;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
pub struct UpdateVectors {
    /// Points with named vectors
    #[validate]
    #[validate(length(min = 1, message = "must specify points to update"))]
    pub points: Vec<PointVectors>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
pub struct PointVectors {
    /// Point id
    pub id: PointIdType,
    /// Vectors
    #[serde(alias = "vectors")]
    #[validate(custom(
        function = "validate_vector_struct_not_empty",
        message = "must specify vectors to update for point"
    ))]
    pub vector: VectorStruct,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct DeleteVectors {
    /// Deletes values from each point in this list
    pub points: Option<Vec<PointIdType>>,
    /// Deletes values from points that satisfy this filter condition
    pub filter: Option<Filter>,
    /// Vector names
    #[serde(alias = "vectors")]
    #[validate(length(min = 1, message = "must specify vector names to delete"))]
    pub vector: HashSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

#[derive(Debug, Deserialize, Serialize, Validate, Clone)]
pub struct UpdateVectorsOp {
    /// Points with named vectors
    #[validate]
    #[validate(length(min = 1, message = "must specify points to update"))]
    pub points: Vec<PointVectors>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum VectorOperations {
    /// Update vectors
    UpdateVectors(UpdateVectorsOp),
    /// Delete vectors if exists
    DeleteVectors(PointIdsList, Vec<String>),
    /// Delete vectors by given filter criteria
    DeleteVectorsByFilter(Filter, Vec<String>),
}

impl VectorOperations {
    pub fn is_write_operation(&self) -> bool {
        match self {
            VectorOperations::UpdateVectors(_) => true,
            VectorOperations::DeleteVectors(..) => false,
            VectorOperations::DeleteVectorsByFilter(..) => false,
        }
    }
}

impl Validate for VectorOperations {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            VectorOperations::UpdateVectors(update_vectors) => update_vectors.validate(),
            VectorOperations::DeleteVectors(..) => Ok(()),
            VectorOperations::DeleteVectorsByFilter(..) => Ok(()),
        }
    }
}

impl SplitByShard for Vec<PointVectors> {
    fn split_by_shard(self, ring: &HashRing<ShardId>) -> OperationToShard<Self> {
        split_iter_by_shard(self, |point| point.id, ring)
    }
}

impl SplitByShard for VectorOperations {
    fn split_by_shard(self, ring: &HashRing<ShardId>) -> OperationToShard<Self> {
        match self {
            VectorOperations::UpdateVectors(update_vectors) => {
                let shard_points = update_vectors
                    .points
                    .into_iter()
                    .map(|point| {
                        let shard_id = point_to_shard(point.id, ring);
                        (shard_id, point)
                    })
                    .fold(
                        HashMap::new(),
                        |mut map: HashMap<u32, Vec<PointVectors>>, (shard_id, points)| {
                            map.entry(shard_id).or_default().push(points);
                            map
                        },
                    );
                let shard_ops = shard_points.into_iter().map(|(shard_id, points)| {
                    (
                        shard_id,
                        VectorOperations::UpdateVectors(UpdateVectorsOp { points }),
                    )
                });
                OperationToShard::by_shard(shard_ops)
            }
            VectorOperations::DeleteVectors(ids, vector_names) => {
                split_iter_by_shard(ids.points, |id| *id, ring)
                    .map(|ids| VectorOperations::DeleteVectors(ids.into(), vector_names.clone()))
            }
            by_filter @ VectorOperations::DeleteVectorsByFilter(..) => {
                OperationToShard::to_all(by_filter)
            }
        }
    }
}

/// Validate the vector struct is not empty.
fn validate_vector_struct_not_empty(value: &VectorStruct) -> Result<(), ValidationError> {
    if !value.is_empty() {
        return Ok(());
    }

    let mut err = ValidationError::new("length");
    err.add_param(Cow::from("min"), &1);
    Err(err)
}
