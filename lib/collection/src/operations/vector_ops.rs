use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use api::rest::schema::VectorStruct;
use schemars::JsonSchema;
use segment::types::{Filter, PointIdType};
use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError, ValidationErrors};

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

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct PointVectors {
    /// Point id
    pub id: PointIdType,
    /// Vectors
    #[serde(alias = "vectors")]
    pub vector: VectorStruct,
}

impl Validate for PointVectors {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        if self.vector.is_empty() {
            let mut err = ValidationError::new("length");
            err.message = Some(Cow::from("must specify vectors to update for point"));
            err.add_param(Cow::from("min"), &1);
            let mut errors = ValidationErrors::new();
            errors.add("vector", err);
            Err(errors)
        } else {
            self.vector.validate()
        }
    }
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

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Validate)]
pub struct UpdateVectorsOp {
    /// Points with named vectors
    #[validate]
    #[validate(length(min = 1, message = "must specify points to update"))]
    pub points: Vec<PointVectors>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
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
