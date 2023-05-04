use std::collections::HashSet;

use schemars::JsonSchema;
use segment::data_types::vectors::VectorStruct;
use segment::types::{Filter, PointIdType};
use serde::{Deserialize, Serialize};
use validator::Validate;

use super::point_ops::{PointIdsList, PointsSelector};
use super::{point_to_shard, split_iter_by_shard, OperationToShard, SplitByShard};
use crate::hash_ring::HashRing;
use crate::shards::shard::ShardId;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
pub struct UpdateVectors {
    /// Point id
    pub id: PointIdType,
    /// Vectors
    #[serde(alias = "vectors")]
    pub vector: VectorStruct,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct DeleteVectors {
    /// Point selector
    pub point_selector: PointsSelector,
    /// Vectors
    #[serde(alias = "vectors")]
    #[validate(length(min = 1))]
    pub vector: HashSet<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum VectorOperations {
    /// Update vectors
    UpdateVectors(UpdateVectors),
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

impl SplitByShard for VectorOperations {
    fn split_by_shard(self, ring: &HashRing<ShardId>) -> OperationToShard<Self> {
        match self {
            VectorOperations::UpdateVectors(update_vectors) => {
                let shard_id = point_to_shard(update_vectors.id, ring);
                OperationToShard::by_shard([(
                    shard_id,
                    VectorOperations::UpdateVectors(update_vectors),
                )])
            }
            VectorOperations::DeleteVectors(ids, vector_names) => {
                split_iter_by_shard(ids.points, |id| *id, ring).map(|ids| {
                    let points_ids_list = PointIdsList { points: ids };
                    VectorOperations::DeleteVectors(points_ids_list, vector_names.clone())
                })
            }
            by_filter @ VectorOperations::DeleteVectorsByFilter(..) => {
                OperationToShard::to_all(by_filter)
            }
        }
    }
}
