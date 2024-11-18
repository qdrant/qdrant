use std::collections::{HashMap, HashSet};

use api::rest::schema::ShardKeySelector;
use api::rest::PointVectors;
use schemars::JsonSchema;
use segment::types::{Filter, PointIdType};
use serde::{Deserialize, Serialize};
use strum::{EnumDiscriminants, EnumIter};
use validator::Validate;

use super::point_ops::{PointIdsList, VectorStructPersisted};
use super::{point_to_shards, split_iter_by_shard, OperationToShard, SplitByShard};
use crate::hash_ring::HashRingRouter;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct PointVectorsPersisted {
    /// Point id
    pub id: PointIdType,
    /// Vectors
    pub vector: VectorStructPersisted,
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

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct UpdateVectorsOp {
    /// Points with named vectors
    pub points: Vec<PointVectorsPersisted>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, EnumDiscriminants)]
#[strum_discriminants(derive(EnumIter))]
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

    pub fn point_ids(&self) -> Option<Vec<PointIdType>> {
        match self {
            Self::UpdateVectors(op) => Some(op.points.iter().map(|point| point.id).collect()),
            Self::DeleteVectors(points, _) => Some(points.points.clone()),
            Self::DeleteVectorsByFilter(_, _) => None,
        }
    }

    pub fn retain_point_ids<F>(&mut self, filter: F)
    where
        F: Fn(&PointIdType) -> bool,
    {
        match self {
            Self::UpdateVectors(op) => op.points.retain(|point| filter(&point.id)),
            Self::DeleteVectors(points, _) => points.points.retain(filter),
            Self::DeleteVectorsByFilter(_, _) => (),
        }
    }
}

impl SplitByShard for Vec<PointVectors> {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
        split_iter_by_shard(self, |point| point.id, ring)
    }
}

impl SplitByShard for VectorOperations {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
        match self {
            VectorOperations::UpdateVectors(update_vectors) => {
                let shard_points = update_vectors
                    .points
                    .into_iter()
                    .flat_map(|point| {
                        point_to_shards(&point.id, ring)
                            .into_iter()
                            .map(move |shard_id| (shard_id, point.clone()))
                    })
                    .fold(
                        HashMap::new(),
                        |mut map: HashMap<u32, Vec<PointVectorsPersisted>>, (shard_id, points)| {
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
