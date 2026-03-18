use std::collections::HashSet;

use ahash::AHashMap;
use api::rest::{PointVectors, ShardKeySelector};
use schemars::JsonSchema;
use segment::types::{Filter, PointIdType, VectorNameBuf};
use serde::{Deserialize, Serialize};
pub use shard::operations::vector_ops::*;
use validator::Validate;

use super::{OperationToShard, SplitByShard, point_to_shards, split_iter_by_shard};
use crate::hash_ring::HashRingRouter;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct DeleteVectors {
    /// Deletes values from each point in this list
    pub points: Option<Vec<PointIdType>>,
    /// Deletes values from points that satisfy this filter condition
    #[validate(nested)]
    pub filter: Option<Filter>,
    /// Vector names
    #[serde(alias = "vectors")]
    #[validate(length(min = 1, message = "must specify vector names to delete"))]
    pub vector: HashSet<VectorNameBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

impl SplitByShard for Vec<PointVectors> {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
        split_iter_by_shard(self, |point| point.id, ring)
    }
}

impl SplitByShard for VectorOperations {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
        match self {
            VectorOperations::UpdateVectors(UpdateVectorsOp {
                points,
                update_filter,
            }) => {
                let shard_points = points
                    .into_iter()
                    .flat_map(|point| {
                        point_to_shards(&point.id, ring)
                            .into_iter()
                            .map(move |shard_id| (shard_id, point.clone()))
                    })
                    .fold(
                        AHashMap::new(),
                        |mut map: AHashMap<u32, Vec<PointVectorsPersisted>>, (shard_id, points)| {
                            map.entry(shard_id).or_default().push(points);
                            map
                        },
                    );
                let shard_ops = shard_points.into_iter().map(|(shard_id, points)| {
                    (
                        shard_id,
                        VectorOperations::UpdateVectors(UpdateVectorsOp {
                            points,
                            update_filter: update_filter.clone(),
                        }),
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
