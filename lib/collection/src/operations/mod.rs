pub mod cluster_ops;
pub mod config_diff;
pub mod consistency_params;
pub mod conversions;
pub mod generalizer;
pub mod loggable;
pub mod operation_effect;
pub mod payload_ops;
pub mod point_ops;
pub mod shard_selector_internal;
pub mod shared_storage_config;
pub mod snapshot_ops;
pub mod snapshot_storage_ops;
pub mod types;
pub mod universal_query;
pub mod validation;
pub mod vector_ops;
pub mod vector_params_builder;
pub mod verification;

pub mod query_enum {
    pub use shard::query::query_enum::QueryEnum;
}

use std::collections::HashMap;

use segment::types::ExtendedPointId;
pub use shard::operations::*;

use crate::hash_ring::{HashRingRouter, ShardIds};
use crate::shards::shard::ShardId;

/// Trait for Operation enums to split them by shard.
pub trait SplitByShard {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self>
    where
        Self: Sized;
}

impl SplitByShard for CollectionUpdateOperations {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
        match self {
            CollectionUpdateOperations::PointOperation(operation) => operation
                .split_by_shard(ring)
                .map(CollectionUpdateOperations::PointOperation),
            CollectionUpdateOperations::VectorOperation(operation) => operation
                .split_by_shard(ring)
                .map(CollectionUpdateOperations::VectorOperation),
            CollectionUpdateOperations::PayloadOperation(operation) => operation
                .split_by_shard(ring)
                .map(CollectionUpdateOperations::PayloadOperation),
            operation @ CollectionUpdateOperations::FieldIndexOperation(_) => {
                OperationToShard::to_all(operation)
            }
        }
    }
}

/// A mapping of operation to shard.
/// Is a result of splitting one operation into several shards by corresponding PointIds
pub enum OperationToShard<O> {
    ByShard(Vec<(ShardId, O)>),
    ToAll(O),
}

impl<O> OperationToShard<O> {
    pub fn by_shard(operations: impl IntoIterator<Item = (ShardId, O)>) -> Self {
        Self::ByShard(operations.into_iter().collect())
    }

    pub fn to_none() -> Self {
        Self::ByShard(Vec::new())
    }

    pub fn to_all(operation: O) -> Self {
        Self::ToAll(operation)
    }

    pub fn map<O2>(self, f: impl Fn(O) -> O2) -> OperationToShard<O2> {
        match self {
            OperationToShard::ByShard(operation_to_shard) => OperationToShard::ByShard(
                operation_to_shard
                    .into_iter()
                    .map(|(id, operation)| (id, f(operation)))
                    .collect(),
            ),
            OperationToShard::ToAll(to_all) => OperationToShard::ToAll(f(to_all)),
        }
    }
}

/// Split iterator of items that have point ids by shard
fn split_iter_by_shard<I, F, O: Clone>(
    iter: I,
    id_extractor: F,
    ring: &HashRingRouter,
) -> OperationToShard<Vec<O>>
where
    I: IntoIterator<Item = O>,
    F: Fn(&O) -> ExtendedPointId,
{
    let mut op_vec_by_shard: HashMap<ShardId, Vec<O>> = HashMap::new();
    for operation in iter {
        for shard_id in point_to_shards(&id_extractor(&operation), ring) {
            op_vec_by_shard
                .entry(shard_id)
                .or_default()
                .push(operation.clone());
        }
    }
    OperationToShard::by_shard(op_vec_by_shard)
}

/// Get the shards for a point ID
///
/// Normally returns a single shard ID. Might return multiple if resharding is currently in
/// progress.
///
/// # Panics
///
/// Panics if the hash ring is empty and there is no shard for the given point ID.
fn point_to_shards(point_id: &ExtendedPointId, ring: &HashRingRouter) -> ShardIds {
    let shard_ids = ring.get(point_id);
    assert!(
        !shard_ids.is_empty(),
        "Hash ring is guaranteed to be non-empty",
    );
    shard_ids
}
