pub mod config_diff;
mod conversions;
pub mod payload_ops;
pub mod point_ops;
pub mod snapshot_ops;
pub mod types;

use std::collections::HashMap;

use crate::ShardId;
use hashring::HashRing;
use schemars::JsonSchema;
use segment::types::{ExtendedPointId, PayloadSchemaType};
use serde::{Deserialize, Serialize};

use self::types::CollectionResult;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub struct CreateIndex {
    pub field_name: String,
    pub field_type: Option<PayloadSchemaType>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub enum FieldIndexOperations {
    /// Create index for payload field
    CreateIndex(CreateIndex),
    /// Delete index for the field
    DeleteIndex(String),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum CollectionUpdateOperations {
    PointOperation(point_ops::PointOperations),
    PayloadOperation(payload_ops::PayloadOps),
    FieldIndexOperation(FieldIndexOperations),
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

/// Stateless validation of operation content.
/// Checks for `CollectionError::BadInput`
pub trait Validate {
    fn validate(&self) -> CollectionResult<()>;
}

impl Validate for CollectionUpdateOperations {
    fn validate(&self) -> CollectionResult<()> {
        match self {
            CollectionUpdateOperations::PointOperation(operation) => operation.validate(),
            CollectionUpdateOperations::PayloadOperation(_) => Ok(()),
            CollectionUpdateOperations::FieldIndexOperation(_) => Ok(()),
        }
    }
}

fn point_to_shard(point_id: ExtendedPointId, ring: &HashRing<ShardId>) -> ShardId {
    *ring
        .get(&point_id)
        .expect("Hash ring is guaranteed to be non-empty")
}

/// Split iterator of items that have point ids by shard
fn split_iter_by_shard<I, F, O>(
    iter: I,
    id_extractor: F,
    ring: &HashRing<ShardId>,
) -> OperationToShard<Vec<O>>
where
    I: IntoIterator<Item = O>,
    F: Fn(&O) -> ExtendedPointId,
{
    let mut op_vec_by_shard: HashMap<ShardId, Vec<O>> = HashMap::new();
    for operation in iter {
        let shard_id = point_to_shard(id_extractor(&operation), ring);
        op_vec_by_shard
            .entry(shard_id)
            .or_insert_with(Vec::new)
            .push(operation);
    }
    OperationToShard::by_shard(op_vec_by_shard)
}

/// Trait for Operation enums to split them by shard.
pub trait SplitByShard {
    fn split_by_shard(self, ring: &HashRing<ShardId>) -> OperationToShard<Self>
    where
        Self: Sized;
}

impl SplitByShard for CollectionUpdateOperations {
    fn split_by_shard(self, ring: &HashRing<ShardId>) -> OperationToShard<Self> {
        match self {
            CollectionUpdateOperations::PointOperation(operation) => operation
                .split_by_shard(ring)
                .map(CollectionUpdateOperations::PointOperation),
            CollectionUpdateOperations::PayloadOperation(operation) => operation
                .split_by_shard(ring)
                .map(CollectionUpdateOperations::PayloadOperation),
            operation @ CollectionUpdateOperations::FieldIndexOperation(_) => {
                OperationToShard::to_all(operation)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_deserialize() {
        let op =
            CollectionUpdateOperations::PayloadOperation(payload_ops::PayloadOps::ClearPayload {
                points: vec![1.into(), 2.into(), 3.into()],
            });

        let json = serde_json::to_string_pretty(&op).unwrap();
        println!("{}", json)
    }
}
