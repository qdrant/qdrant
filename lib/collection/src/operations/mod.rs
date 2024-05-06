pub mod cluster_ops;
pub mod config_diff;
pub mod consistency_params;
pub mod conversions;
pub mod conversions_rest;
pub mod operation_effect;
pub mod payload_ops;
pub mod point_ops;
pub mod query_enum;
pub mod shard_key_selector;
pub mod shard_selector_internal;
pub mod shared_storage_config;
pub mod snapshot_ops;
pub mod types;
pub mod validation;
pub mod vector_ops;
pub mod vector_params_builder;
pub mod universal_query;

use std::collections::HashMap;

use segment::json_path::JsonPath;
use segment::types::{ExtendedPointId, PayloadFieldSchema};
use serde::{Deserialize, Serialize};
use strum::{EnumDiscriminants, EnumIter};
use validator::Validate;

use crate::hash_ring::HashRing;
use crate::shards::shard::{PeerId, ShardId};

pub type ClockToken = u64;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Validate)]
#[serde(rename_all = "snake_case")]
pub struct CreateIndex {
    pub field_name: JsonPath,
    pub field_schema: Option<PayloadFieldSchema>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, EnumDiscriminants)]
#[strum_discriminants(derive(EnumIter))]
#[serde(rename_all = "snake_case")]
pub enum FieldIndexOperations {
    /// Create index for payload field
    CreateIndex(CreateIndex),
    /// Delete index for the field
    DeleteIndex(JsonPath),
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct OperationWithClockTag {
    #[serde(flatten)]
    pub operation: CollectionUpdateOperations,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clock_tag: Option<ClockTag>,
}

impl OperationWithClockTag {
    pub fn new(
        operation: impl Into<CollectionUpdateOperations>,
        clock_tag: Option<ClockTag>,
    ) -> Self {
        Self {
            operation: operation.into(),
            clock_tag,
        }
    }
}

impl From<CollectionUpdateOperations> for OperationWithClockTag {
    fn from(operation: CollectionUpdateOperations) -> Self {
        Self::new(operation, None)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ClockTag {
    pub peer_id: PeerId,
    pub clock_id: u32,
    pub clock_tick: u64,
    /// A unique token for each clock tag.
    pub token: ClockToken,
    pub force: bool,
}

impl ClockTag {
    pub fn new(peer_id: PeerId, clock_id: u32, clock_tick: u64) -> Self {
        let random_token = rand::random();
        Self::new_with_token(peer_id, clock_id, clock_tick, random_token)
    }

    pub fn new_with_token(
        peer_id: PeerId,
        clock_id: u32,
        clock_tick: u64,
        token: ClockToken,
    ) -> Self {
        Self {
            peer_id,
            clock_id,
            clock_tick,
            token,
            force: false,
        }
    }

    pub fn force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }
}

impl From<api::grpc::qdrant::ClockTag> for ClockTag {
    fn from(tag: api::grpc::qdrant::ClockTag) -> Self {
        Self {
            peer_id: tag.peer_id,
            clock_id: tag.clock_id,
            clock_tick: tag.clock_tick,
            token: tag.token,
            force: tag.force,
        }
    }
}

impl From<ClockTag> for api::grpc::qdrant::ClockTag {
    fn from(tag: ClockTag) -> Self {
        Self {
            peer_id: tag.peer_id,
            clock_id: tag.clock_id,
            clock_tick: tag.clock_tick,
            token: tag.token,
            force: tag.force,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, EnumDiscriminants)]
#[strum_discriminants(derive(EnumIter))]
#[serde(untagged, rename_all = "snake_case")]
pub enum CollectionUpdateOperations {
    PointOperation(point_ops::PointOperations),
    VectorOperation(vector_ops::VectorOperations),
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

impl FieldIndexOperations {
    pub fn is_write_operation(&self) -> bool {
        match self {
            FieldIndexOperations::CreateIndex(_) => true,
            FieldIndexOperations::DeleteIndex(_) => false,
        }
    }
}

impl Validate for FieldIndexOperations {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            FieldIndexOperations::CreateIndex(create_index) => create_index.validate(),
            FieldIndexOperations::DeleteIndex(_) => Ok(()),
        }
    }
}

impl Validate for CollectionUpdateOperations {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            CollectionUpdateOperations::PointOperation(operation) => operation.validate(),
            CollectionUpdateOperations::VectorOperation(operation) => operation.validate(),
            CollectionUpdateOperations::PayloadOperation(operation) => operation.validate(),
            CollectionUpdateOperations::FieldIndexOperation(operation) => operation.validate(),
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
        op_vec_by_shard.entry(shard_id).or_default().push(operation);
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

impl CollectionUpdateOperations {
    pub fn is_write_operation(&self) -> bool {
        match self {
            CollectionUpdateOperations::PointOperation(operation) => operation.is_write_operation(),
            CollectionUpdateOperations::VectorOperation(operation) => {
                operation.is_write_operation()
            }
            CollectionUpdateOperations::PayloadOperation(operation) => {
                operation.is_write_operation()
            }
            CollectionUpdateOperations::FieldIndexOperation(operation) => {
                operation.is_write_operation()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use segment::types::*;

    use super::payload_ops::*;
    use super::point_ops::*;
    use super::vector_ops::*;
    use super::*;

    proptest::proptest! {
        #[test]
        fn operation_with_clock_tag_json(operation in any::<OperationWithClockTag>()) {
            // Assert that `OperationWithClockTag` can be serialized
            let input = serde_json::to_string(&operation).unwrap();
            let output: OperationWithClockTag = serde_json::from_str(&input).unwrap();
            assert_eq!(operation, output);

            // Assert that `OperationWithClockTag` can be deserialized from `CollectionUpdateOperation`
            let input = serde_json::to_string(&operation.operation).unwrap();
            let output: OperationWithClockTag = serde_json::from_str(&input).unwrap();
            assert_eq!(operation.operation, output.operation);

            // Assert that `CollectionUpdateOperation` serializes into JSON object with a single key
            // (e.g., `{ "upsert_points": <upsert points object> }`)
            match serde_json::to_value(&operation.operation).unwrap() {
                serde_json::Value::Object(map) if map.len() == 1 => (),
                _ => panic!("TODO"),
            };
        }

        #[test]
        fn operation_with_clock_tag_cbor(operation in any::<OperationWithClockTag>()) {
            // Assert that `OperationWithClockTag` can be serialized
            let input = serde_cbor::to_vec(&operation).unwrap();
            let output: OperationWithClockTag = serde_cbor::from_slice(&input).unwrap();
            assert_eq!(operation, output);

            // Assert that `OperationWithClockTag` can be deserialized from `CollectionUpdateOperation`
            let input = serde_cbor::to_vec(&operation.operation).unwrap();
            let output: OperationWithClockTag = serde_cbor::from_slice(&input).unwrap();
            assert_eq!(operation.operation, output.operation);
        }
    }

    impl Arbitrary for OperationWithClockTag {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            any::<(CollectionUpdateOperations, Option<ClockTag>)>()
                .prop_map(|(operation, clock_tag)| Self::new(operation, clock_tag))
                .boxed()
        }
    }

    impl Arbitrary for ClockTag {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            any::<(PeerId, u32, u64)>()
                .prop_map(|(peer_id, clock_id, clock_tick)| {
                    Self::new(peer_id, clock_id, clock_tick)
                })
                .boxed()
        }
    }

    impl Arbitrary for CollectionUpdateOperations {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                any::<point_ops::PointOperations>().prop_map(Self::PointOperation),
                any::<vector_ops::VectorOperations>().prop_map(Self::VectorOperation),
                any::<payload_ops::PayloadOps>().prop_map(Self::PayloadOperation),
                any::<FieldIndexOperations>().prop_map(Self::FieldIndexOperation),
            ]
            .boxed()
        }
    }

    impl Arbitrary for point_ops::PointOperations {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            let upsert = Self::UpsertPoints(PointInsertOperationsInternal::PointsList(Vec::new()));
            let delete = Self::DeletePoints { ids: Vec::new() };

            let delete_by_filter = Self::DeletePointsByFilter(Filter {
                should: None,
                min_should: None,
                must: None,
                must_not: None,
            });

            let sync = Self::SyncPoints(PointSyncOperation {
                from_id: None,
                to_id: None,
                points: Vec::new(),
            });

            prop_oneof![
                Just(upsert),
                Just(delete),
                Just(delete_by_filter),
                Just(sync),
            ]
            .boxed()
        }
    }

    impl Arbitrary for vector_ops::VectorOperations {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            let update = Self::UpdateVectors(UpdateVectorsOp { points: Vec::new() });

            let delete = Self::DeleteVectors(
                PointIdsList {
                    points: Vec::new(),
                    shard_key: None,
                },
                Vec::new(),
            );

            let delete_by_filter = Self::DeleteVectorsByFilter(
                Filter {
                    should: None,
                    min_should: None,
                    must: None,
                    must_not: None,
                },
                Vec::new(),
            );

            prop_oneof![Just(update), Just(delete), Just(delete_by_filter),].boxed()
        }
    }

    impl Arbitrary for payload_ops::PayloadOps {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            let set = Self::SetPayload(SetPayloadOp {
                payload: Payload(Default::default()),
                points: None,
                filter: None,
                key: None,
            });

            let overwrite = Self::OverwritePayload(SetPayloadOp {
                payload: Payload(Default::default()),
                points: None,
                filter: None,
                key: None,
            });

            let delete = Self::DeletePayload(DeletePayloadOp {
                keys: Vec::new(),
                points: None,
                filter: None,
            });

            let clear = Self::ClearPayload { points: Vec::new() };

            let clear_by_filter = Self::ClearPayloadByFilter(Filter {
                should: None,
                min_should: None,
                must: None,
                must_not: None,
            });

            prop_oneof![
                Just(set),
                Just(overwrite),
                Just(delete),
                Just(clear),
                Just(clear_by_filter),
            ]
            .boxed()
        }
    }

    impl Arbitrary for FieldIndexOperations {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            let create = Self::CreateIndex(CreateIndex {
                field_name: "field_name".parse().unwrap(),
                field_schema: None,
            });

            let delete = Self::DeleteIndex("field_name".parse().unwrap());

            prop_oneof![Just(create), Just(delete),].boxed()
        }
    }
}
