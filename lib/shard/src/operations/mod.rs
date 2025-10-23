pub mod payload_ops;
pub mod point_ops;
pub mod vector_ops;

use segment::json_path::JsonPath;
use segment::types::{PayloadFieldSchema, PointIdType};
use serde::{Deserialize, Serialize};
use strum::{EnumDiscriminants, EnumIter};

use crate::PeerId;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, EnumDiscriminants, Hash)]
#[strum_discriminants(derive(EnumIter))]
#[serde(untagged, rename_all = "snake_case")]
pub enum CollectionUpdateOperations {
    PointOperation(point_ops::PointOperations),
    VectorOperation(vector_ops::VectorOperations),
    PayloadOperation(payload_ops::PayloadOps),
    FieldIndexOperation(FieldIndexOperations),
}

impl CollectionUpdateOperations {
    pub fn is_upsert_points(&self) -> bool {
        matches!(
            self,
            Self::PointOperation(point_ops::PointOperations::UpsertPoints(_))
        )
    }

    pub fn is_delete_points(&self) -> bool {
        matches!(
            self,
            Self::PointOperation(point_ops::PointOperations::DeletePoints { .. })
        )
    }

    pub fn point_ids(&self) -> Option<Vec<PointIdType>> {
        match self {
            Self::PointOperation(op) => op.point_ids(),
            Self::VectorOperation(op) => op.point_ids(),
            Self::PayloadOperation(op) => op.point_ids(),
            Self::FieldIndexOperation(_) => None,
        }
    }

    pub fn retain_point_ids<F>(&mut self, filter: F)
    where
        F: Fn(&PointIdType) -> bool,
    {
        match self {
            Self::PointOperation(op) => op.retain_point_ids(filter),
            Self::VectorOperation(op) => op.retain_point_ids(filter),
            Self::PayloadOperation(op) => op.retain_point_ids(filter),
            Self::FieldIndexOperation(_) => (),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, EnumDiscriminants, Hash)]
#[strum_discriminants(derive(EnumIter))]
#[serde(rename_all = "snake_case")]
pub enum FieldIndexOperations {
    /// Create index for payload field
    CreateIndex(CreateIndex),
    /// Delete index for the field
    DeleteIndex(JsonPath),
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Hash)]
#[serde(rename_all = "snake_case")]
pub struct CreateIndex {
    pub field_name: JsonPath,
    pub field_schema: Option<PayloadFieldSchema>,
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

pub type ClockToken = u64;

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
        let api::grpc::qdrant::ClockTag {
            peer_id,
            clock_id,
            clock_tick,
            token,
            force,
        } = tag;
        Self {
            peer_id,
            clock_id,
            clock_tick,
            token,
            force,
        }
    }
}

impl From<ClockTag> for api::grpc::qdrant::ClockTag {
    fn from(tag: ClockTag) -> Self {
        let ClockTag {
            peer_id,
            clock_id,
            clock_tick,
            token,
            force,
        } = tag;
        Self {
            peer_id,
            clock_id,
            clock_tick,
            token,
            force,
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
            let update = Self::UpdateVectors(UpdateVectorsOp {
                points: Vec::new(),
                update_filter: None,
            });

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
