pub mod operation_name;
pub mod optimization;
pub mod payload_ops;
pub mod point_ops;
#[cfg(feature = "staging")]
pub mod staging;
pub mod vector_name_ops;
pub mod vector_ops;

use std::collections::HashSet;

use segment::json_path::JsonPath;
use segment::types::{PayloadFieldSchema, PointIdType, VectorNameBuf};
use serde::{Deserialize, Serialize};
use strum::{EnumDiscriminants, EnumIter};

pub use self::vector_name_ops::{
    CreateVectorName, DeleteVectorName, VectorNameConfig, VectorNameOperations,
};
use crate::PeerId;
use crate::operations::point_ops::PointOperations;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, EnumDiscriminants, Hash)]
#[strum_discriminants(derive(EnumIter))]
#[serde(untagged, rename_all = "snake_case")]
pub enum CollectionUpdateOperations {
    PointOperation(point_ops::PointOperations),
    VectorOperation(vector_ops::VectorOperations),
    PayloadOperation(payload_ops::PayloadOps),
    FieldIndexOperation(FieldIndexOperations),
    VectorNameOperation(VectorNameOperations),
    /// Staging-only operations for testing and debugging purposes
    #[cfg(feature = "staging")]
    StagingOperation(staging::StagingOperations),
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
            Self::VectorNameOperation(_) => None,
            #[cfg(feature = "staging")]
            Self::StagingOperation(_) => None,
        }
    }

    /// List point IDs that can be created during the operation.
    /// Do not list IDs that are deleted or modified.
    pub fn upsert_point_ids(&self) -> Option<Vec<PointIdType>> {
        match self {
            Self::PointOperation(op) => match op {
                PointOperations::UpsertPoints(op) => Some(op.point_ids()),
                PointOperations::UpsertPointsConditional(op) => Some(op.points_op.point_ids()),
                PointOperations::DeletePoints { .. } => None,
                PointOperations::DeletePointsByFilter(_) => None,
                PointOperations::SyncPoints(op) => {
                    Some(op.points.iter().map(|point| point.id).collect())
                }
            },
            Self::VectorOperation(_) => None,
            Self::PayloadOperation(_) => None,
            Self::FieldIndexOperation(_) => None,
            Self::VectorNameOperation(_) => None,
            #[cfg(feature = "staging")]
            Self::StagingOperation(_) => None,
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
            Self::VectorNameOperation(_) => (),
            #[cfg(feature = "staging")]
            Self::StagingOperation(_) => (),
        }
    }

    /// Drop named-vector references to vector names not in `valid`.
    ///
    /// Used during WAL replay: a historical operation may reference a vector name that was
    /// since removed by `delete_named_vector`. Without this, such an operation fails segment
    /// validation (`VectorNameNotExists`) and is dropped wholesale on reload, taking its
    /// points with it. Stripping the dead names lets the rest of the operation apply, matching
    /// the live outcome (the point survives, just without the deleted vector).
    ///
    /// This does not touch `VectorNameOperation` responsible for creating/deleting a named vector.
    ///
    /// Only affects the named-vector variants; the default (unnamed) vector is left untouched.
    ///
    /// Note: this is best-effort. Stripping a vector from an early operation silently changes the
    /// behavior of a later operation that depended on it (e.g. a `has_vector` filter or
    /// `UpdateVectors`), so the replayed timeline can still diverge from the live one. Tracked in
    /// <https://github.com/qdrant/qdrant/issues/9386>.
    pub fn retain_vector_names(&mut self, valid: &HashSet<VectorNameBuf>) {
        match self {
            Self::PointOperation(op) => op.retain_vector_names(valid),
            Self::VectorOperation(op) => op.retain_vector_names(valid),
            Self::PayloadOperation(_) => (),
            Self::FieldIndexOperation(_) => (),
            Self::VectorNameOperation(_) => (),
            #[cfg(feature = "staging")]
            Self::StagingOperation(_) => (),
        }
    }

    /// If this operation creates a named vector, return the name it introduces.
    ///
    /// Used during WAL replay to grow the set of valid vector names: a historical
    /// `CreateVectorName` must make its name valid for the operations that follow it in
    /// the WAL, otherwise a later upsert referencing it would be wrongly stripped by
    /// [`Self::retain_vector_names`].
    pub fn created_vector_name(&self) -> Option<&VectorNameBuf> {
        match self {
            Self::VectorNameOperation(VectorNameOperations::CreateVectorName(op)) => {
                Some(&op.vector_name)
            }
            Self::VectorNameOperation(VectorNameOperations::DeleteVectorName(_))
            | Self::PointOperation(_)
            | Self::VectorOperation(_)
            | Self::PayloadOperation(_)
            | Self::FieldIndexOperation(_) => None,
            #[cfg(feature = "staging")]
            Self::StagingOperation(_) => None,
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

#[cfg(feature = "api")]
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

#[cfg(feature = "api")]
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
    #![expect(clippy::wildcard_enum_match_arm, reason = "test code")]

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
                any::<VectorNameOperations>().prop_map(Self::VectorNameOperation),
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
                    #[cfg(feature = "api")]
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

    impl Arbitrary for VectorNameOperations {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            use crate::operations::vector_name_ops::{
                self as vnops, DenseVectorConfig, SparseVectorConfig,
            };

            let create_dense = Self::CreateVectorName(CreateVectorName {
                vector_name: "test_vector".into(),
                config: vnops::VectorNameConfig::dense(DenseVectorConfig {
                    size: 4,
                    distance: Distance::Cosine,
                    multivector_config: None,
                    datatype: None,
                }),
            });

            let create_sparse = Self::CreateVectorName(CreateVectorName {
                vector_name: "sparse_test".into(),
                config: vnops::VectorNameConfig::sparse(SparseVectorConfig {
                    modifier: None,
                    datatype: None,
                }),
            });

            let delete = Self::DeleteVectorName(DeleteVectorName {
                vector_name: "test_vector".into(),
            });

            prop_oneof![Just(create_dense), Just(create_sparse), Just(delete),].boxed()
        }
    }

    #[test]
    fn test_delete_by_filter_with_has_id_uuids_cbor_roundtrip() {
        let uuids: Vec<PointIdType> = vec![ExtendedPointId::Uuid(
            uuid::Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap(),
        )];

        let filter = Filter {
            should: None,
            min_should: None,
            must: None,
            must_not: Some(vec![Condition::HasId(HasIdCondition::from(
                uuids.into_iter().collect::<ahash::AHashSet<_>>(),
            ))]),
        };

        let operation = CollectionUpdateOperations::PointOperation(
            PointOperations::DeletePointsByFilter(filter),
        );

        let cbor_bytes = serde_cbor::to_vec(&operation).unwrap();
        let deserialized: CollectionUpdateOperations = serde_cbor::from_slice(&cbor_bytes).unwrap();

        assert_eq!(operation, deserialized);
    }

    #[test]
    fn test_wal_roundtrip_delete_by_filter_with_has_id_uuids() {
        use crate::wal::WalRawRecord;

        let uuids: Vec<PointIdType> = vec![ExtendedPointId::Uuid(
            uuid::Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap(),
        )];

        let filter = Filter {
            should: None,
            min_should: None,
            must: None,
            must_not: Some(vec![Condition::HasId(HasIdCondition::from(
                uuids.into_iter().collect::<ahash::AHashSet<_>>(),
            ))]),
        };

        let operation = CollectionUpdateOperations::PointOperation(
            PointOperations::DeletePointsByFilter(filter),
        );

        let raw = WalRawRecord::new(&operation).unwrap();
        let deserialized: CollectionUpdateOperations = raw.deserialize().unwrap();

        assert_eq!(operation, deserialized);
    }
}
