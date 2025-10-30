use std::borrow::Cow;

use api::rest::{LookupLocation, SearchRequestInternal};
use collection::collection::distance_matrix::CollectionSearchMatrixRequest;
use collection::grouping::group_by::{GroupRequest, SourceRequest};
use collection::lookup::WithLookup;
use collection::operations::CollectionUpdateOperations;
use collection::operations::types::{
    CoreSearchRequest, CountRequestInternal, DiscoverRequestInternal, PointRequestInternal,
    RecommendRequestInternal, ScrollRequestInternal,
};
use collection::operations::universal_query::collection_query::{
    CollectionPrefetch, CollectionQueryRequest,
};
use segment::data_types::facets::FacetParams;

use super::{Access, AccessRequirements, CollectionAccessList, CollectionPass};
use crate::content_manager::collection_meta_ops::CollectionMetaOperations;
use crate::content_manager::errors::{StorageError, StorageResult};

impl Access {
    #[allow(private_bounds)]
    pub(crate) fn check_point_op<'a>(
        &self,
        collection_name: &'a str,
        op: &impl CheckableCollectionOperation,
    ) -> Result<CollectionPass<'a>, StorageError> {
        let requirements = op.access_requirements();
        match self {
            Access::Global(mode) => mode.meets_requirements(requirements)?,
            Access::Collection(list) => {
                let view = list.find_view(collection_name)?;
                view.meets_requirements(requirements)?;
                op.check_access(list)?;
            }
        }
        Ok(CollectionPass(Cow::Borrowed(collection_name)))
    }

    pub(crate) fn check_collection_meta_operation(
        &self,
        operation: &CollectionMetaOperations,
    ) -> Result<(), StorageError> {
        match operation {
            CollectionMetaOperations::CreateCollection(_)
            | CollectionMetaOperations::UpdateCollection(_)
            | CollectionMetaOperations::DeleteCollection(_)
            | CollectionMetaOperations::ChangeAliases(_)
            | CollectionMetaOperations::Resharding(_, _)
            | CollectionMetaOperations::TransferShard(_, _)
            | CollectionMetaOperations::SetShardReplicaState(_)
            | CollectionMetaOperations::CreateShardKey(_)
            | CollectionMetaOperations::DropShardKey(_) => {
                self.check_global_access(AccessRequirements::new().manage())?;
            }
            CollectionMetaOperations::CreatePayloadIndex(op) => {
                self.check_collection_access(
                    &op.collection_name,
                    AccessRequirements::new().write().extras(),
                )?;
            }
            CollectionMetaOperations::DropPayloadIndex(op) => {
                self.check_collection_access(
                    &op.collection_name,
                    AccessRequirements::new().write().extras(),
                )?;
            }
            CollectionMetaOperations::Nop { token: _ } => (),
        }
        Ok(())
    }
}

trait CheckableCollectionOperation {
    /// Used to distinguish whether the operation is read-only or read-write.
    fn access_requirements(&self) -> AccessRequirements;

    fn check_access(&self, access: &CollectionAccessList) -> Result<(), StorageError>;
}

impl CollectionAccessList {
    fn check_lookup_from(
        &self,
        lookup_location: &Option<LookupLocation>,
    ) -> Result<(), StorageError> {
        if let Some(lookup_location) = lookup_location {
            self.find_view(&lookup_location.collection)?;
        }
        Ok(())
    }

    fn check_with_lookup(&self, with_lookup: &Option<WithLookup>) -> Result<(), StorageError> {
        if let Some(with_lookup) = with_lookup {
            self.find_view(&with_lookup.collection_name)?;
        }
        Ok(())
    }
}

impl CheckableCollectionOperation for SearchRequestInternal {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            extras: false,
        }
    }

    fn check_access(&self, _access: &CollectionAccessList) -> Result<(), StorageError> {
        Ok(())
    }
}

impl CheckableCollectionOperation for RecommendRequestInternal {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            extras: false,
        }
    }

    fn check_access(&self, access: &CollectionAccessList) -> Result<(), StorageError> {
        access.check_lookup_from(&self.lookup_from)?;
        Ok(())
    }
}

impl CheckableCollectionOperation for PointRequestInternal {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            extras: false,
        }
    }

    fn check_access(&self, _access: &CollectionAccessList) -> Result<(), StorageError> {
        Ok(())
    }
}

impl CheckableCollectionOperation for CoreSearchRequest {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            extras: false,
        }
    }

    fn check_access(&self, _access: &CollectionAccessList) -> Result<(), StorageError> {
        Ok(())
    }
}

impl CheckableCollectionOperation for CountRequestInternal {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            extras: false,
        }
    }

    fn check_access(&self, _access: &CollectionAccessList) -> Result<(), StorageError> {
        Ok(())
    }
}

impl CheckableCollectionOperation for GroupRequest {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            extras: false,
        }
    }

    fn check_access(&self, access: &CollectionAccessList) -> Result<(), StorageError> {
        match &self.source {
            SourceRequest::Search(s) => s.check_access(access)?,
            SourceRequest::Recommend(r) => r.check_access(access)?,
            SourceRequest::Query(q) => q.check_access(access)?,
        }
        access.check_with_lookup(&self.with_lookup)?;
        Ok(())
    }
}

impl CheckableCollectionOperation for DiscoverRequestInternal {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            extras: false,
        }
    }

    fn check_access(&self, access: &CollectionAccessList) -> Result<(), StorageError> {
        access.check_lookup_from(&self.lookup_from)?;
        Ok(())
    }
}

impl CheckableCollectionOperation for ScrollRequestInternal {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            extras: false,
        }
    }

    fn check_access(&self, _access: &CollectionAccessList) -> Result<(), StorageError> {
        Ok(())
    }
}

impl CheckableCollectionOperation for CollectionQueryRequest {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            extras: false,
        }
    }

    fn check_access(&self, access: &CollectionAccessList) -> Result<(), StorageError> {
        access.check_lookup_from(&self.lookup_from)?;

        for prefetch_query in self.prefetch.iter() {
            check_access_for_prefetch(prefetch_query, access)?;
        }

        Ok(())
    }
}

fn check_access_for_prefetch(
    prefetch: &CollectionPrefetch,
    access: &CollectionAccessList,
) -> Result<(), StorageError> {
    access.check_lookup_from(&prefetch.lookup_from)?;

    // Recurse inner prefetches
    for prefetch_query in prefetch.prefetch.iter() {
        check_access_for_prefetch(prefetch_query, access)?;
    }

    Ok(())
}

impl CheckableCollectionOperation for FacetParams {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            extras: false,
        }
    }

    fn check_access(&self, _access: &CollectionAccessList) -> StorageResult<()> {
        Ok(())
    }
}

impl CheckableCollectionOperation for CollectionSearchMatrixRequest {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            extras: false,
        }
    }

    fn check_access(&self, _access: &CollectionAccessList) -> StorageResult<()> {
        Ok(())
    }
}

impl CheckableCollectionOperation for CollectionUpdateOperations {
    fn access_requirements(&self) -> AccessRequirements {
        match self {
            CollectionUpdateOperations::PointOperation(_)
            | CollectionUpdateOperations::VectorOperation(_)
            | CollectionUpdateOperations::PayloadOperation(_) => AccessRequirements {
                write: true,
                manage: false,
                extras: false,
            },
            CollectionUpdateOperations::FieldIndexOperation(_) => AccessRequirements {
                write: true,
                manage: true,
                extras: true,
            },
        }
    }

    fn check_access(&self, _access: &CollectionAccessList) -> Result<(), StorageError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests_ops {
    use std::fmt::Debug;

    use api::rest::{
        self, LookupLocation, OrderByInterface, RecommendStrategy, SearchRequestInternal,
    };
    use collection::operations::payload_ops::PayloadOpsDiscriminants;
    use collection::operations::point_ops::{
        BatchPersisted, BatchVectorStructPersisted, ConditionalInsertOperationInternal,
        PointInsertOperationsInternal, PointInsertOperationsInternalDiscriminants,
        PointOperationsDiscriminants, PointStructPersisted, PointSyncOperation,
        VectorStructPersisted,
    };
    use collection::operations::query_enum::QueryEnum;
    use collection::operations::types::{ContextExamplePair, RecommendExample, UsingVector};
    use collection::operations::vector_ops::{
        PointVectorsPersisted, UpdateVectorsOp, VectorOperationsDiscriminants,
    };
    use collection::operations::{
        CollectionUpdateOperationsDiscriminants, CreateIndex, FieldIndexOperations,
        FieldIndexOperationsDiscriminants,
    };
    use segment::data_types::vectors::NamedQuery;
    use segment::types::{
        Condition, ExtendedPointId, Filter, Payload, PointIdType, SearchParams,
        WithPayloadInterface, WithVector,
    };
    use shard::operations::payload_ops::{DeletePayloadOp, PayloadOps, SetPayloadOp};
    use shard::operations::point_ops::{PointIdsList, PointOperations};
    use shard::operations::vector_ops::VectorOperations;
    use strum::IntoEnumIterator as _;

    use super::*;
    use crate::rbac::{AccessCollectionBuilder, GlobalAccessMode};

    /// Create a `must` filter from a list of point IDs.
    #[cfg(test)]
    fn make_filter_from_ids(ids: Vec<ExtendedPointId>) -> Filter {
        let cond = ids.into_iter().collect::<ahash::AHashSet<_>>().into();
        Filter {
            must: Some(vec![Condition::HasId(cond)]),
            ..Default::default()
        }
    }

    /// Operation is allowed with the given access, and no rewrite is expected.
    fn assert_allowed<Op: Debug + Clone + PartialEq + CheckableCollectionOperation>(
        op: &Op,
        access: &Access,
    ) {
        access.check_point_op("col", op).expect("Should be allowed");
    }

    /// Operation is forbidden with the given access.
    fn assert_forbidden<Op: Clone + CheckableCollectionOperation + PartialEq>(
        op: &Op,
        access: &Access,
    ) {
        access
            .check_point_op("col", op)
            .expect_err("should be forbidden");
    }

    /// Operation requires write + whole collection access.
    fn assert_requires_whole_write_access<Op>(op: &Op)
    where
        Op: CheckableCollectionOperation + Clone + Debug + PartialEq,
    {
        assert_allowed(op, &Access::Global(GlobalAccessMode::Manage));
        assert_forbidden(op, &Access::Global(GlobalAccessMode::Read));

        assert_allowed(op, &AccessCollectionBuilder::new().add("col", true).into());
        assert_forbidden(op, &AccessCollectionBuilder::new().add("col", false).into());
    }

    #[test]
    fn test_recommend_request_internal() {
        let op = RecommendRequestInternal {
            positive: vec![RecommendExample::Dense(vec![0.0, 1.0, 2.0])],
            negative: vec![RecommendExample::Sparse(vec![(0, 0.0)].try_into().unwrap())],
            strategy: Some(RecommendStrategy::AverageVector),
            filter: None,
            params: Some(SearchParams::default()),
            limit: 100,
            offset: Some(100),
            with_payload: Some(WithPayloadInterface::Bool(true)),
            with_vector: Some(WithVector::Bool(true)),
            score_threshold: Some(42.0),
            using: Some(UsingVector::Name("vector".into())),
            lookup_from: Some(LookupLocation {
                collection: "col2".to_string(),
                vector: Some("vector".into()),
                shard_key: None,
            }),
        };

        assert_allowed(&op, &Access::Global(GlobalAccessMode::Manage));
        assert_allowed(&op, &Access::Global(GlobalAccessMode::Read));

        // Point ID is used
        assert_forbidden(
            &RecommendRequestInternal {
                positive: vec![RecommendExample::PointId(ExtendedPointId::NumId(12345))],
                ..op.clone()
            },
            &AccessCollectionBuilder::new().add("col2", false).into(),
        );

        // lookup_from requires read access
        assert_forbidden(
            &op,
            &AccessCollectionBuilder::new().add("col", false).into(),
        );
        assert_allowed(
            &RecommendRequestInternal {
                lookup_from: None,
                ..op
            },
            &AccessCollectionBuilder::new().add("col", false).into(),
        );
    }

    #[test]
    fn test_point_request_internal() {
        let op = PointRequestInternal {
            ids: vec![PointIdType::NumId(12345)],
            with_payload: None,
            with_vector: WithVector::Bool(true),
        };

        assert_allowed(&op, &Access::Global(GlobalAccessMode::Manage));
        assert_allowed(&op, &Access::Global(GlobalAccessMode::Read));

        assert_forbidden(&op, &AccessCollectionBuilder::new().into());

        assert_allowed(
            &op,
            &AccessCollectionBuilder::new().add("col", false).into(),
        );
    }

    #[test]
    fn test_core_search_request() {
        let op = CoreSearchRequest {
            query: QueryEnum::Nearest(NamedQuery::default_dense(vec![0.0, 1.0, 2.0])),
            filter: None,
            params: Some(SearchParams::default()),
            limit: 100,
            offset: 100,
            with_payload: Some(WithPayloadInterface::Bool(true)),
            with_vector: Some(WithVector::Bool(true)),
            score_threshold: Some(42.0),
        };

        assert_allowed(&op, &Access::Global(GlobalAccessMode::Manage));
        assert_allowed(&op, &Access::Global(GlobalAccessMode::Read));

        assert_allowed(
            &op,
            &AccessCollectionBuilder::new().add("col", false).into(),
        );
    }

    #[test]
    fn test_count_request_internal() {
        let op = CountRequestInternal {
            filter: None,
            exact: false,
        };

        assert_allowed(&op, &Access::Global(GlobalAccessMode::Manage));
        assert_allowed(&op, &Access::Global(GlobalAccessMode::Read));

        assert_allowed(
            &op,
            &AccessCollectionBuilder::new().add("col", false).into(),
        );
    }

    #[test]
    fn test_group_request_source() {
        let op = GroupRequest {
            // NOTE: SourceRequest::Recommend is already tested in test_recommend_request_internal
            source: SourceRequest::Search(SearchRequestInternal {
                vector: rest::NamedVectorStruct::Default(vec![0.0, 1.0, 2.0]),
                filter: None,
                params: Some(SearchParams::default()),
                limit: 100,
                offset: Some(100),
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vector: Some(WithVector::Bool(true)),
                score_threshold: Some(42.0),
            }),
            group_by: "path".parse().unwrap(),
            group_size: 100,
            limit: 100,
            with_lookup: Some(WithLookup {
                collection_name: "col2".to_string(),
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vectors: Some(WithVector::Bool(true)),
            }),
        };

        assert_allowed(&op, &Access::Global(GlobalAccessMode::Manage));
        assert_allowed(&op, &Access::Global(GlobalAccessMode::Read));

        assert_allowed(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false)
                .add("col2", false)
                .into(),
        );

        // with_lookup requires whole read access
        assert_forbidden(
            &op,
            &AccessCollectionBuilder::new().add("col", false).into(),
        );
        assert_allowed(
            &GroupRequest {
                with_lookup: None,
                ..op.clone()
            },
            &AccessCollectionBuilder::new().add("col", false).into(),
        );
    }

    #[test]
    fn test_discover_request_internal() {
        let op = DiscoverRequestInternal {
            target: Some(RecommendExample::Dense(vec![0.0, 1.0, 2.0])),
            context: Some(vec![ContextExamplePair {
                positive: RecommendExample::Dense(vec![0.0, 1.0, 2.0]),
                negative: RecommendExample::Dense(vec![0.0, 1.0, 2.0]),
            }]),
            filter: None,
            params: Some(SearchParams::default()),
            limit: 100,
            offset: Some(100),
            with_payload: Some(WithPayloadInterface::Bool(true)),
            with_vector: Some(WithVector::Bool(true)),
            using: Some(UsingVector::Name("vector".into())),
            lookup_from: Some(LookupLocation {
                collection: "col2".to_string(),
                vector: Some("vector".into()),
                shard_key: None,
            }),
        };

        assert_allowed(&op, &Access::Global(GlobalAccessMode::Manage));
        assert_allowed(&op, &Access::Global(GlobalAccessMode::Read));

        assert_allowed(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false)
                .add("col2", false)
                .into(),
        );

        // Point ID is used
        assert_forbidden(
            &DiscoverRequestInternal {
                target: Some(RecommendExample::PointId(ExtendedPointId::NumId(12345))),
                ..op.clone()
            },
            &AccessCollectionBuilder::new().add("col2", false).into(),
        );
        assert_forbidden(
            &DiscoverRequestInternal {
                context: Some(vec![ContextExamplePair {
                    positive: RecommendExample::PointId(ExtendedPointId::NumId(12345)),
                    negative: RecommendExample::Dense(vec![0.0, 1.0, 2.0]),
                }]),
                ..op.clone()
            },
            &AccessCollectionBuilder::new().add("col2", false).into(),
        );
        assert_forbidden(
            &DiscoverRequestInternal {
                context: Some(vec![ContextExamplePair {
                    positive: RecommendExample::Dense(vec![0.0, 1.0, 2.0]),
                    negative: RecommendExample::PointId(ExtendedPointId::NumId(12345)),
                }]),
                ..op.clone()
            },
            &AccessCollectionBuilder::new().add("col2", false).into(),
        );

        // lookup_from requires read access
        assert_forbidden(
            &op,
            &AccessCollectionBuilder::new().add("col", false).into(),
        );
        assert_allowed(
            &DiscoverRequestInternal {
                lookup_from: None,
                ..op
            },
            &AccessCollectionBuilder::new().add("col", false).into(),
        );
    }

    #[test]
    fn test_scroll_request_internal() {
        let op = ScrollRequestInternal {
            offset: Some(ExtendedPointId::NumId(12345)),
            limit: Some(100),
            filter: None,
            with_payload: Some(WithPayloadInterface::Bool(true)),
            with_vector: WithVector::Bool(true),
            order_by: Some(OrderByInterface::Key("path".parse().unwrap())),
        };

        assert_allowed(&op, &Access::Global(GlobalAccessMode::Manage));
        assert_allowed(&op, &Access::Global(GlobalAccessMode::Read));

        assert_allowed(
            &op,
            &AccessCollectionBuilder::new().add("col", false).into(),
        );
    }

    #[test]
    fn test_collection_update_operations() {
        CollectionUpdateOperationsDiscriminants::iter().for_each(|discr| match discr {
            CollectionUpdateOperationsDiscriminants::PointOperation => {
                check_collection_update_operations_points()
            }
            CollectionUpdateOperationsDiscriminants::VectorOperation => {
                check_collection_update_operations_update_vectors()
            }
            CollectionUpdateOperationsDiscriminants::PayloadOperation => {
                check_collection_update_operations_payload()
            }
            CollectionUpdateOperationsDiscriminants::FieldIndexOperation => {
                check_collection_update_operations_field_index()
            }
        });
    }

    /// Tests for [`CollectionUpdateOperations::PointOperation`].
    fn check_collection_update_operations_points() {
        PointOperationsDiscriminants::iter().for_each(|discr| match discr {
            PointOperationsDiscriminants::UpsertPoints => {
                for discr in PointInsertOperationsInternalDiscriminants::iter() {
                    let inner = match discr {
                        PointInsertOperationsInternalDiscriminants::PointsBatch => {
                            PointInsertOperationsInternal::PointsBatch(BatchPersisted {
                                ids: vec![ExtendedPointId::NumId(12345)],
                                vectors: BatchVectorStructPersisted::Single(vec![vec![
                                    0.0, 1.0, 2.0,
                                ]]),
                                payloads: None,
                            })
                        }
                        PointInsertOperationsInternalDiscriminants::PointsList => {
                            PointInsertOperationsInternal::PointsList(vec![PointStructPersisted {
                                id: ExtendedPointId::NumId(12345),
                                vector: VectorStructPersisted::Single(vec![0.0, 1.0, 2.0]),
                                payload: None,
                            }])
                        }
                    };

                    let op = CollectionUpdateOperations::PointOperation(
                        PointOperations::UpsertPoints(inner),
                    );
                    assert_requires_whole_write_access(&op);
                }
            }
            PointOperationsDiscriminants::UpsertPointsConditional => {
                let inner = PointInsertOperationsInternal::PointsList(vec![PointStructPersisted {
                    id: ExtendedPointId::NumId(12345),
                    vector: VectorStructPersisted::Single(vec![0.0, 1.0, 2.0]),
                    payload: None,
                }]);

                let filter = make_filter_from_ids(vec![ExtendedPointId::NumId(12345)]);

                let op = CollectionUpdateOperations::PointOperation(
                    PointOperations::UpsertPointsConditional(ConditionalInsertOperationInternal {
                        points_op: inner,
                        condition: filter,
                    }),
                );

                assert_requires_whole_write_access(&op);
            }

            PointOperationsDiscriminants::DeletePoints => {
                let op =
                    CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
                        ids: vec![ExtendedPointId::NumId(12345)],
                    });
                check_collection_update_operations_delete_points(&op);
            }

            PointOperationsDiscriminants::DeletePointsByFilter => {
                let op = CollectionUpdateOperations::PointOperation(
                    PointOperations::DeletePointsByFilter(make_filter_from_ids(vec![
                        ExtendedPointId::NumId(12345),
                    ])),
                );
                check_collection_update_operations_delete_points(&op);
            }

            PointOperationsDiscriminants::SyncPoints => {
                let op = CollectionUpdateOperations::PointOperation(PointOperations::SyncPoints(
                    PointSyncOperation {
                        from_id: None,
                        to_id: None,
                        points: Vec::new(),
                    },
                ));
                assert_requires_whole_write_access(&op);
            }
        });
    }

    /// Tests for [`CollectionUpdateOperations::PointOperation`] with
    /// [`PointOperations::DeletePoints`] and [`PointOperations::DeletePointsByFilter`].
    fn check_collection_update_operations_delete_points(op: &CollectionUpdateOperations) {
        assert_allowed(op, &Access::Global(GlobalAccessMode::Manage));
        assert_forbidden(op, &Access::Global(GlobalAccessMode::Read));

        assert_allowed(op, &AccessCollectionBuilder::new().add("col", true).into());
        assert_forbidden(op, &AccessCollectionBuilder::new().add("col", false).into());
    }

    /// Tests for [`CollectionUpdateOperations::VectorOperation`].
    fn check_collection_update_operations_update_vectors() {
        VectorOperationsDiscriminants::iter().for_each(|discr| match discr {
            VectorOperationsDiscriminants::UpdateVectors => {
                let op = CollectionUpdateOperations::VectorOperation(
                    VectorOperations::UpdateVectors(UpdateVectorsOp {
                        points: vec![PointVectorsPersisted {
                            id: ExtendedPointId::NumId(12345),
                            vector: VectorStructPersisted::Single(vec![0.0, 1.0, 2.0]),
                        }],
                        update_filter: None,
                    }),
                );
                assert_requires_whole_write_access(&op);
            }
            VectorOperationsDiscriminants::DeleteVectors => {
                let op =
                    CollectionUpdateOperations::VectorOperation(VectorOperations::DeleteVectors(
                        PointIdsList {
                            points: vec![ExtendedPointId::NumId(12345)],
                            shard_key: None,
                        },
                        vec!["vector".into()],
                    ));
                check_collection_update_operations_delete_vectors(&op);
            }
            VectorOperationsDiscriminants::DeleteVectorsByFilter => {
                let op = CollectionUpdateOperations::VectorOperation(
                    VectorOperations::DeleteVectorsByFilter(
                        make_filter_from_ids(vec![ExtendedPointId::NumId(12345)]),
                        vec!["vector".into()],
                    ),
                );
                check_collection_update_operations_delete_vectors(&op);
            }
        });
    }

    /// Tests for [`CollectionUpdateOperations::VectorOperation`] with
    /// [`VectorOperations::DeleteVectors`] and [`VectorOperations::DeleteVectorsByFilter`].
    fn check_collection_update_operations_delete_vectors(op: &CollectionUpdateOperations) {
        assert_allowed(op, &Access::Global(GlobalAccessMode::Manage));
        assert_forbidden(op, &Access::Global(GlobalAccessMode::Read));

        assert_allowed(op, &AccessCollectionBuilder::new().add("col", true).into());
        assert_forbidden(op, &AccessCollectionBuilder::new().add("col", false).into());
    }

    /// Tests for [`CollectionUpdateOperations::PayloadOperation`].
    fn check_collection_update_operations_payload() {
        for discr in PayloadOpsDiscriminants::iter() {
            let inner = match discr {
                PayloadOpsDiscriminants::SetPayload => PayloadOps::SetPayload(SetPayloadOp {
                    payload: Payload::default(),
                    points: Some(vec![ExtendedPointId::NumId(12345)]),
                    filter: None,
                    key: None,
                }),
                PayloadOpsDiscriminants::DeletePayload => {
                    PayloadOps::DeletePayload(DeletePayloadOp {
                        keys: vec!["path".parse().unwrap()],
                        points: Some(vec![ExtendedPointId::NumId(12345)]),
                        filter: None,
                    })
                }
                PayloadOpsDiscriminants::ClearPayload => PayloadOps::ClearPayload {
                    points: vec![ExtendedPointId::NumId(12345)],
                },
                PayloadOpsDiscriminants::ClearPayloadByFilter => {
                    PayloadOps::ClearPayloadByFilter(make_filter_from_ids(vec![
                        ExtendedPointId::NumId(12345),
                    ]))
                }
                PayloadOpsDiscriminants::OverwritePayload => {
                    PayloadOps::OverwritePayload(SetPayloadOp {
                        payload: Payload::default(),
                        points: Some(vec![ExtendedPointId::NumId(12345)]),
                        filter: None,
                        key: None,
                    })
                }
            };

            let op = CollectionUpdateOperations::PayloadOperation(inner);
            assert_requires_whole_write_access(&op);
        }
    }

    /// Tests for [`CollectionUpdateOperations::FieldIndexOperation`].
    fn check_collection_update_operations_field_index() {
        for discr in FieldIndexOperationsDiscriminants::iter() {
            let inner = match discr {
                FieldIndexOperationsDiscriminants::CreateIndex => {
                    FieldIndexOperations::CreateIndex(CreateIndex {
                        field_name: "path".parse().unwrap(),
                        field_schema: None,
                    })
                }
                FieldIndexOperationsDiscriminants::DeleteIndex => {
                    FieldIndexOperations::DeleteIndex("path".parse().unwrap())
                }
            };

            let op = CollectionUpdateOperations::FieldIndexOperation(inner);
            assert_allowed(&op, &Access::Global(GlobalAccessMode::Manage));
            assert_forbidden(&op, &Access::Global(GlobalAccessMode::Read));
            assert_forbidden(&op, &AccessCollectionBuilder::new().add("col", true).into());
            assert_forbidden(
                &op,
                &AccessCollectionBuilder::new().add("col", false).into(),
            );
        }
    }
}
