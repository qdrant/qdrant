use std::borrow::Cow;
use std::collections::HashSet;
use std::mem::take;

use collection::grouping::group_by::{GroupRequest, SourceRequest};
use collection::lookup::WithLookup;
use collection::operations::payload_ops::{DeletePayloadOp, PayloadOps, SetPayloadOp};
use collection::operations::point_ops::{PointIdsList, PointOperations};
use collection::operations::types::{
    ContextExamplePair, CoreSearchRequest, CountRequestInternal, DiscoverRequestInternal,
    LookupLocation, PointRequestInternal, RecommendExample, RecommendRequestInternal,
    ScrollRequestInternal,
};
use collection::operations::vector_ops::VectorOperations;
use collection::operations::CollectionUpdateOperations;
use segment::types::{Condition, ExtendedPointId, FieldCondition, Filter, Match, Payload};

use super::{
    incompatible_with_payload_constraint, Access, AccessRequirements, CollectionAccessList,
    CollectionAccessView, CollectionPass, PayloadConstraint,
};
use crate::content_manager::collection_meta_ops::CollectionMetaOperations;
use crate::content_manager::errors::StorageError;

impl Access {
    #[allow(private_bounds)]
    pub(crate) fn check_point_op<'a>(
        &self,
        collection_name: &'a str,
        op: &mut impl CheckableCollectionOperation,
    ) -> Result<CollectionPass<'a>, StorageError> {
        let requirements = op.access_requirements();
        match self {
            Access::Global(mode) => mode.meets_requirements(requirements)?,
            Access::Collection(list) => {
                let view = list.find_view(collection_name)?;
                view.meets_requirements(requirements)?;
                op.check_access(view, list)?;
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
                    AccessRequirements::new().write().whole(),
                )?;
            }
            CollectionMetaOperations::DropPayloadIndex(op) => {
                self.check_collection_access(
                    &op.collection_name,
                    AccessRequirements::new().write().whole(),
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

    fn check_access(
        &mut self,
        view: CollectionAccessView<'_>,
        access: &CollectionAccessList,
    ) -> Result<(), StorageError>;
}

impl CollectionAccessList {
    fn check_lookup_from(
        &self,
        lookup_location: &Option<LookupLocation>,
    ) -> Result<(), StorageError> {
        if let Some(lookup_location) = lookup_location {
            self.find_view(&lookup_location.collection)?
                .check_whole_access()?;
        }
        Ok(())
    }

    fn check_with_lookup(&self, with_lookup: &Option<WithLookup>) -> Result<(), StorageError> {
        if let Some(with_lookup) = with_lookup {
            self.find_view(&with_lookup.collection_name)?
                .check_whole_access()?;
        }
        Ok(())
    }
}

impl<'a> CollectionAccessView<'a> {
    fn apply_filter(&self, filter: &mut Option<Filter>) {
        if let Some(payload) = &self.payload {
            let f = filter.get_or_insert_with(Default::default);
            *f = take(f).merge_owned(payload.to_filter());
        }
    }

    fn check_recommend_example(&self, example: &RecommendExample) -> Result<(), StorageError> {
        match example {
            RecommendExample::PointId(_) => self.check_whole_access(),
            RecommendExample::Dense(_) | RecommendExample::Sparse(_) => Ok(()),
        }
    }
}

impl CheckableCollectionOperation for RecommendRequestInternal {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            whole: false,
        }
    }

    fn check_access(
        &mut self,
        view: CollectionAccessView<'_>,
        access: &CollectionAccessList,
    ) -> Result<(), StorageError> {
        for e in &self.positive {
            view.check_recommend_example(e)?;
        }
        for e in &self.negative {
            view.check_recommend_example(e)?;
        }
        access.check_lookup_from(&self.lookup_from)?;
        view.apply_filter(&mut self.filter);
        Ok(())
    }
}

impl CheckableCollectionOperation for PointRequestInternal {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            whole: true,
        }
    }

    fn check_access(
        &mut self,
        _view: CollectionAccessView<'_>,
        _access: &CollectionAccessList,
    ) -> Result<(), StorageError> {
        Ok(())
    }
}

impl CheckableCollectionOperation for CoreSearchRequest {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            whole: false,
        }
    }

    fn check_access(
        &mut self,
        view: CollectionAccessView<'_>,
        _access: &CollectionAccessList,
    ) -> Result<(), StorageError> {
        view.apply_filter(&mut self.filter);
        Ok(())
    }
}

impl CheckableCollectionOperation for CountRequestInternal {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            whole: false,
        }
    }

    fn check_access(
        &mut self,
        view: CollectionAccessView<'_>,
        _access: &CollectionAccessList,
    ) -> Result<(), StorageError> {
        view.apply_filter(&mut self.filter);
        Ok(())
    }
}

impl CheckableCollectionOperation for GroupRequest {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            whole: false,
        }
    }

    fn check_access(
        &mut self,
        view: CollectionAccessView<'_>,
        access: &CollectionAccessList,
    ) -> Result<(), StorageError> {
        match &mut self.source {
            SourceRequest::Search(s) => {
                view.apply_filter(&mut s.filter);
            }
            SourceRequest::Recommend(r) => r.check_access(view, access)?,
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
            whole: false,
        }
    }

    fn check_access(
        &mut self,
        view: CollectionAccessView<'_>,
        access: &CollectionAccessList,
    ) -> Result<(), StorageError> {
        if let Some(target) = &self.target {
            view.check_recommend_example(target)?;
        }
        for ContextExamplePair { positive, negative } in self.context.iter().flat_map(|c| c.iter())
        {
            view.check_recommend_example(positive)?;
            view.check_recommend_example(negative)?;
        }
        view.apply_filter(&mut self.filter);
        access.check_lookup_from(&self.lookup_from)?;
        Ok(())
    }
}

impl CheckableCollectionOperation for ScrollRequestInternal {
    fn access_requirements(&self) -> AccessRequirements {
        AccessRequirements {
            write: false,
            manage: false,
            whole: false,
        }
    }

    fn check_access(
        &mut self,
        view: CollectionAccessView<'_>,
        _access: &CollectionAccessList,
    ) -> Result<(), StorageError> {
        view.apply_filter(&mut self.filter);
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
                whole: false, // Checked in `check_access()`
            },
            CollectionUpdateOperations::FieldIndexOperation(_) => AccessRequirements {
                write: true,
                manage: true,
                whole: true,
            },
        }
    }

    fn check_access(
        &mut self,
        view: CollectionAccessView<'_>,
        _access: &CollectionAccessList,
    ) -> Result<(), StorageError> {
        match self {
            CollectionUpdateOperations::PointOperation(op) => match op {
                PointOperations::UpsertPoints(_) => {
                    view.check_whole_access()?;
                }
                PointOperations::DeletePoints { ids } => {
                    if let Some(payload) = &view.payload {
                        *op = PointOperations::DeletePointsByFilter(
                            make_filter_from_ids(take(ids)).merge_owned(payload.to_filter()),
                        );
                    }
                }
                PointOperations::DeletePointsByFilter(filter) => {
                    if let Some(payload) = &view.payload {
                        *filter = take(filter).merge_owned(payload.to_filter());
                    }
                }
                PointOperations::SyncPoints(_) => {
                    view.check_whole_access()?;
                }
            },

            CollectionUpdateOperations::VectorOperation(op) => match op {
                VectorOperations::UpdateVectors(_) => {
                    view.check_whole_access()?;
                }
                VectorOperations::DeleteVectors(PointIdsList { points, shard_key }, vectors) => {
                    if let Some(payload) = &view.payload {
                        if shard_key.is_some() {
                            // It is unclear where to put the shard_key
                            return incompatible_with_payload_constraint(view.collection);
                        }
                        *op = VectorOperations::DeleteVectorsByFilter(
                            make_filter_from_ids(take(points)).merge_owned(payload.to_filter()),
                            take(vectors),
                        );
                    }
                }
                VectorOperations::DeleteVectorsByFilter(filter, _) => {
                    if let Some(payload) = &view.payload {
                        *filter = take(filter).merge_owned(payload.to_filter());
                    }
                }
            },

            CollectionUpdateOperations::PayloadOperation(op) => 'a: {
                let Some(payload) = &view.payload else {
                    // Allow all operations when there is no payload constraint
                    break 'a;
                };

                match op {
                    PayloadOps::SetPayload(SetPayloadOp {
                        payload: _, // TODO: validate
                        points,
                        filter,
                        key: _, // TODO: validate
                    }) => {
                        let filter = filter.get_or_insert_with(Default::default);
                        if let Some(points) = take(points) {
                            *filter = take(filter).merge_owned(make_filter_from_ids(points));
                        }

                        // Reject as not implemented
                        return incompatible_with_payload_constraint(view.collection);
                    }
                    PayloadOps::DeletePayload(DeletePayloadOp {
                        keys: _, // TODO: validate
                        points,
                        filter,
                    }) => {
                        let filter = filter.get_or_insert_with(Default::default);
                        if let Some(points) = take(points) {
                            *filter = take(filter).merge_owned(make_filter_from_ids(points));
                        }

                        // Reject as not implemented
                        return incompatible_with_payload_constraint(view.collection);
                    }
                    PayloadOps::ClearPayload { points } => {
                        *op = PayloadOps::OverwritePayload(SetPayloadOp {
                            payload: payload.make_payload(view.collection)?,
                            points: None,
                            filter: Some(
                                make_filter_from_ids(take(points)).merge_owned(payload.to_filter()),
                            ),
                            key: None,
                        });
                    }
                    PayloadOps::ClearPayloadByFilter(filter) => {
                        *op = PayloadOps::OverwritePayload(SetPayloadOp {
                            payload: payload.make_payload(view.collection)?,
                            points: None,
                            filter: Some(take(filter).merge_owned(payload.to_filter())),
                            key: None,
                        });
                    }
                    PayloadOps::OverwritePayload(SetPayloadOp {
                        payload: _, // TODO: validate
                        points,
                        filter,
                        key: _, // TODO: validate
                    }) => {
                        let filter = filter.get_or_insert_with(Default::default);
                        if let Some(points) = take(points) {
                            *filter = take(filter).merge_owned(make_filter_from_ids(points));
                        }

                        // Reject as not implemented
                        return incompatible_with_payload_constraint(view.collection);
                    }
                }
            }

            CollectionUpdateOperations::FieldIndexOperation(_) => (),
        }
        Ok(())
    }
}

/// Create a `must` filter from a list of point IDs.
fn make_filter_from_ids(ids: Vec<ExtendedPointId>) -> Filter {
    let cond = ids.into_iter().collect::<HashSet<_>>().into();
    Filter {
        must: Some(vec![Condition::HasId(cond)]),
        ..Default::default()
    }
}

impl PayloadConstraint {
    /// Create a `must` filter.
    fn to_filter(&self) -> Filter {
        Filter {
            must: Some(
                self.0
                    .iter()
                    .map(|(path, value)| {
                        Condition::Field(FieldCondition::new_match(
                            path.clone(),
                            Match::new_value(value.clone()),
                        ))
                    })
                    .collect(),
            ),
            ..Default::default()
        }
    }

    fn make_payload(&self, collection_name: &str) -> Result<Payload, StorageError> {
        // TODO: We need to construct a payload, then validate it against the claim
        incompatible_with_payload_constraint(collection_name) // Reject as not implemented
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use segment::json_path::JsonPath;
    use segment::types::{MinShould, ValueVariants};

    use super::*;
    use crate::rbac::{CollectionAccess, CollectionAccessMode};

    #[test]
    fn test_apply_filter() {
        let list = CollectionAccessList(vec![CollectionAccess {
            collection: "col".to_string(),
            access: CollectionAccessMode::Read,
            payload: Some(PayloadConstraint(HashMap::from([(
                "field".parse().unwrap(),
                ValueVariants::Integer(42),
            )]))),
        }]);

        let mut filter = None;
        list.find_view("col").unwrap().apply_filter(&mut filter);
        assert_eq!(
            filter,
            Some(Filter {
                must: Some(vec![Condition::Field(FieldCondition::new_match(
                    "field".parse().unwrap(),
                    Match::new_value(ValueVariants::Integer(42))
                ))]),
                ..Default::default()
            })
        );

        let cond = |path: &str| Condition::IsNull(path.parse::<JsonPath>().unwrap().into());

        let mut filter = Some(Filter {
            should: Some(vec![cond("a")]),
            min_should: Some(MinShould {
                conditions: vec![cond("b")],
                min_count: 1,
            }),
            must: Some(vec![cond("c")]),
            must_not: Some(vec![cond("d")]),
        });
        list.find_view("col").unwrap().apply_filter(&mut filter);
        assert_eq!(
            filter,
            Some(Filter {
                should: Some(vec![cond("a")]),
                min_should: Some(MinShould {
                    conditions: vec![cond("b")],
                    min_count: 1,
                }),
                must: Some(vec![
                    cond("c"),
                    Condition::Field(FieldCondition::new_match(
                        "field".parse().unwrap(),
                        Match::new_value(ValueVariants::Integer(42))
                    ))
                ]),
                must_not: Some(vec![cond("d")]),
            })
        );
    }
}

#[cfg(test)]
mod tests_ops {
    use std::fmt::Debug;

    use api::rest::{BatchVectorStruct, OrderByInterface, RecommendStrategy, VectorStruct};
    use collection::operations::payload_ops::PayloadOpsDiscriminants;
    use collection::operations::point_ops::{
        Batch, PointInsertOperationsInternal, PointInsertOperationsInternalDiscriminants,
        PointOperationsDiscriminants, PointStruct, PointSyncOperation,
    };
    use collection::operations::query_enum::QueryEnum;
    use collection::operations::types::{SearchRequestInternal, UsingVector};
    use collection::operations::vector_ops::{
        PointVectors, UpdateVectorsOp, VectorOperationsDiscriminants,
    };
    use collection::operations::{
        CollectionUpdateOperationsDiscriminants, CreateIndex, FieldIndexOperations,
        FieldIndexOperationsDiscriminants,
    };
    use segment::data_types::vectors::NamedVectorStruct;
    use segment::types::{PointIdType, SearchParams, WithPayloadInterface, WithVector};
    use strum::IntoEnumIterator as _;

    use super::*;
    use crate::rbac::{AccessCollectionBuilder, GlobalAccessMode};

    /// Operation is allowed with the given access, and no rewrite is expected.
    fn assert_allowed<Op: Debug + Clone + PartialEq + CheckableCollectionOperation>(
        op: &Op,
        access: &Access,
    ) {
        let mut op_actual = op.clone();
        access
            .check_point_op("col", &mut op_actual)
            .expect("Should be allowed");
        assert_eq!(op, &op_actual, "Expected not to change");
    }

    /// Operation is allowed with the given access, and the rewrite is expected.
    /// A closure `rewrite` is expected to produce the same result as the rewritten operation.
    fn assert_allowed_rewrite<Op: Debug + Clone + PartialEq + CheckableCollectionOperation>(
        op: &Op,
        access: &Access,
        rewrite: impl FnOnce(&mut Op),
    ) {
        let mut op_actual = op.clone();
        access
            .check_point_op("col", &mut op_actual)
            .expect("Should be allowed");
        let mut op_reference = op.clone();
        rewrite(&mut op_reference);
        assert_eq!(op_reference, op_actual, "Expected to change");
    }

    /// Operation is forbidden with the given access.
    fn assert_forbidden<Op: Clone + CheckableCollectionOperation + PartialEq>(
        op: &Op,
        access: &Access,
    ) {
        access
            .check_point_op("col", &mut op.clone())
            .expect_err("Should be allowed");
    }

    /// Operation requires write + whole collection access.
    fn assert_requires_whole_write_access<Op>(op: &Op)
    where
        Op: CheckableCollectionOperation + Clone + Debug + PartialEq,
    {
        assert_allowed(op, &Access::Global(GlobalAccessMode::Manage));
        assert_forbidden(op, &Access::Global(GlobalAccessMode::Read));

        assert_allowed(
            op,
            &AccessCollectionBuilder::new().add("col", true, true).into(),
        );
        assert_forbidden(
            op,
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .into(),
        );
        assert_forbidden(
            op,
            &AccessCollectionBuilder::new()
                .add("col", true, false)
                .into(),
        );
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
            using: Some(UsingVector::Name("vector".to_string())),
            lookup_from: Some(LookupLocation {
                collection: "col2".to_string(),
                vector: Some("vector".to_string()),
                shard_key: None,
            }),
        };

        assert_allowed(&op, &Access::Global(GlobalAccessMode::Manage));
        assert_allowed(&op, &Access::Global(GlobalAccessMode::Read));

        // Require whole access to col2
        assert_forbidden(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false, false)
                .add("col2", false, false)
                .into(),
        );

        assert_allowed_rewrite(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false, false)
                .add("col2", false, true)
                .into(),
            |op| {
                op.filter = Some(PayloadConstraint::new_test("col").to_filter());
            },
        );

        // Point ID is used
        assert_forbidden(
            &RecommendRequestInternal {
                positive: vec![RecommendExample::PointId(ExtendedPointId::NumId(12345))],
                ..op.clone()
            },
            &AccessCollectionBuilder::new()
                .add("col", false, false)
                .add("col2", false, true)
                .into(),
        );

        // lookup_from requires read access
        assert_forbidden(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .into(),
        );
        assert_allowed(
            &RecommendRequestInternal {
                lookup_from: None,
                ..op.clone()
            },
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .into(),
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

        assert_forbidden(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false, false)
                .into(),
        );

        assert_allowed(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .into(),
        );
    }

    #[test]
    fn test_core_search_request() {
        let op = CoreSearchRequest {
            query: QueryEnum::Nearest(NamedVectorStruct::Default(vec![0.0, 1.0, 2.0])),
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
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .into(),
        );

        assert_allowed_rewrite(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false, false)
                .into(),
            |op| {
                op.filter = Some(PayloadConstraint::new_test("col").to_filter());
            },
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
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .into(),
        );

        assert_allowed_rewrite(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false, false)
                .into(),
            |op| {
                op.filter = Some(PayloadConstraint::new_test("col").to_filter());
            },
        );
    }

    #[test]
    fn test_group_request_source() {
        let op = GroupRequest {
            // NOTE: SourceRequest::Recommend is already tested in test_recommend_request_internal
            source: SourceRequest::Search(SearchRequestInternal {
                vector: NamedVectorStruct::Default(vec![0.0, 1.0, 2.0]).into(),
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
                .add("col", false, true)
                .add("col2", false, true)
                .into(),
        );

        // with_lookup requires whole read access
        assert_forbidden(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .into(),
        );
        assert_forbidden(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .add("col", false, false)
                .into(),
        );
        assert_allowed(
            &GroupRequest {
                with_lookup: None,
                ..op.clone()
            },
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .into(),
        );

        // filter rewrite
        assert_allowed_rewrite(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false, false)
                .add("col2", false, true)
                .into(),
            |op| match &mut op.source {
                SourceRequest::Search(s) => {
                    s.filter = Some(PayloadConstraint::new_test("col").to_filter());
                }
                SourceRequest::Recommend(_) => unreachable!(),
            },
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
            using: Some(UsingVector::Name("vector".to_string())),
            lookup_from: Some(LookupLocation {
                collection: "col2".to_string(),
                vector: Some("vector".to_string()),
                shard_key: None,
            }),
        };

        assert_allowed(&op, &Access::Global(GlobalAccessMode::Manage));
        assert_allowed(&op, &Access::Global(GlobalAccessMode::Read));

        assert_allowed(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .add("col2", false, true)
                .into(),
        );

        assert_allowed_rewrite(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false, false)
                .add("col2", false, true)
                .into(),
            |op| {
                op.filter = Some(PayloadConstraint::new_test("col").to_filter());
            },
        );

        // Point ID is used
        assert_forbidden(
            &DiscoverRequestInternal {
                target: Some(RecommendExample::PointId(ExtendedPointId::NumId(12345))),
                ..op.clone()
            },
            &AccessCollectionBuilder::new()
                .add("col", false, false)
                .add("col2", false, true)
                .into(),
        );
        assert_forbidden(
            &DiscoverRequestInternal {
                context: Some(vec![ContextExamplePair {
                    positive: RecommendExample::PointId(ExtendedPointId::NumId(12345)),
                    negative: RecommendExample::Dense(vec![0.0, 1.0, 2.0]),
                }]),
                ..op.clone()
            },
            &AccessCollectionBuilder::new()
                .add("col", false, false)
                .add("col2", false, true)
                .into(),
        );
        assert_forbidden(
            &DiscoverRequestInternal {
                context: Some(vec![ContextExamplePair {
                    positive: RecommendExample::Dense(vec![0.0, 1.0, 2.0]),
                    negative: RecommendExample::PointId(ExtendedPointId::NumId(12345)),
                }]),
                ..op.clone()
            },
            &AccessCollectionBuilder::new()
                .add("col", false, false)
                .add("col2", false, true)
                .into(),
        );

        // lookup_from requires read access
        assert_forbidden(
            &op,
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .into(),
        );
        assert_allowed(
            &DiscoverRequestInternal {
                lookup_from: None,
                ..op.clone()
            },
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .into(),
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
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .into(),
        );

        assert_allowed_rewrite(
            &ScrollRequestInternal { ..op.clone() },
            &AccessCollectionBuilder::new()
                .add("col", false, false)
                .into(),
            |op| {
                op.filter = Some(PayloadConstraint::new_test("col").to_filter());
            },
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
                            PointInsertOperationsInternal::PointsBatch(Batch {
                                ids: vec![ExtendedPointId::NumId(12345)],
                                vectors: BatchVectorStruct::Single(vec![vec![0.0, 1.0, 2.0]]),
                                payloads: None,
                            })
                        }
                        PointInsertOperationsInternalDiscriminants::PointsList => {
                            PointInsertOperationsInternal::PointsList(vec![PointStruct {
                                id: ExtendedPointId::NumId(12345),
                                vector: VectorStruct::Single(vec![0.0, 1.0, 2.0]),
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

        assert_allowed(
            op,
            &AccessCollectionBuilder::new().add("col", true, true).into(),
        );
        assert_forbidden(
            op,
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .into(),
        );

        assert_allowed_rewrite(
            op,
            &AccessCollectionBuilder::new()
                .add("col", true, false)
                .into(),
            |op| {
                *op = CollectionUpdateOperations::PointOperation(
                    PointOperations::DeletePointsByFilter(
                        make_filter_from_ids(vec![ExtendedPointId::NumId(12345)])
                            .merge_owned(PayloadConstraint::new_test("col").to_filter()),
                    ),
                );
            },
        );
    }

    /// Tests for [`CollectionUpdateOperations::VectorOperation`].
    fn check_collection_update_operations_update_vectors() {
        VectorOperationsDiscriminants::iter().for_each(|discr| match discr {
            VectorOperationsDiscriminants::UpdateVectors => {
                let op = CollectionUpdateOperations::VectorOperation(
                    VectorOperations::UpdateVectors(UpdateVectorsOp {
                        points: vec![PointVectors {
                            id: ExtendedPointId::NumId(12345),
                            vector: VectorStruct::Single(vec![0.0, 1.0, 2.0]),
                        }],
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
                        vec!["vector".to_string()],
                    ));
                check_collection_update_operations_delete_vectors(&op);
            }
            VectorOperationsDiscriminants::DeleteVectorsByFilter => {
                let op = CollectionUpdateOperations::VectorOperation(
                    VectorOperations::DeleteVectorsByFilter(
                        make_filter_from_ids(vec![ExtendedPointId::NumId(12345)]),
                        vec!["vector".to_string()],
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

        assert_allowed(
            op,
            &AccessCollectionBuilder::new().add("col", true, true).into(),
        );
        assert_forbidden(
            op,
            &AccessCollectionBuilder::new()
                .add("col", false, true)
                .into(),
        );

        assert_allowed_rewrite(
            op,
            &AccessCollectionBuilder::new()
                .add("col", true, false)
                .into(),
            |op| {
                *op = CollectionUpdateOperations::VectorOperation(
                    VectorOperations::DeleteVectorsByFilter(
                        make_filter_from_ids(vec![ExtendedPointId::NumId(12345)])
                            .merge_owned(PayloadConstraint::new_test("col").to_filter()),
                        vec!["vector".to_string()],
                    ),
                );
            },
        );
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
            assert_forbidden(
                &op,
                &AccessCollectionBuilder::new().add("col", true, true).into(),
            );
            assert_forbidden(
                &op,
                &AccessCollectionBuilder::new()
                    .add("col", false, true)
                    .into(),
            );
        }
    }
}
