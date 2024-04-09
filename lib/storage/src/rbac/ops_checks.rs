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
    fn apply_filter(&self, filter: &mut Filter) {
        if let Some(payload) = &self.payload {
            *filter = take(filter).merge_owned(payload.to_filter());
        }
    }

    fn apply_filter_opt(&self, filter: &mut Option<Filter>) {
        if let Some(filter) = filter {
            self.apply_filter(filter);
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
        view.apply_filter_opt(&mut self.filter);
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
        view.apply_filter_opt(&mut self.filter);
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
        view.apply_filter_opt(&mut self.filter);
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
                view.apply_filter_opt(&mut s.filter);
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
        view.apply_filter_opt(&mut self.filter);
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
        view.apply_filter_opt(&mut self.filter);
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
        // Currently, this whole function is shortcut with an error when there is a payload constraint
        // TODO: Handle payload constraints for all ops and remove this block
        if let Some(_payload) = &view.payload {
            // Reject as not implemented, yet
            return incompatible_with_payload_constraint(view.collection);
        }
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
