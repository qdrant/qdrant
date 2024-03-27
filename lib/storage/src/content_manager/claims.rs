use std::collections::HashSet;
use std::mem::take;

use collection::grouping::group_by::{GroupRequest, SourceRequest};
use collection::operations::payload_ops::{DeletePayloadOp, PayloadOps, SetPayloadOp};
use collection::operations::point_ops::{PointIdsList, PointOperations};
use collection::operations::types::{
    ContextExamplePair, CoreSearchRequest, CountRequestInternal, DiscoverRequestInternal,
    PointRequestInternal, RecommendExample, RecommendRequestInternal, ScrollRequestInternal,
};
use collection::operations::vector_ops::VectorOperations;
use collection::operations::{CollectionUpdateOperations, FieldIndexOperations};
use itertools::{Either, Itertools as _};
use rbac::jwt::PayloadClaim;
use segment::types::{Condition, ExtendedPointId, FieldCondition, Filter, Match, Payload};

use super::errors::StorageError;
use crate::rbac::access::Access;

pub fn check_collection_name(
    collections: Option<&Vec<String>>,
    collection_name: &str,
) -> Result<(), StorageError> {
    let ok = collections
        .as_ref()
        .map_or(true, |c| c.iter().any(|c| c == collection_name));
    ok.then_some(()).ok_or_else(|| {
        StorageError::unauthorized(format!("Collection '{collection_name}' is not allowed"))
    })
}

/// Check if a access object is allowed to manage collections.
pub fn check_manage_rights(access: &Access) -> Result<(), StorageError> {
    let Access {
        collections,
        payload,
    } = access;
    if collections.is_some() {
        return incompatible_with_collection_claim();
    }
    if payload.is_some() {
        return incompatible_with_payload_claim();
    }
    Ok(())
}

/// Check if a claim object has full access to a collection.
pub fn check_full_access_to_collection(
    access: &Access,
    collection_name: &str,
) -> Result<(), StorageError> {
    let Access {
        collections,
        payload,
    } = access;
    if let Some(collections) = collections {
        check_collection_name(Some(collections), collection_name)?;
    }
    if payload.is_some() {
        return incompatible_with_payload_claim();
    }
    Ok(())
}

pub fn check_points_op(
    collections: Option<&Vec<String>>,
    payload: Option<&PayloadClaim>,
    op: &mut impl PointsOpClaimsChecker,
) -> Result<(), StorageError> {
    for collection in op.collections_used() {
        check_collection_name(collections, collection)?;
    }
    if let Some(payload) = payload {
        op.apply_payload_claim(payload)?;
    }
    Ok(())
}

pub trait PointsOpClaimsChecker {
    /// An iterator over the collection names used in the operation, for checking `collections`
    /// claim.
    fn collections_used(&self) -> impl Iterator<Item = &str>;

    /// Apply the `payload` claim to the operation.
    fn apply_payload_claim(&mut self, claim: &PayloadClaim) -> Result<(), StorageError>;
}

impl PointsOpClaimsChecker for RecommendRequestInternal {
    fn collections_used(&self) -> impl Iterator<Item = &str> {
        self.lookup_from.iter().map(|l| l.collection.as_str())
    }

    fn apply_payload_claim(&mut self, claim: &PayloadClaim) -> Result<(), StorageError> {
        apply_filter_opt(&mut self.filter, claim)
    }
}

impl PointsOpClaimsChecker for PointRequestInternal {
    fn collections_used(&self) -> impl Iterator<Item = &str> {
        std::iter::empty()
    }

    fn apply_payload_claim(&mut self, _claim: &PayloadClaim) -> Result<(), StorageError> {
        incompatible_with_payload_claim()
    }
}

impl PointsOpClaimsChecker for CoreSearchRequest {
    fn collections_used(&self) -> impl Iterator<Item = &str> {
        std::iter::empty()
    }

    fn apply_payload_claim(&mut self, claim: &PayloadClaim) -> Result<(), StorageError> {
        apply_filter_opt(&mut self.filter, claim)
    }
}

impl PointsOpClaimsChecker for CountRequestInternal {
    fn collections_used(&self) -> impl Iterator<Item = &str> {
        std::iter::empty()
    }

    fn apply_payload_claim(&mut self, claim: &PayloadClaim) -> Result<(), StorageError> {
        apply_filter_opt(&mut self.filter, claim)
    }
}

impl PointsOpClaimsChecker for GroupRequest {
    fn collections_used(&self) -> impl Iterator<Item = &str> {
        let source = match &self.source {
            SourceRequest::Search(_) => Either::Left(std::iter::empty()),
            SourceRequest::Recommend(r) => Either::Right(r.collections_used()),
        };
        let with_lookup = self.with_lookup.iter().map(|l| l.collection_name.as_str());
        source.merge(with_lookup)
    }

    fn apply_payload_claim(&mut self, claim: &PayloadClaim) -> Result<(), StorageError> {
        match &mut self.source {
            SourceRequest::Search(s) => apply_filter_opt(&mut s.filter, claim),
            SourceRequest::Recommend(r) => r.apply_payload_claim(claim),
        }
    }
}

impl PointsOpClaimsChecker for DiscoverRequestInternal {
    fn collections_used(&self) -> impl Iterator<Item = &str> {
        self.lookup_from.iter().map(|l| l.collection.as_str())
    }

    fn apply_payload_claim(&mut self, claim: &PayloadClaim) -> Result<(), StorageError> {
        if let Some(target) = &self.target {
            validate_payload_claim_for_recommended_example(target)?;
        }
        for ContextExamplePair { positive, negative } in self.context.iter().flat_map(|c| c.iter())
        {
            validate_payload_claim_for_recommended_example(positive)?;
            validate_payload_claim_for_recommended_example(negative)?;
        }
        apply_filter_opt(&mut self.filter, claim)
    }
}

impl PointsOpClaimsChecker for ScrollRequestInternal {
    fn collections_used(&self) -> impl Iterator<Item = &str> {
        std::iter::empty()
    }

    fn apply_payload_claim(&mut self, claim: &PayloadClaim) -> Result<(), StorageError> {
        apply_filter_opt(&mut self.filter, claim)
    }
}

impl PointsOpClaimsChecker for CollectionUpdateOperations {
    fn collections_used(&self) -> impl Iterator<Item = &str> {
        std::iter::empty()
    }

    fn apply_payload_claim(&mut self, claim: &PayloadClaim) -> Result<(), StorageError> {
        match self {
            CollectionUpdateOperations::PointOperation(op) => match op {
                PointOperations::UpsertPoints(_) => incompatible_with_payload_claim(),
                PointOperations::DeletePoints { ids } => {
                    *op = PointOperations::DeletePointsByFilter(
                        make_filter_from_ids(take(ids))
                            .merge_owned(make_filter_from_payload_claim(claim)),
                    );
                    Ok(())
                }
                PointOperations::DeletePointsByFilter(filter) => apply_filter(filter, claim),
                PointOperations::SyncPoints(_) => incompatible_with_payload_claim(),
            },

            CollectionUpdateOperations::VectorOperation(op) => match op {
                VectorOperations::UpdateVectors(_) => incompatible_with_payload_claim(),
                VectorOperations::DeleteVectors(PointIdsList { points, shard_key }, vectors) => {
                    if shard_key.is_some() {
                        // It is unclear where to put the shard_key
                        return incompatible_with_payload_claim();
                    }
                    *op = VectorOperations::DeleteVectorsByFilter(
                        make_filter_from_ids(take(points))
                            .merge_owned(make_filter_from_payload_claim(claim)),
                        take(vectors),
                    );
                    Ok(())
                }
                VectorOperations::DeleteVectorsByFilter(filter, _) => apply_filter(filter, claim),
            },

            CollectionUpdateOperations::PayloadOperation(op) => match op {
                PayloadOps::SetPayload(SetPayloadOp {
                    payload: _, // TODO: validate
                    points,
                    filter,
                    key: _, // TODO: validate
                }) => {
                    incompatible_with_payload_claim()?; // Reject as not implemented

                    let filter = filter.get_or_insert_with(Default::default);
                    if let Some(points) = take(points) {
                        *filter = take(filter).merge_owned(make_filter_from_ids(points));
                    }
                    Ok(())
                }
                PayloadOps::DeletePayload(DeletePayloadOp {
                    keys: _, // TODO: validate
                    points,
                    filter,
                }) => {
                    incompatible_with_payload_claim()?; // Reject as not implemented

                    let filter = filter.get_or_insert_with(Default::default);
                    if let Some(points) = take(points) {
                        *filter = take(filter).merge_owned(make_filter_from_ids(points));
                    }
                    Ok(())
                }
                PayloadOps::ClearPayload { points } => {
                    *op = PayloadOps::OverwritePayload(SetPayloadOp {
                        payload: make_payload_from_payload_claim(claim)?,
                        points: None,
                        filter: Some(
                            make_filter_from_ids(take(points))
                                .merge_owned(make_filter_from_payload_claim(claim)),
                        ),
                        key: None,
                    });
                    Ok(())
                }
                PayloadOps::ClearPayloadByFilter(filter) => {
                    *op = PayloadOps::OverwritePayload(SetPayloadOp {
                        payload: make_payload_from_payload_claim(claim)?,
                        points: None,
                        filter: Some(
                            take(filter).merge_owned(make_filter_from_payload_claim(claim)),
                        ),
                        key: None,
                    });
                    Ok(())
                }
                PayloadOps::OverwritePayload(SetPayloadOp {
                    payload: _, // TODO: validate
                    points,
                    filter,
                    key: _, // TODO: validate
                }) => {
                    incompatible_with_payload_claim()?; // Reject as not implemented

                    let filter = filter.get_or_insert_with(Default::default);
                    if let Some(points) = take(points) {
                        *filter = take(filter).merge_owned(make_filter_from_ids(points));
                    }
                    Ok(())
                }
            },

            // These are already checked in CollectionMetaOperations, but we'll check them anyway
            // to be sure.
            CollectionUpdateOperations::FieldIndexOperation(op) => match op {
                FieldIndexOperations::CreateIndex(_) => incompatible_with_payload_claim(),
                FieldIndexOperations::DeleteIndex(_) => incompatible_with_payload_claim(),
            },
        }
    }
}

/// Helper function to indicate that the operation is not allowed when `payload` claim is present.
/// Usually used when point IDs are involved.
pub fn incompatible_with_payload_claim<T>() -> Result<T, StorageError> {
    Err(StorageError::unauthorized(
        "This operation is not allowed when payload JWT claim is present",
    ))
}

pub fn incompatible_with_collection_claim<T>() -> Result<T, StorageError> {
    Err(StorageError::unauthorized(
        "This operation is not allowed when collection JWT claim is present",
    ))
}

fn validate_payload_claim_for_recommended_example(
    example: &RecommendExample,
) -> Result<(), StorageError> {
    match example {
        RecommendExample::PointId(_) => incompatible_with_payload_claim(),
        RecommendExample::Dense(_) | RecommendExample::Sparse(_) => Ok(()),
    }
}

fn apply_filter_opt(filter: &mut Option<Filter>, claim: &PayloadClaim) -> Result<(), StorageError> {
    apply_filter(filter.get_or_insert_with(Default::default), claim)
}

fn apply_filter(filter: &mut Filter, claim: &PayloadClaim) -> Result<(), StorageError> {
    *filter = take(filter).merge_owned(make_filter_from_payload_claim(claim));
    Ok(())
}

/// Create a `must` filter from a list of point IDs.
fn make_filter_from_ids(ids: Vec<ExtendedPointId>) -> Filter {
    let cond = ids.into_iter().collect::<HashSet<_>>().into();
    Filter {
        must: Some(vec![Condition::HasId(cond)]),
        ..Default::default()
    }
}

/// Create a `must` filter from a `payload` claim.
fn make_filter_from_payload_claim(claim: &PayloadClaim) -> Filter {
    Filter {
        must: Some(
            claim
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

fn make_payload_from_payload_claim(_claim: &PayloadClaim) -> Result<Payload, StorageError> {
    // TODO: We need to construct a payload, then validate it against the claim
    incompatible_with_payload_claim() // Reject as not implemented
}
