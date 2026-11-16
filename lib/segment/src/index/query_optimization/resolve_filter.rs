//! Resolving a filter to the offsets of the points it matches in one segment, so that several
//! searches check a point with a bit test or a binary search instead of the filter itself.
//!
//! Two places resolve a filter:
//! - a segment, for the searches of several groups of a batch (see [`share_filter`]);
//! - a vector index, for the query vectors of one search call (see
//!   [`resolve_for_query_vectors`]), because a graph or an inverted index search checks its
//!   filter once per query vector.
//!
//! [`share_filter`]: crate::entry::entry_point::ReadSegmentEntry::share_filter

use std::sync::atomic::AtomicBool;

use common::types::DeferredBehavior;
use uuid::Uuid;

use super::filter_checks::{FilterChecks, worth_sharing};
use super::point_set::{PointSet, PointSetContext};
use super::resolved_points::{ResolvedPoints, contains_resolved_points};
use crate::common::check_stopped;
use crate::common::operation_error::OperationResult;
use crate::index::PayloadIndexRead;
use crate::index::field_index::CardinalityEstimation;
use crate::types::{Condition, Filter};

/// `filter` resolved to the visible points of the segment of `payload_index`.
///
/// `total_point_count` is the segment's, it decides between a bitset and a list. Fails if
/// stopped: a stopped iteration ends early, and a partial set must never be handed out.
pub fn resolve_filter(
    payload_index: &impl PayloadIndexRead,
    total_point_count: usize,
    filter: &Filter,
    cardinality: &CardinalityEstimation,
    is_stopped: &AtomicBool,
    segment_uuid: Uuid,
) -> OperationResult<Filter> {
    // The visibility vector searches use
    let points = payload_index.iter_filtered_points(
        filter,
        cardinality,
        is_stopped,
        DeferredBehavior::VisibleOnly,
    )?;
    // Whether the set is a bitset or a list depends only on its density in the segment
    let context = PointSetContext::by_density(total_point_count);
    let points = PointSet::from_points(points, cardinality.exp, context);
    check_stopped(is_stopped)?;

    Ok(Filter::new_must(Condition::ResolvedPoints(
        ResolvedPoints::new(segment_uuid, points, cardinality),
    )))
}

/// `filter` resolved for the query vectors of one search call, if that is cheaper than the
/// search evaluating it as `checks` says.
///
/// A filter that already holds resolved points is never resolved again: its checks are already
/// cheap, which the cost model, counting every check the same, doesn't see.
///
/// The resolved filter must not leave the search call: it doesn't carry the segment's uuid,
/// which [`check_resolved_points`] has already checked against the original filter.
///
/// [`check_resolved_points`]: super::resolved_points::check_resolved_points
pub fn resolve_for_query_vectors(
    payload_index: &impl PayloadIndexRead,
    total_point_count: usize,
    filter: &Filter,
    cardinality: &CardinalityEstimation,
    checks: FilterChecks,
    is_stopped: &AtomicBool,
) -> OperationResult<Option<Filter>> {
    if contains_resolved_points(filter) {
        return Ok(None);
    }
    let materialization_cost = payload_index.materialization_cost(cardinality)?;
    if !worth_sharing(materialization_cost, [checks]) {
        return Ok(None);
    }
    resolve_filter(
        payload_index,
        total_point_count,
        filter,
        cardinality,
        is_stopped,
        Uuid::nil(),
    )
    .map(Some)
}
