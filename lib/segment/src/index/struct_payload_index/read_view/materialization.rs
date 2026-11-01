//! Reading small secondary conditions into point sets on the plain filtering path.
//!
//! When the caller knows how many points it will check against a filter, a `match` or
//! `range` field condition whose posting list is small relative to that is read up front.
//! All such posting lists of one clause are then merged into a single [`PointSet`], so a
//! clause pays for at most one set, and that set is a sorted list unless a bitset is cheaper.

use common::types::PointOffsetType;

use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::CardinalityEstimation;
use crate::index::query_optimization::expected_checks::ExpectedChecks;
use crate::index::query_optimization::point_set::{PointSet, PointSetContext};
use crate::types::{Condition, FieldCondition};

/// What the caller of `optimize_filter` knows about how the filter will be used.
#[derive(Debug, Clone, Copy)]
pub struct MaterializationHint<'h> {
    /// How many points the caller will check against the filter, and how.
    pub expected_checks: ExpectedChecks,
    /// Estimation whose primary clauses produce the checked points, if any. Its primary
    /// conditions are never read up front: that would read their posting lists a second
    /// time just to confirm what the primary iterator already yielded.
    pub primary: Option<&'h CardinalityEstimation>,
}

impl MaterializationHint<'_> {
    pub(super) fn is_primary(&self, condition: &Condition) -> bool {
        self.primary
            .is_some_and(|estimation| estimation.is_primary(condition))
    }

    pub(super) fn point_set_context(&self, total_points: usize) -> PointSetContext {
        PointSetContext {
            total_points,
            expected_checks: self.expected_checks.count,
        }
    }
}

/// A condition converted for one clause: either a per-point checker or a posting list
/// read up front, which the clause merges with its other posting lists.
pub(super) enum ConvertedCondition<'b> {
    Checker(ConditionCheckerEnum<'b>),
    Posting(Vec<PointOffsetType>),
}

/// How a clause combines its posting lists.
#[derive(Debug, Clone, Copy)]
pub(super) enum PostingMerge {
    /// `must`: a point must be in every list.
    Intersection,
    /// `should` / `must_not`: a point must be in at least one list.
    Union,
}

/// Split converted conditions (already in check order) into per-point checkers and one
/// merged point set, placed first: it's a cheap check and, for `must`, the most selective.
pub(super) fn merge_postings<'b>(
    converted: Vec<ConvertedCondition<'b>>,
    merge: PostingMerge,
    context: Option<PointSetContext>,
) -> Vec<ConditionCheckerEnum<'b>> {
    let mut postings = Vec::new();
    let mut checkers = Vec::with_capacity(converted.len() + 1);
    checkers.push(None); // slot for the merged set
    for condition in converted {
        match condition {
            ConvertedCondition::Checker(checker) => checkers.push(Some(checker)),
            ConvertedCondition::Posting(posting) => postings.push(posting),
        }
    }
    if let Some(context) = context
        && !postings.is_empty()
    {
        let set = match merge {
            PostingMerge::Intersection => PointSet::intersection(postings, context),
            PostingMerge::Union => PointSet::union(postings, context),
        };
        checkers[0] = Some(ConditionCheckerEnum::Points(set));
    }
    checkers.into_iter().flatten().collect()
}

/// Whether the index answers `condition` by reading posting lists or sorted values,
/// so that reading it up front costs about one sequential entry per match. Geo conditions
/// re-check every point of the matching geohash cells and `values_count` scans points,
/// so those stay per-point checks.
pub(super) fn reads_posting_list(condition: &FieldCondition) -> bool {
    let FieldCondition {
        key: _,
        r#match,
        range,
        geo_bounding_box,
        geo_radius,
        geo_polygon,
        values_count,
        is_empty,
        is_null,
    } = condition;
    (r#match.is_some() || range.is_some())
        && geo_bounding_box.is_none()
        && geo_radius.is_none()
        && geo_polygon.is_none()
        && values_count.is_none()
        && is_empty.is_none()
        && is_null.is_none()
}
