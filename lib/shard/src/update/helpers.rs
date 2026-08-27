//! Shared helpers for update operations: filter-based point selection
//! (including the deferred-points corner case) and result validation.

use std::sync::atomic::AtomicBool;

use ahash::{AHashMap, AHashSet};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::DeferredBehavior;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::types::{Condition, Filter, PointIdType, SeqNumberType};

use crate::segment_holder::{SegmentHolder, SegmentId};

/// Deferred points corner case for filtered operations.
///
/// When a point has multiple copies across segments, the old non-deferred copy may match
/// a filter while the newest deferred copy does not. In this case, the operation should NOT
/// be applied to the point — the old copy will be cleaned up during optimization deduplication.
///
/// Given per-segment filter match results, this function returns the set of point IDs that
/// should be excluded from the operation because a newer deferred version exists that wasn't
/// matched by the filter.
pub(super) fn deferred_points_to_exclude_by_filter(
    segments: &SegmentHolder,
    per_segment_points: &AHashMap<SegmentId, Vec<PointIdType>>,
) -> AHashSet<PointIdType> {
    // Find the maximum version for each point across segments where the filter matched.
    let mut max_versions: AHashMap<PointIdType, Option<SeqNumberType>> = Default::default();
    for (segment_id, point_ids) in per_segment_points {
        let segment = segments.get(*segment_id).unwrap().get().read();
        for point_id in point_ids {
            let version = segment.point_version(*point_id);
            let entry = max_versions.entry(*point_id).or_insert(None);
            *entry = std::cmp::max(*entry, version);
        }
    }

    // Check if any deferred point has a newer version than the max matched version.
    // Such a point was not matched by the filter (its deferred version has different data),
    // so the operation should not be applied.
    let mut to_exclude = AHashSet::new();
    for (_segment_id, segment) in segments.iter() {
        let segment = segment.get().read();
        if !segment.has_deferred_points() {
            continue;
        }
        for (point_id, max_version) in &max_versions {
            if segment.has_point(*point_id, DeferredBehavior::WithDeferred)
                && segment.point_version(*point_id) > *max_version
                && segment.point_is_deferred(*point_id)
            {
                to_exclude.insert(*point_id);
            }
        }
    }

    to_exclude
}

pub(crate) fn select_excluded_by_filter_ids(
    segments: &SegmentHolder,
    point_ids: impl IntoIterator<Item = PointIdType>,
    filter: Filter,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<AHashSet<PointIdType>> {
    // Filter for points that doesn't match the condition, and have matching
    let non_match_filter =
        Filter::new_must_not(Condition::Filter(filter)).with_point_ids(point_ids);

    Ok(points_by_filter(segments, &non_match_filter, hw_counter)?
        .into_iter()
        .collect())
}

pub(crate) fn points_by_filter(
    segments: &SegmentHolder,
    filter: &Filter,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Vec<PointIdType>> {
    // we don’t want to cancel this filtered read
    let is_stopped = AtomicBool::new(false);
    let mut has_deferred = false;
    let per_segment_points: AHashMap<SegmentId, Vec<PointIdType>> = segments
        .iter()
        .map(|(segment_id, segment)| {
            let segment = segment.get().read();
            let point_ids = segment.read_filtered(
                None,
                None,
                Some(filter),
                &is_stopped,
                hw_counter,
                // Read operation used for updates, so we must handle all points
                DeferredBehavior::WithDeferred,
            )?;
            has_deferred |= segment.has_deferred_points();
            Ok((segment_id, point_ids))
        })
        .collect::<OperationResult<_>>()?;

    let mut affected_points: Vec<PointIdType> = per_segment_points
        .values()
        .flat_map(|v| v.iter().copied())
        .collect();

    // Deferred points corner case: exclude points where the newest version is deferred
    // and wasn’t matched by the filter (only an old stale copy matched).
    if has_deferred {
        let to_exclude = deferred_points_to_exclude_by_filter(segments, &per_segment_points);
        if !to_exclude.is_empty() {
            affected_points.retain(|id| !to_exclude.contains(id));
        }
    }

    Ok(affected_points)
}

pub(super) fn check_unprocessed_points(
    points: &[PointIdType],
    processed: &AHashSet<PointIdType>,
) -> OperationResult<usize> {
    let first_missed_point = points.iter().copied().find(|p| !processed.contains(p));

    match first_missed_point {
        None => Ok(processed.len()),
        Some(missed_point_id) => Err(OperationError::PointIdError { missed_point_id }),
    }
}
