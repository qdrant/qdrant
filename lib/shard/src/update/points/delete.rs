//! Point deletes: by id and by filter.

use std::sync::atomic::AtomicBool;

use ahash::{AHashMap, AHashSet};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::DeferredBehavior;
use segment::common::operation_error::OperationResult;
use segment::types::{Filter, PointIdType, SeqNumberType};

use crate::segment_holder::SegmentHolder;
use crate::update::helpers::deferred_points_to_exclude_by_filter;

/// Max amount of points to delete in a batched deletion iteration
const DELETION_BATCH_SIZE: usize = 512;

/// Tries to delete points from all segments, returns number of actually deleted points.
///
/// Iterates all segments directly (rather than going through `apply_points`) to ensure
/// that ALL copies of a point are deleted, including old non-deferred copies in optimized
/// segments when the latest version is deferred in an appendable segment.
pub fn delete_points(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    ids: &[PointIdType],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_deleted_points = 0;

    for batch in ids.chunks(DELETION_BATCH_SIZE) {
        for (_segment_id, segment) in segments.iter() {
            let segment_arc = segment.get();
            let mut write_segment = segment_arc.write();
            for &id in batch {
                if write_segment.delete_point(op_num, id, hw_counter)? {
                    total_deleted_points += 1;
                }
            }
        }
    }

    if total_deleted_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(total_deleted_points)
}

/// Deletes points from all segments matching the given filter
pub fn delete_points_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    filter: &Filter,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_deleted = 0;
    // we don’t want to cancel this filtered read
    let is_stopped = AtomicBool::new(false);
    let mut has_deferred = false;
    let mut points_to_delete: AHashMap<_, _> = segments
        .iter()
        .map(|(segment_id, segment)| {
            let segment = segment.get().read();
            let point_ids = segment.read_filtered(
                None,
                None,
                Some(filter),
                &is_stopped,
                hw_counter,
                // Include also deferred points.
                DeferredBehavior::WithDeferred,
            )?;
            has_deferred |= segment.has_deferred_points();
            Ok((segment_id, point_ids))
        })
        .collect::<OperationResult<_>>()?;

    // Deferred points corner case.
    // If the latest version of a point is deferred and does not match the filter,
    // we need to skip deletion for all copies and let deduplication during optimization delete old points.
    if has_deferred {
        let points_to_keep = deferred_points_to_exclude_by_filter(segments, &points_to_delete);

        // Expand per-segment lists to include all segments that have each matched point,
        // so that ALL copies get deleted (not just the segment where the filter matched).
        let all_matched_points: AHashSet<PointIdType> = points_to_delete
            .values()
            .flat_map(|v| v.iter().copied())
            .collect();
        for (segment_id, segment) in segments.iter() {
            let segment = segment.get().read();
            let present: Vec<_> = all_matched_points
                .iter()
                .copied()
                .filter(|point_id| {
                    segment.has_point(*point_id, DeferredBehavior::WithDeferred)
                        && !points_to_keep.contains(point_id)
                })
                .collect();
            points_to_delete.insert(segment_id, present);
        }
    }

    segments.apply_segments_batched(|s, segment_id| {
        let Some(curr_points) = points_to_delete.get_mut(&segment_id) else {
            return Ok(false);
        };
        if curr_points.is_empty() {
            return Ok(false);
        }

        let mut deleted_in_batch = 0;
        while let Some(point_id) = curr_points.pop() {
            if s.delete_point(op_num, point_id, hw_counter)? {
                total_deleted += 1;
                deleted_in_batch += 1;
            }

            if deleted_in_batch >= DELETION_BATCH_SIZE {
                break;
            }
        }

        Ok(true)
    })?;

    if total_deleted == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(total_deleted)
}
