//! Point upserts: plain, conditional and raw.

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::DeferredBehavior;
use parking_lot::RwLockWriteGuard;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::named_vectors::NamedVectors;
use segment::entry::entry_point::SegmentEntry;
use segment::types::{Filter, Payload, PointIdType, SeqNumberType};

use crate::operations::point_ops::{
    ConditionalInsertOperationInternal, PointInsertOperationsInternal, PointStructPersisted,
    PointStructRawPersisted, UpdateMode,
};
use crate::segment_holder::SegmentHolder;
use crate::update::helpers::select_excluded_by_filter_ids;

/// Do not insert more than this number of points in a single update operation chunk
/// This is needed to avoid locking segments for too long, so that
/// parallel read operations are not starved.
const UPDATE_OP_CHUNK_SIZE: usize = 32;

/// Checks point id in each segment, update point if found.
/// All not found points are inserted into random segment.
/// Returns: number of updated points.
pub fn upsert_points<'a, T>(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: T,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize>
where
    T: IntoIterator<Item = &'a PointStructPersisted>,
{
    let points_map: AHashMap<PointIdType, _> = points.into_iter().map(|p| (p.id, p)).collect();
    let ids: Vec<PointIdType> = points_map.keys().copied().collect();

    let mut res = 0;

    for ids_chunk in ids.chunks(UPDATE_OP_CHUNK_SIZE) {
        // Update points in writable segments
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            ids_chunk,
            |id, write_segment| {
                let point = points_map[&id];
                upsert_with_payload(
                    write_segment,
                    op_num,
                    id,
                    point.get_vectors(),
                    point.payload.as_ref(),
                    hw_counter,
                )
            },
            |id, raw_vectors, updated_vectors, old_payload| {
                let point = points_map[&id];
                // Upsert replaces the whole point: old named vectors and
                // payload must not survive the move, matching the in-place
                // `upsert_with_payload` path (`replace_all_vectors` +
                // clear/set payload).
                raw_vectors.clear();
                *updated_vectors = point.get_vectors();
                *old_payload = point.payload.clone().unwrap_or_default();
            },
            hw_counter,
        )?;

        res += updated_points.len();
        // Insert new points, which was not updated or existed
        let new_point_ids = ids_chunk
            .iter()
            .copied()
            .filter(|x| !updated_points.contains(x));

        {
            let default_write_segment =
                segments.smallest_appendable_segment().ok_or_else(|| {
                    OperationError::service_error(
                        "No appendable segments exist, expected at least one",
                    )
                })?;

            let segment_arc = default_write_segment.get();
            let mut write_segment = segment_arc.write();
            for point_id in new_point_ids {
                let point = points_map[&point_id];
                res += usize::from(upsert_with_payload(
                    &mut write_segment,
                    op_num,
                    point_id,
                    point.get_vectors(),
                    point.payload.as_ref(),
                    hw_counter,
                )?);
            }
            RwLockWriteGuard::unlock_fair(write_segment);
        };
    }

    Ok(res)
}

pub fn upsert_points_raw<'a, T>(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: T,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize>
where
    T: IntoIterator<Item = &'a PointStructRawPersisted>,
{
    let points_map: AHashMap<PointIdType, _> = points.into_iter().map(|p| (p.id, p)).collect();
    let ids: Vec<PointIdType> = points_map.keys().copied().collect();

    let mut res = 0;

    for ids_chunk in ids.chunks(UPDATE_OP_CHUNK_SIZE) {
        // Replace points which are already present in some writable segment
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            ids_chunk,
            |id, write_segment| {
                upsert_raw_with_payload(write_segment, op_num, points_map[&id], hw_counter)
            },
            |id, raw_vectors, _updated_vectors, payload| {
                // A raw upsert replaces the whole point: drop the old raw
                // vectors and payload, carry the incoming bytes verbatim.
                let point = points_map[&id];
                raw_vectors.clear();
                raw_vectors.extend(point.vectors.iter().cloned());
                *payload = point.payload.clone().unwrap_or_default();
            },
            hw_counter,
        )?;

        res += updated_points.len();
        // Insert new points, which was not updated or existed
        let new_point_ids = ids_chunk
            .iter()
            .copied()
            .filter(|x| !updated_points.contains(x));

        {
            let default_write_segment =
                segments.smallest_appendable_segment().ok_or_else(|| {
                    OperationError::service_error(
                        "No appendable segments exist, expected at least one",
                    )
                })?;

            let segment_arc = default_write_segment.get();
            let mut write_segment = segment_arc.write();
            for point_id in new_point_ids {
                res += usize::from(upsert_raw_with_payload(
                    &mut write_segment,
                    op_num,
                    points_map[&point_id],
                    hw_counter,
                )?);
            }
            RwLockWriteGuard::unlock_fair(write_segment);
        };
    }

    Ok(res)
}

/// Drop from `points_op` every point that the conditional-upsert
/// `update_mode` excludes, judged against current segment state.
///
/// This is the state-reading half of a conditional upsert, shared by the
/// apply path ([`conditional_upsert`]) and the submit-time resolution that
/// rewrites the operation to a plain upsert before it reaches the WAL
/// (`resolve::resolve_operation`).
pub(crate) fn retain_conditional_upsert_points(
    segments: &SegmentHolder,
    points_op: &mut PointInsertOperationsInternal,
    condition: Filter,
    update_mode: Option<UpdateMode>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<()> {
    let point_ids = points_op.point_ids();
    let update_mode = update_mode.unwrap_or_default();

    match update_mode {
        UpdateMode::Upsert => {
            // Default behavior: insert new points, update existing points that match the condition
            let points_to_exclude =
                select_excluded_by_filter_ids(segments, point_ids, condition, hw_counter)?;
            points_op.retain_point_ids(|idx| !points_to_exclude.contains(idx));
        }
        UpdateMode::InsertOnly => {
            // Only insert new points, skip all existing points entirely
            let existing_points = segments.select_existing_points(point_ids);
            points_op.retain_point_ids(|idx| !existing_points.contains(idx));
        }
        UpdateMode::UpdateOnly => {
            // Only update existing points that match the condition, don't insert new points
            let points_to_exclude =
                select_excluded_by_filter_ids(segments, point_ids.clone(), condition, hw_counter)?;
            let existing_points = segments.select_existing_points(point_ids);
            // Keep only points that exist AND are not excluded by the condition
            points_op.retain_point_ids(|idx| {
                existing_points.contains(idx) && !points_to_exclude.contains(idx)
            });
        }
    }

    Ok(())
}

pub fn conditional_upsert(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    operation: ConditionalInsertOperationInternal,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let ConditionalInsertOperationInternal {
        mut points_op,
        condition,
        update_mode,
    } = operation;

    retain_conditional_upsert_points(segments, &mut points_op, condition, update_mode, hw_counter)?;

    let points = points_op.into_point_vec();
    let upserted_points = upsert_points(segments, op_num, points.iter(), hw_counter)?;

    if upserted_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(upserted_points)
}

/// Upsert to a point ID with the specified vectors and payload in the given segment.
///
/// If the payload is None, the existing payload will be cleared.
///
/// Returns
/// - Ok(true) if the operation was successful and point replaced existing value
/// - Ok(false) if the operation was successful and point was inserted
/// - Err if the operation failed
fn upsert_with_payload(
    segment: &mut RwLockWriteGuard<dyn SegmentEntry>,
    op_num: SeqNumberType,
    point_id: PointIdType,
    vectors: NamedVectors,
    payload: Option<&Payload>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<bool> {
    let mut res = segment.upsert_point(op_num, point_id, vectors, hw_counter)?;
    if let Some(full_payload) = payload {
        res &= segment.set_full_payload(op_num, point_id, full_payload, hw_counter)?;
    } else {
        res &= segment.clear_payload(op_num, point_id, hw_counter)?;
    }
    debug_assert!(
        segment.has_point(point_id, DeferredBehavior::WithDeferred),
        "the point {point_id} should be present immediately after the upsert"
    );
    Ok(res)
}

fn upsert_raw_with_payload(
    segment: &mut RwLockWriteGuard<dyn SegmentEntry>,
    op_num: SeqNumberType,
    point: &PointStructRawPersisted,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<bool> {
    let mut res = segment.upsert_point_raw(op_num, point.id, &point.vectors, hw_counter)?;
    if let Some(full_payload) = &point.payload {
        res &= segment.set_full_payload(op_num, point.id, full_payload, hw_counter)?;
    } else {
        res &= segment.clear_payload(op_num, point.id, hw_counter)?;
    }
    debug_assert!(
        segment.has_point(point.id, DeferredBehavior::WithDeferred),
        "the point {} should be present immediately after the upsert",
        point.id,
    );
    Ok(res)
}
