//! Payload operations: set, delete, clear and overwrite payloads.

use common::counter::hardware_counter::HardwareCounterCell;
use segment::common::operation_error::OperationResult;
use segment::json_path::JsonPath;
use segment::types::{Filter, Payload, PayloadKeyType, PointIdType, SeqNumberType};

use super::helpers::{check_unprocessed_points, points_by_filter};
use crate::segment_holder::SegmentHolder;

/// Batch size when modifying payload.
///
/// Also the overshoot bound of the appendable segment size cap: capacity is checked once per
/// batch, so a CoW-move destination can grow past the cap by at most this many points.
pub const PAYLOAD_OP_BATCH_SIZE: usize = 32;

pub fn set_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload: &Payload,
    points: &[PointIdType],
    key: &Option<JsonPath>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_updated_points = 0;

    for chunk in points.chunks(PAYLOAD_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            chunk,
            |id, write_segment| write_segment.set_payload(op_num, id, payload, key, hw_counter),
            |_, _, _, old_payload| match key {
                Some(key) => old_payload.merge_by_key(payload, key),
                None => old_payload.merge(payload),
            },
            hw_counter,
        )?;

        check_unprocessed_points(chunk, &updated_points)?;
        total_updated_points += updated_points.len();
    }

    if total_updated_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(total_updated_points)
}

pub fn set_payload_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload: &Payload,
    filter: &Filter,
    key: &Option<JsonPath>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let affected_points = points_by_filter(segments, filter, hw_counter)?;
    let points_updated = set_payload(segments, op_num, payload, &affected_points, key, hw_counter)?;

    if points_updated == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(points_updated)
}

pub fn delete_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: &[PointIdType],
    keys: &[PayloadKeyType],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_deleted_points = 0;

    for batch in points.chunks(PAYLOAD_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            batch,
            |id, write_segment| {
                let mut res = true;
                for key in keys {
                    res &= write_segment.delete_payload(op_num, id, key, hw_counter)?;
                }
                Ok(res)
            },
            |_, _, _, payload| {
                for key in keys {
                    payload.remove(key);
                }
            },
            hw_counter,
        )?;

        check_unprocessed_points(batch, &updated_points)?;
        total_deleted_points += updated_points.len();
    }

    if total_deleted_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(total_deleted_points)
}

pub fn delete_payload_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    filter: &Filter,
    keys: &[PayloadKeyType],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let affected_points = points_by_filter(segments, filter, hw_counter)?;
    let points_updated = delete_payload(segments, op_num, &affected_points, keys, hw_counter)?;

    if points_updated == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(points_updated)
}

pub fn clear_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: &[PointIdType],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_updated_points = 0;

    for batch in points.chunks(PAYLOAD_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            batch,
            |id, write_segment| write_segment.clear_payload(op_num, id, hw_counter),
            |_, _, _, payload| payload.0.clear(),
            hw_counter,
        )?;
        check_unprocessed_points(batch, &updated_points)?;
        total_updated_points += updated_points.len();
    }

    if total_updated_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(total_updated_points)
}

/// Clear Payloads from all segments matching the given filter
pub fn clear_payload_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    filter: &Filter,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let points_to_clear = points_by_filter(segments, filter, hw_counter)?;
    let points_cleared = clear_payload(segments, op_num, &points_to_clear, hw_counter)?;

    if points_cleared == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(points_cleared)
}

pub fn overwrite_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload: &Payload,
    points: &[PointIdType],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_updated_points = 0;

    for batch in points.chunks(PAYLOAD_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            batch,
            |id, write_segment| write_segment.set_full_payload(op_num, id, payload, hw_counter),
            |_, _, _, old_payload| {
                *old_payload = payload.clone();
            },
            hw_counter,
        )?;

        total_updated_points += updated_points.len();
        check_unprocessed_points(batch, &updated_points)?;
    }

    if total_updated_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(total_updated_points)
}

pub fn overwrite_payload_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload: &Payload,
    filter: &Filter,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let affected_points = points_by_filter(segments, filter, hw_counter)?;
    let points_updated =
        overwrite_payload(segments, op_num, payload, &affected_points, hw_counter)?;

    if points_updated == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(points_updated)
}
