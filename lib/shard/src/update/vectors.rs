//! Named-vector operations: update and delete vectors of existing points.

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use segment::common::operation_error::OperationResult;
use segment::data_types::named_vectors::NamedVectors;
use segment::types::{Filter, PointIdType, SeqNumberType, VectorNameBuf};

use super::helpers::{check_unprocessed_points, points_by_filter, select_excluded_by_filter_ids};
use crate::operations::vector_ops::{PointVectorsPersisted, UpdateVectorsOp};
use crate::segment_holder::SegmentHolder;

/// Batch size when modifying vector
const VECTOR_OP_BATCH_SIZE: usize = 32;

pub fn update_vectors_conditional(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: UpdateVectorsOp,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let UpdateVectorsOp {
        mut points,
        update_filter,
    } = points;

    let Some(filter_condition) = update_filter else {
        return update_vectors(segments, op_num, points, hw_counter);
    };

    let point_ids: Vec<_> = points.iter().map(|point| point.id).collect();

    let points_to_exclude =
        select_excluded_by_filter_ids(segments, point_ids, filter_condition, hw_counter)?;

    points.retain(|p| !points_to_exclude.contains(&p.id));
    update_vectors(segments, op_num, points, hw_counter)
}

/// Update the specified named vectors of a point, keeping unspecified vectors intact.
fn update_vectors(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: Vec<PointVectorsPersisted>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    // Build a map of vectors to update per point, merge updates on same point ID
    let mut points_map: AHashMap<PointIdType, NamedVectors> = AHashMap::new();
    for point in points {
        let PointVectorsPersisted { id, vector } = point;
        let named_vector = NamedVectors::from(vector);

        let entry = points_map.entry(id).or_default();
        entry.merge(named_vector);
    }

    let ids: Vec<PointIdType> = points_map.keys().copied().collect();

    let mut total_updated_points = 0;
    for batch in ids.chunks(VECTOR_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            batch,
            |id, write_segment| {
                let vectors = points_map[&id].clone();
                write_segment.update_vectors(op_num, id, vectors, hw_counter)
            },
            |id, _raw_vectors, updated_vectors, _| {
                for (vector_name, vector_ref) in points_map[&id].iter() {
                    updated_vectors.insert_ref(vector_name, vector_ref);
                }
            },
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

/// Delete the given named vectors for the given points, keeping other vectors intact.
pub fn delete_vectors(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: &[PointIdType],
    vector_names: &[VectorNameBuf],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_deleted_points = 0;

    for batch in points.chunks(VECTOR_OP_BATCH_SIZE) {
        let modified_points = segments.apply_points_with_conditional_move(
            op_num,
            batch,
            |id, write_segment| {
                let mut res = true;
                for name in vector_names {
                    res &= write_segment.delete_vector(op_num, id, name)?;
                }
                Ok(res)
            },
            |_, raw_vectors, _, _| {
                raw_vectors.retain(|(name, _)| !vector_names.contains(name));
            },
            hw_counter,
        )?;
        check_unprocessed_points(batch, &modified_points)?;
        total_deleted_points += modified_points.len();
    }

    if total_deleted_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(total_deleted_points)
}

/// Delete the given named vectors for points matching the given filter, keeping other vectors intact.
pub fn delete_vectors_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    filter: &Filter,
    vector_names: &[VectorNameBuf],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let affected_points = points_by_filter(segments, filter, hw_counter)?;
    let vectors_deleted =
        delete_vectors(segments, op_num, &affected_points, vector_names, hw_counter)?;

    if vectors_deleted == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(vectors_deleted)
}
