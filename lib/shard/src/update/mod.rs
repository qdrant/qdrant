//! A collection of functions for updating points and payloads stored in segments

mod field_index;
mod helpers;
mod payload;
mod points;
#[cfg(test)]
mod tests;
mod vectors;

use common::counter::hardware_counter::HardwareCounterCell;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::types::{Payload, SeqNumberType};

pub use self::field_index::{create_field_index, delete_field_index};
pub(crate) use self::helpers::{points_by_filter, select_excluded_by_filter_ids};
pub use self::payload::{
    clear_payload, clear_payload_by_filter, delete_payload, delete_payload_by_filter,
    overwrite_payload, overwrite_payload_by_filter, set_payload, set_payload_by_filter,
};
pub(crate) use self::points::retain_conditional_upsert_points;
pub use self::points::{
    conditional_upsert, delete_points, delete_points_by_filter, sync_points, sync_points_raw,
    upsert_points, upsert_points_raw,
};
pub use self::vectors::{delete_vectors, delete_vectors_by_filter, update_vectors_conditional};
use crate::operations::payload_ops::PayloadOps;
use crate::operations::point_ops::PointOperations;
use crate::operations::vector_ops::VectorOperations;
use crate::operations::{
    CreateVectorName, DeleteVectorName, FieldIndexOperations, VectorNameOperations,
};
use crate::segment_holder::SegmentHolder;

pub fn process_point_operation(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    point_operation: PointOperations,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    match point_operation {
        PointOperations::UpsertPoints(operation) => {
            let points = operation.into_point_vec();
            if points.is_empty() {
                // An empty upsert (e.g. an emptied-out resolved conditional upsert)
                // touches no segment; bump so WAL can acknowledge it.
                segments.bump_max_segment_version_overwrite(op_num);
            }
            let res = upsert_points(segments, op_num, points.iter(), hw_counter)?;
            Ok(res)
        }
        PointOperations::UpsertPointsConditional(operation) => {
            conditional_upsert(segments, op_num, operation, hw_counter)
        }
        PointOperations::DeletePoints { ids } => delete_points(segments, op_num, &ids, hw_counter),
        PointOperations::DeletePointsByFilter(filter) => {
            delete_points_by_filter(segments, op_num, &filter, hw_counter)
        }
        PointOperations::SyncPoints(operation) => {
            let (deleted, new, updated) = sync_points(
                segments,
                op_num,
                operation.from_id,
                operation.to_id,
                &operation.points,
                hw_counter,
            )?;
            Ok(deleted + new + updated)
        }
        PointOperations::UpsertPointsRaw(points) => {
            if points.is_empty() {
                // An empty upsert touches no segment; bump so WAL can acknowledge it.
                segments.bump_max_segment_version_overwrite(op_num);
            }
            let res = upsert_points_raw(segments, op_num, points.iter(), hw_counter)?;
            Ok(res)
        }
        PointOperations::SyncPointsRaw(operation) => {
            let (deleted, new, updated) = sync_points_raw(
                segments,
                op_num,
                operation.from_id,
                operation.to_id,
                &operation.points,
                hw_counter,
            )?;
            Ok(deleted + new + updated)
        }
    }
}

#[cfg(feature = "staging")]
pub fn process_staging_operation(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    operation: crate::operations::staging::StagingOperations,
) -> OperationResult<usize> {
    match operation {
        crate::operations::staging::StagingOperations::Delay(delay_op) => {
            delay_op.execute();
        }
    }

    // This operation doesn't directly affect segment/point versions, so we bump it here
    segments.bump_max_segment_version_overwrite(op_num);

    Ok(0)
}

pub fn process_vector_operation(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    vector_operation: VectorOperations,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    match vector_operation {
        VectorOperations::UpdateVectors(update_vectors) => {
            update_vectors_conditional(segments, op_num, update_vectors, hw_counter)
        }
        VectorOperations::DeleteVectors(ids, vector_names) => {
            delete_vectors(segments, op_num, &ids.points, &vector_names, hw_counter)
        }
        VectorOperations::DeleteVectorsByFilter(filter, vector_names) => {
            delete_vectors_by_filter(segments, op_num, &filter, &vector_names, hw_counter)
        }
    }
}

pub fn process_payload_operation(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload_operation: PayloadOps,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    match payload_operation {
        PayloadOps::SetPayload(sp) => {
            let payload: Payload = sp.payload;
            if let Some(points) = sp.points {
                set_payload(segments, op_num, &payload, &points, &sp.key, hw_counter)
            } else if let Some(filter) = sp.filter {
                set_payload_by_filter(segments, op_num, &payload, &filter, &sp.key, hw_counter)
            } else {
                // TODO: BadRequest (prev) vs BadInput (current)!?
                Err(OperationError::validation_error(
                    "No points or filter specified",
                ))
            }
        }
        PayloadOps::DeletePayload(dp) => {
            if let Some(points) = dp.points {
                delete_payload(segments, op_num, &points, &dp.keys, hw_counter)
            } else if let Some(filter) = dp.filter {
                delete_payload_by_filter(segments, op_num, &filter, &dp.keys, hw_counter)
            } else {
                // TODO: BadRequest (prev) vs BadInput (current)!?
                Err(OperationError::validation_error(
                    "No points or filter specified",
                ))
            }
        }
        PayloadOps::ClearPayload { ref points, .. } => {
            clear_payload(segments, op_num, points, hw_counter)
        }
        PayloadOps::ClearPayloadByFilter(ref filter) => {
            clear_payload_by_filter(segments, op_num, filter, hw_counter)
        }
        PayloadOps::OverwritePayload(sp) => {
            let payload: Payload = sp.payload;
            if let Some(points) = sp.points {
                overwrite_payload(segments, op_num, &payload, &points, hw_counter)
            } else if let Some(filter) = sp.filter {
                overwrite_payload_by_filter(segments, op_num, &payload, &filter, hw_counter)
            } else {
                // TODO: BadRequest (prev) vs BadInput (current)!?
                Err(OperationError::validation_error(
                    "No points or filter specified",
                ))
            }
        }
    }
}

pub fn process_field_index_operation(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    field_index_operation: &FieldIndexOperations,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    match field_index_operation {
        FieldIndexOperations::CreateIndex(index_data) => create_field_index(
            segments,
            op_num,
            &index_data.field_name,
            index_data.field_schema.as_ref(),
            hw_counter,
        ),
        FieldIndexOperations::DeleteIndex(field_name) => {
            delete_field_index(segments, op_num, field_name)
        }
    }
}

pub fn process_vector_name_operation(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    vector_name_operation: &VectorNameOperations,
) -> OperationResult<usize> {
    match vector_name_operation {
        VectorNameOperations::CreateVectorName(create_data) => {
            let CreateVectorName {
                vector_name,
                config,
            } = create_data;

            segments.apply_segments(|write_segment| {
                write_segment.with_upgraded(|segment| {
                    segment.create_vector_name(op_num, vector_name, config)
                })
            })
        }
        VectorNameOperations::DeleteVectorName(delete_data) => {
            let DeleteVectorName { vector_name } = delete_data;
            segments.apply_segments(|write_segment| {
                write_segment
                    .with_upgraded(|segment| segment.delete_vector_name(op_num, vector_name))
            })
        }
    }
}
