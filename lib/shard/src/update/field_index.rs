//! Payload field index operations: create and delete field indexes.

use common::counter::hardware_counter::HardwareCounterCell;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::build_index_result::BuildFieldIndexResult;
use segment::types::{PayloadFieldSchema, PayloadKeyTypeRef, SeqNumberType};

use crate::segment_holder::SegmentHolder;

pub fn create_field_index(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    field_name: PayloadKeyTypeRef,
    field_schema: Option<&PayloadFieldSchema>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let Some(field_schema) = field_schema else {
        return Err(OperationError::TypeInferenceError {
            field_name: field_name.to_owned(),
        });
    };

    segments.apply_segments(|write_segment| {
        write_segment.with_upgraded(|segment| {
            segment.delete_field_index_if_incompatible(op_num, field_name, field_schema)
        })?;

        // Pin the state the build will observe before the index config durably marks
        // the index as complete. The build reads the live id tracker and payload
        // storage, which may hold changes that are not flushed yet; if such a change
        // were lost in a crash, the durable index (and the config that WAL replay
        // trusts via `AlreadyBuilt`) would permanently disagree with the durable
        // payload. Flushing the whole segment first makes every crash outcome
        // consistent: either the config is not durable yet and replay rebuilds, or
        // config, index data, and observed state are durable together.
        //
        // Serialized with the flush pipeline: an extra flush run between another
        // flusher's capture and its execution corrupts storage (see
        // `SegmentHolder::with_flush_serialized`). Skipped for idempotent
        // ensure-calls (index already present with an identical schema), which must
        // stay cheap; those resolve to `AlreadyExists` in the build below.
        let already_indexed =
            write_segment.get_indexed_fields().get(field_name) == Some(field_schema);
        if !already_indexed {
            segments.with_flush_serialized(|| write_segment.flush(true))?;
        }

        let (schema, indexes) =
            match write_segment.build_field_index(op_num, field_name, field_schema, hw_counter)? {
                BuildFieldIndexResult::SkippedByVersion => {
                    return Ok(false);
                }
                BuildFieldIndexResult::AlreadyExists => {
                    return Ok(false);
                }
                BuildFieldIndexResult::IncompatibleSchema => {
                    // This is a service error, as we should have just removed the old index
                    // So it should not be possible to get this error
                    return Err(OperationError::service_error(format!(
                        "Incompatible schema for field index on field {field_name}",
                    )));
                }
                BuildFieldIndexResult::Built { schema, indexes } => (schema, indexes),
            };

        write_segment.with_upgraded(|segment| {
            segment.apply_field_index(op_num, field_name.to_owned(), schema, indexes)
        })
    })
}

pub fn delete_field_index(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    field_name: PayloadKeyTypeRef,
) -> OperationResult<usize> {
    segments.apply_segments(|write_segment| {
        write_segment.with_upgraded(|segment| segment.delete_field_index(op_num, field_name))
    })
}
