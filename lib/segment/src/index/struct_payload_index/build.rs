use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::StructPayloadIndex;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead;
use crate::index::field_index::{FieldIndex, FieldIndexBuilderTrait as _};
use crate::payload_storage::PayloadStorageRead;
use crate::types::{PayloadContainer, PayloadFieldSchema, PayloadKeyTypeRef};

impl StructPayloadIndex {
    pub fn build_field_indexes(
        &self,
        field: PayloadKeyTypeRef,
        payload_schema: &PayloadFieldSchema,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<FieldIndex>> {
        let payload_storage = self.payload.borrow();
        let id_tracker_borrow = self.id_tracker.borrow();
        let selector = self.selector(payload_schema);
        let mut builders = selector.index_builder(
            field,
            payload_schema,
            id_tracker_borrow.deleted_point_bitslice(),
        )?;

        // Special null index complements every index. Seed it with the segment's total
        // point count so `iter_falses()` returns points that are missing from payload
        // storage (e.g. after `clear_payload`), matching the regular "no value" points.
        // Bug: <https://github.com/qdrant/qdrant/issues/8723>
        let total_point_count = self.id_tracker.borrow().total_point_count();
        let null_index = selector.null_builder(field, total_point_count)?;
        builders.push(null_index);

        for index in &mut builders {
            index.init()?;
        }

        payload_storage.iter(
            |point_id, point_payload| {
                let field_value = &point_payload.get_value(field);
                for builder in builders.iter_mut() {
                    builder.add_point(point_id, field_value, hw_counter)?;
                }
                Ok(true)
            },
            hw_counter,
        )?;

        builders
            .into_iter()
            .map(|builder| builder.finalize())
            .collect()
    }

    pub(super) fn clear_index_for_point(
        &mut self,
        point_id: PointOffsetType,
    ) -> OperationResult<()> {
        for (_, field_indexes) in self.field_indexes.iter_mut() {
            for index in field_indexes {
                index.remove_point(point_id)?;
            }
        }
        Ok(())
    }
}
