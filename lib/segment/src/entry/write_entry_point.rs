use common::counter::hardware_counter::HardwareCounterCell;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::build_index_result::BuildFieldIndexResult;
use crate::data_types::named_vectors::NamedVectors;
use crate::index::field_index::FieldIndex;
use crate::json_path::JsonPath;
use crate::types::{
    Payload, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef, PointIdType, SeqNumberType,
    VectorName,
};

/// Define all operations which can be performed with Segment or Segment-like entity.
///
/// Assume all operations are idempotent - which means that no matter how many times an operation
/// is executed - the storage state will be the same.
pub trait SegmentEntryWrite {
    fn upsert_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vectors: NamedVectors,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    fn delete_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    fn update_vectors(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vectors: NamedVectors,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    fn delete_vector(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vector_name: &VectorName,
    ) -> OperationResult<bool>;

    fn set_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        payload: &Payload,
        key: &Option<JsonPath>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    fn set_full_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        full_payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    fn delete_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        key: PayloadKeyTypeRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    fn clear_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    /// Removes all persisted data and forces to destroy segment
    fn drop_data(self) -> OperationResult<()>;

    /// Delete field index, if exists
    fn delete_field_index(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<bool>;

    /// Delete field index, if exists and doesn't match the schema
    fn delete_field_index_if_incompatible(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
        field_schema: &PayloadFieldSchema,
    ) -> OperationResult<bool>;

    /// Build the field index for the key and schema, if not built before.
    fn build_field_index(
        &self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
        field_type: &PayloadFieldSchema,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BuildFieldIndexResult>;

    /// Apply a built index. Returns whether it was actually applied or not.
    fn apply_field_index(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyType,
        field_schema: PayloadFieldSchema,
        field_index: Vec<FieldIndex>,
    ) -> OperationResult<bool>;

    /// Create index for a payload field, if not exists
    fn create_field_index(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
        field_schema: Option<&PayloadFieldSchema>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        let Some(field_schema) = field_schema else {
            // Legacy case, where we tried to automatically detect the schema for the field.
            // We don't do this anymore, as it is not reliable.
            return Err(OperationError::TypeInferenceError {
                field_name: key.clone(),
            });
        };

        self.delete_field_index_if_incompatible(op_num, key, field_schema)?;

        let (schema, indexes) =
            match self.build_field_index(op_num, key, field_schema, hw_counter)? {
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
                        "Incompatible schema for field index on field {key}",
                    )));
                }
                BuildFieldIndexResult::Built { schema, indexes } => (schema, indexes),
            };

        self.apply_field_index(op_num, key.to_owned(), schema, indexes)
    }
}
