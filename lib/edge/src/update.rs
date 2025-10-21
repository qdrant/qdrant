use std::fmt;

use common::counter::hardware_counter::HardwareCounterCell;
use segment::common::operation_error::{OperationError, OperationResult};
use shard::operations::CollectionUpdateOperations;
use shard::update::*;

use crate::Shard;

impl Shard {
    pub fn update(&self, operation: CollectionUpdateOperations) -> OperationResult<()> {
        let mut wal = self.wal.lock();

        let operation_id = wal.write(&operation).map_err(service_error)?;
        let hw_counter = HardwareCounterCell::disposable();

        let result = match operation {
            CollectionUpdateOperations::PointOperation(point_operation) => {
                process_point_operation(&self.segments, operation_id, point_operation, &hw_counter)
            }
            CollectionUpdateOperations::VectorOperation(vector_operation) => {
                process_vector_operation(
                    &self.segments,
                    operation_id,
                    vector_operation,
                    &hw_counter,
                )
            }
            CollectionUpdateOperations::PayloadOperation(payload_operation) => {
                process_payload_operation(
                    &self.segments,
                    operation_id,
                    payload_operation,
                    &hw_counter,
                )
            }
            CollectionUpdateOperations::FieldIndexOperation(index_operation) => {
                process_field_index_operation(
                    &self.segments,
                    operation_id,
                    &index_operation,
                    &hw_counter,
                )
            }
        };

        result.map(|_| ())
    }
}

fn service_error(err: impl fmt::Display) -> OperationError {
    OperationError::service_error(err.to_string())
}
