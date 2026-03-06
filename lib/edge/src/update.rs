use std::fmt;

use common::counter::hardware_counter::HardwareCounterCell;
use segment::common::operation_error::{OperationError, OperationResult};
use shard::operations::CollectionUpdateOperations;
use shard::update::*;
use shard::wal::WalRawRecord;

use crate::EdgeShard;

impl EdgeShard {
    pub fn update(&self, operation: CollectionUpdateOperations) -> OperationResult<()> {
        let record = WalRawRecord::new(&operation).map_err(service_error)?;

        let mut wal = self.wal.lock();

        let operation_id = wal.write(&record).map_err(service_error)?;
        let hw_counter = HardwareCounterCell::disposable();

        let segments_guard = self.segments.read();

        let result = match operation {
            CollectionUpdateOperations::PointOperation(point_operation) => {
                process_point_operation(&segments_guard, operation_id, point_operation, &hw_counter)
            }
            CollectionUpdateOperations::VectorOperation(vector_operation) => {
                process_vector_operation(
                    &segments_guard,
                    operation_id,
                    vector_operation,
                    &hw_counter,
                )
            }
            CollectionUpdateOperations::PayloadOperation(payload_operation) => {
                process_payload_operation(
                    &segments_guard,
                    operation_id,
                    payload_operation,
                    &hw_counter,
                )
            }
            CollectionUpdateOperations::FieldIndexOperation(index_operation) => {
                process_field_index_operation(
                    &segments_guard,
                    operation_id,
                    &index_operation,
                    &hw_counter,
                )
            }
            #[cfg(feature = "staging")]
            CollectionUpdateOperations::StagingOperation(staging_operation) => {
                shard::update::process_staging_operation(
                    &segments_guard,
                    operation_id,
                    staging_operation,
                )
            }
        };

        result.map(|_| ())
    }
}

fn service_error(err: impl fmt::Display) -> OperationError {
    OperationError::service_error(err.to_string())
}
