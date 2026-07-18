use std::fmt;

use common::counter::hardware_counter::HardwareCounterCell;
use segment::common::operation_error::{OperationError, OperationResult};
use shard::operations::vector_name_ops::VectorNameConfig;
use shard::operations::{CollectionUpdateOperations, VectorNameOperations};
use shard::update::*;
use shard::wal::WalRawRecord;

use crate::EdgeShard;
use crate::config::vectors::{EdgeSparseVectorParams, EdgeVectorParams};

impl EdgeShard {
    pub fn update(&self, operation: CollectionUpdateOperations) -> OperationResult<()> {
        let record = WalRawRecord::new(&operation).map_err(service_error)?;

        let mut wal = self.wal.lock();

        let operation_id = wal.write(&record).map_err(service_error)?;
        let hw_counter = HardwareCounterCell::disposable();
        let _update_guard = self.segments.acquire_updates_lock();

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
            CollectionUpdateOperations::VectorNameOperation(ref vector_name_operation) => {
                let result = process_vector_name_operation(
                    &segments_guard,
                    operation_id,
                    vector_name_operation,
                );
                // Also update the edge shard config so queries can resolve the vector name
                if result.is_ok() {
                    self.apply_vector_name_to_config(vector_name_operation)?;
                }
                result
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

    /// Update the edge shard config to reflect a vector name create/delete operation.
    fn apply_vector_name_to_config(&self, operation: &VectorNameOperations) -> OperationResult<()> {
        match operation {
            VectorNameOperations::CreateVectorName(create) => {
                self.config
                    .write(|config| match &create.config {
                        VectorNameConfig::Dense(wrapper) => {
                            config.vectors.insert(
                                create.vector_name.clone(),
                                EdgeVectorParams {
                                    size: wrapper.dense.size,
                                    distance: wrapper.dense.distance,
                                    on_disk: None,
                                    multivector_config: wrapper.dense.multivector_config,
                                    datatype: wrapper.dense.datatype,
                                    quantization_config: None,
                                    hnsw_config: None,
                                },
                            );
                        }
                        VectorNameConfig::Sparse(wrapper) => {
                            config.sparse_vectors.insert(
                                create.vector_name.clone(),
                                EdgeSparseVectorParams {
                                    full_scan_threshold: None,
                                    on_disk: None,
                                    modifier: wrapper.sparse.modifier,
                                    datatype: wrapper.sparse.datatype,
                                },
                            );
                        }
                    })
                    .map_err(service_error)?;
            }
            VectorNameOperations::DeleteVectorName(delete) => {
                self.config
                    .write(|config| {
                        config.vectors.remove(&delete.vector_name);
                        config.sparse_vectors.remove(&delete.vector_name);
                    })
                    .map_err(service_error)?;
            }
        }
        Ok(())
    }
}

fn service_error(err: impl fmt::Display) -> OperationError {
    OperationError::service_error(err.to_string())
}
