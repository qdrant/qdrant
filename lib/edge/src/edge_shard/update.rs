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
        // Reject a conflicting vector-name re-create before it reaches the WAL:
        // the segment-level create is idempotent, so re-creating an existing
        // name with different params silently no-ops storage. Failing loudly up
        // front keeps the bad op out of the WAL and stops `config()` from
        // drifting away from the stored vectors.
        if let CollectionUpdateOperations::VectorNameOperation(
            VectorNameOperations::CreateVectorName(create),
        ) = &operation
        {
            self.check_vector_name_create_compatible(&create.vector_name, &create.config)?;
        }

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
                // Keep the edge shard config in sync with storage so queries can
                // resolve the vector name — but only when storage actually
                // changed. A re-create of an existing name is an idempotent
                // no-op (0 segments changed); re-applying its params would
                // desync `config()` from what is stored.
                if let Ok(changed) = result {
                    self.apply_vector_name_to_config(vector_name_operation, changed)?;
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

    /// Rejects a re-create whose params conflict with the vector already
    /// advertised under the same name. Called before the op reaches the WAL.
    fn check_vector_name_create_compatible(
        &self,
        name: &str,
        config: &VectorNameConfig,
    ) -> OperationResult<()> {
        // Compare only the identity fields a `CreateVectorName` op actually
        // defines. Storage/tuning fields (`on_disk`, `quantization_config`,
        // `hnsw_config`, `full_scan_threshold`) are populated from the initial
        // config, the optimizer, or `set_*_config` — e.g. a config-defined
        // vector is stored with `on_disk: Some(false)` while the op leaves it
        // `None` — so full-struct equality would flag an otherwise-identical
        // re-create as a false conflict.
        let conflict = match requested_vector_params(config) {
            RequestedVectorParams::Dense(params) => self
                .config
                .read()
                .vectors
                .get(name)
                .is_some_and(|existing| !dense_identity_matches(existing, &params)),
            RequestedVectorParams::Sparse(params) => self
                .config
                .read()
                .sparse_vectors
                .get(name)
                .is_some_and(|existing| !sparse_identity_matches(existing, &params)),
        };
        if conflict {
            return Err(OperationError::validation_error(format!(
                "vector '{name}' already exists with different parameters; delete \
                 it before recreating with a different configuration"
            )));
        }
        Ok(())
    }

    /// Update the edge shard config to reflect a vector name create/delete operation.
    fn apply_vector_name_to_config(
        &self,
        operation: &VectorNameOperations,
        changed: usize,
    ) -> OperationResult<()> {
        match operation {
            VectorNameOperations::CreateVectorName(create) => {
                // Only record params when storage actually created the vector.
                // A no-op re-create (`changed == 0`) must not overwrite the
                // config; `update` has already rejected any conflicting
                // re-create, so a no-op here is a benign duplicate whose params
                // already match what is stored.
                if changed == 0 {
                    return Ok(());
                }
                self.config
                    .write(|config| match requested_vector_params(&create.config) {
                        RequestedVectorParams::Dense(params) => {
                            config.vectors.insert(create.vector_name.clone(), params);
                        }
                        RequestedVectorParams::Sparse(params) => {
                            config
                                .sparse_vectors
                                .insert(create.vector_name.clone(), params);
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

/// Dense- or sparse-vector params requested by a `CreateVectorName` op,
/// projected onto the edge config's user-facing shape.
enum RequestedVectorParams {
    Dense(EdgeVectorParams),
    Sparse(EdgeSparseVectorParams),
}

fn requested_vector_params(config: &VectorNameConfig) -> RequestedVectorParams {
    match config {
        VectorNameConfig::Dense(wrapper) => RequestedVectorParams::Dense(EdgeVectorParams {
            size: wrapper.dense.size,
            distance: wrapper.dense.distance,
            on_disk: None,
            multivector_config: wrapper.dense.multivector_config,
            datatype: wrapper.dense.datatype,
            quantization_config: None,
            hnsw_config: None,
        }),
        VectorNameConfig::Sparse(wrapper) => RequestedVectorParams::Sparse(EdgeSparseVectorParams {
            full_scan_threshold: None,
            on_disk: None,
            modifier: wrapper.sparse.modifier,
            datatype: wrapper.sparse.datatype,
        }),
    }
}

/// Whether two dense vectors share the identity a `CreateVectorName` op
/// defines. Excludes storage/tuning fields (`on_disk`, `quantization_config`,
/// `hnsw_config`) that the op cannot express and that are set elsewhere.
fn dense_identity_matches(existing: &EdgeVectorParams, requested: &EdgeVectorParams) -> bool {
    existing.size == requested.size
        && existing.distance == requested.distance
        && existing.multivector_config == requested.multivector_config
        && existing.datatype == requested.datatype
}

/// Whether two sparse vectors share the identity a `CreateVectorName` op
/// defines. Excludes `on_disk`/`full_scan_threshold`, which the op cannot
/// express.
fn sparse_identity_matches(
    existing: &EdgeSparseVectorParams,
    requested: &EdgeSparseVectorParams,
) -> bool {
    existing.modifier == requested.modifier && existing.datatype == requested.datatype
}

fn service_error(err: impl fmt::Display) -> OperationError {
    OperationError::service_error(err.to_string())
}
