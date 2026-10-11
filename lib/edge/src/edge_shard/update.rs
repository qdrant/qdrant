#![allow(deprecated)]
use std::collections::HashSet;
use std::fmt;

use common::ambient;
use common::reason::Reason;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::types::{SeqNumberType, VectorNameBuf};
use shard::operations::vector_name_ops::VectorNameConfig;
use shard::operations::{CollectionUpdateOperations, OperationWithClockTag, VectorNameOperations};
use shard::update::*;

use super::update_log::UpdateLog;
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

        let record = OperationWithClockTag::new(operation, None);

        let mut update_log = self.update_log.lock();

        let operation_id = update_log.append(&record)?;
        let OperationWithClockTag {
            operation,
            clock_tag: _,
        } = record;
        self.apply(operation_id, operation)
    }

    /// Re-apply the WAL entries not yet acknowledged as persisted by the segments, so that
    /// operations written to the WAL before a crash are not lost.
    ///
    /// Entries come from this shard's own updates or from a Qdrant snapshot's WAL. Re-applying an
    /// operation the segments already hold is a no-op thanks to point versions. An operation that
    /// fails with a user error is skipped, as it failed the same way when it was first applied.
    pub(super) fn replay_wal(&self) -> OperationResult<()> {
        let update_log = self.update_log.lock();
        let wal = match &*update_log {
            UpdateLog::Wal(wal) => wal,
            UpdateLog::Disabled {
                last_operation_id: _,
            } => return Ok(()),
        };

        // Operations may reference a vector name that was deleted later. The config already
        // reflects the deletion, so such operations would fail validation and drop all their
        // points; strip the dead names instead, as the collection-level replay does.
        let mut valid_vector_names: HashSet<VectorNameBuf> = {
            let config = self.config.read();
            config
                .vectors
                .keys()
                .chain(config.sparse_vectors.keys())
                .cloned()
                .collect()
        };

        for entry in wal.read(wal.first_index()) {
            let (operation_id, record) = entry.map_err(|err| {
                OperationError::service_error(format!("failed to read WAL entry: {err}"))
            })?;
            let OperationWithClockTag {
                mut operation,
                clock_tag: _,
            } = record;

            if let Some(name) = operation.created_vector_name() {
                valid_vector_names.insert(name.clone());
            }
            operation.retain_vector_names(&valid_vector_names);

            if let Err(err) = self.apply(operation_id, operation) {
                if aborts_wal_replay(&err) {
                    return Err(err);
                }
                log::warn!("skipping WAL operation {operation_id} that failed to apply: {err}");
            }
        }

        Ok(())
    }

    /// Apply an operation already written to the WAL under `operation_id`.
    fn apply(
        &self,
        operation_id: SeqNumberType,
        operation: CollectionUpdateOperations,
    ) -> OperationResult<()> {
        let _scope = ambient::unmeasured_guard(Reason::EDGE_UNMEASURED);
        let _update_guard = self.segments.acquire_updates_lock();

        let segments_guard = self.segments.read();

        let result = match operation {
            CollectionUpdateOperations::PointOperation(point_operation) => {
                process_point_operation(
                    &segments_guard,
                    operation_id,
                    point_operation,
                    None, // edge shards have a single appendable segment, no size cap to apply
                )
            }
            CollectionUpdateOperations::VectorOperation(vector_operation) => {
                process_vector_operation(&segments_guard, operation_id, vector_operation, None)
            }
            CollectionUpdateOperations::PayloadOperation(payload_operation) => {
                process_payload_operation(&segments_guard, operation_id, payload_operation, None)
            }
            CollectionUpdateOperations::FieldIndexOperation(index_operation) => {
                process_field_index_operation(&segments_guard, operation_id, &index_operation)
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
            memory: None,
            multivector_config: wrapper.dense.multivector_config,
            datatype: wrapper.dense.datatype,
            quantization_config: None,
            hnsw_config: None,
        }),
        VectorNameConfig::Sparse(wrapper) => {
            RequestedVectorParams::Sparse(EdgeSparseVectorParams {
                full_scan_threshold: None,
                on_disk: None,
                memory: None,
                modifier: wrapper.sparse.modifier,
                datatype: wrapper.sparse.datatype,
            })
        }
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

/// Whether a failure to re-apply a WAL operation must fail the shard load. Storage and resource
/// failures abort it; user errors are skipped, since they rejected the operation the first time too.
fn aborts_wal_replay(err: &OperationError) -> bool {
    match err {
        OperationError::ServiceError { .. }
        | OperationError::InconsistentStorage { .. }
        | OperationError::FileNotFound { .. }
        | OperationError::OutOfMemory { .. }
        | OperationError::OutOfAppendableCapacity { .. } => true,
        OperationError::WrongVectorDimension { .. }
        | OperationError::MalformedVectorBlob { .. }
        | OperationError::MalformedPayloadBlob { .. }
        | OperationError::VectorNameNotExists { .. }
        | OperationError::PointIdError { .. }
        | OperationError::TypeError { .. }
        | OperationError::TypeInferenceError { .. }
        | OperationError::Cancelled { .. }
        | OperationError::Timeout { .. }
        | OperationError::ValidationError { .. }
        | OperationError::WrongSparse
        | OperationError::WrongMulti
        | OperationError::MissingRangeIndexForOrderBy { .. }
        | OperationError::MissingMapIndexForFacet { .. }
        | OperationError::VariableTypeError { .. }
        | OperationError::NonFiniteNumber { .. } => false,
    }
}

fn service_error(err: impl fmt::Display) -> OperationError {
    OperationError::service_error(err.to_string())
}
