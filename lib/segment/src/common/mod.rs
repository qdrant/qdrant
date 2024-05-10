pub mod anonymize;
pub mod error_logging;
pub mod macros;
pub mod mmap_type;
pub mod operation_error;
pub mod operation_time_statistics;
pub mod rocksdb_buffered_delete_wrapper;
pub mod rocksdb_buffered_update_wrapper;
pub mod rocksdb_wrapper;
pub mod utils;
pub mod validate_snapshot_archive;
pub mod vector_utils;
pub mod version;

use std::sync::atomic::AtomicBool;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::{QueryVector, VectorRef};
use crate::types::{SegmentConfig, SparseVectorDataConfig, VectorDataConfig};

pub type Flusher = Box<dyn FnOnce() -> OperationResult<()> + Send>;

/// Check that the given vector name is part of the segment config.
///
/// Returns an error if incompatible.
pub fn check_vector_name(vector_name: &str, segment_config: &SegmentConfig) -> OperationResult<()> {
    // TODO(sparse) it's a wrong error check. We use the fact,
    // that get_vector_config_or_error can return only one type of error - VectorNameNotExists
    if get_vector_config_or_error(vector_name, segment_config).is_err() {
        get_sparse_vector_config_or_error(vector_name, segment_config)?;
    }
    Ok(())
}

/// Check that the given vector name and elements are compatible with the given segment config.
///
/// Returns an error if incompatible.
pub fn check_vector(
    vector_name: &str,
    query_vector: &QueryVector,
    segment_config: &SegmentConfig,
) -> OperationResult<()> {
    let vector_config = get_vector_config_or_error(vector_name, segment_config);
    if vector_config.is_ok() {
        check_query_vector(query_vector, vector_config?)
    } else {
        let sparse_vector_config = get_sparse_vector_config_or_error(vector_name, segment_config)?;
        check_query_sparse_vector(query_vector, sparse_vector_config)
    }
}

fn check_query_vector(
    query_vector: &QueryVector,
    vector_config: &VectorDataConfig,
) -> OperationResult<()> {
    match query_vector {
        QueryVector::Nearest(vector) => {
            check_vector_against_config(VectorRef::from(vector), vector_config)?
        }
        QueryVector::Recommend(reco_query) => reco_query.flat_iter().try_for_each(|vector| {
            check_vector_against_config(VectorRef::from(vector), vector_config)
        })?,
        QueryVector::Discovery(discovery_query) => {
            discovery_query.flat_iter().try_for_each(|vector| {
                check_vector_against_config(VectorRef::from(vector), vector_config)
            })?
        }
        QueryVector::Context(discovery_context_query) => {
            discovery_context_query.flat_iter().try_for_each(|vector| {
                check_vector_against_config(VectorRef::from(vector), vector_config)
            })?
        }
    }

    Ok(())
}

fn check_query_sparse_vector(
    query_vector: &QueryVector,
    vector_config: &SparseVectorDataConfig,
) -> OperationResult<()> {
    match query_vector {
        QueryVector::Nearest(vector) => {
            check_sparse_vector_against_config(VectorRef::from(vector), vector_config)?
        }
        QueryVector::Recommend(reco_query) => reco_query.flat_iter().try_for_each(|vector| {
            check_sparse_vector_against_config(VectorRef::from(vector), vector_config)
        })?,
        QueryVector::Discovery(discovery_query) => {
            discovery_query.flat_iter().try_for_each(|vector| {
                check_sparse_vector_against_config(VectorRef::from(vector), vector_config)
            })?
        }
        QueryVector::Context(discovery_context_query) => {
            discovery_context_query.flat_iter().try_for_each(|vector| {
                check_sparse_vector_against_config(VectorRef::from(vector), vector_config)
            })?
        }
    }

    Ok(())
}

/// Check that the given vector name and elements are compatible with the given segment config.
///
/// Returns an error if incompatible.
pub fn check_query_vectors(
    vector_name: &str,
    query_vectors: &[&QueryVector],
    segment_config: &SegmentConfig,
) -> OperationResult<()> {
    let vector_config = get_vector_config_or_error(vector_name, segment_config);
    if let Ok(vector_config) = vector_config {
        query_vectors
            .iter()
            .try_for_each(|qv| check_query_vector(qv, vector_config))?;
    } else {
        let sparse_vector_config = get_sparse_vector_config_or_error(vector_name, segment_config)?;
        query_vectors
            .iter()
            .try_for_each(|qv| check_query_sparse_vector(qv, sparse_vector_config))?;
    }
    Ok(())
}

/// Check that the given named vectors are compatible with the given segment config.
///
/// Returns an error if incompatible.
pub fn check_named_vectors(
    vectors: &NamedVectors,
    segment_config: &SegmentConfig,
) -> OperationResult<()> {
    for (vector_name, vector_data) in vectors.iter() {
        check_vector(vector_name, &vector_data.into(), segment_config)?;
    }
    Ok(())
}

/// Get the vector config for the given name, or return a name error.
///
/// Returns an error if incompatible.
fn get_vector_config_or_error<'a>(
    vector_name: &str,
    segment_config: &'a SegmentConfig,
) -> OperationResult<&'a VectorDataConfig> {
    segment_config
        .vector_data
        .get(vector_name)
        .ok_or_else(|| OperationError::VectorNameNotExists {
            received_name: vector_name.into(),
        })
}

/// Get the sparse vector config for the given name, or return a name error.
///
/// Returns an error if incompatible.
fn get_sparse_vector_config_or_error<'a>(
    vector_name: &str,
    segment_config: &'a SegmentConfig,
) -> OperationResult<&'a SparseVectorDataConfig> {
    segment_config
        .sparse_vector_data
        .get(vector_name)
        .ok_or_else(|| OperationError::VectorNameNotExists {
            received_name: vector_name.into(),
        })
}

/// Check if the given dense vector data is compatible with the given configuration.
///
/// Returns an error if incompatible.
fn check_vector_against_config(
    vector: VectorRef,
    vector_config: &VectorDataConfig,
) -> OperationResult<()> {
    match vector {
        VectorRef::Dense(vector) => {
            // Check dimensionality
            let dim = vector_config.size;
            if vector.len() != dim {
                return Err(OperationError::WrongVectorDimension {
                    expected_dim: dim,
                    received_dim: vector.len(),
                });
            }
            Ok(())
        }
        VectorRef::Sparse(_) => Err(OperationError::WrongSparse),
        VectorRef::MultiDense(multi_vector) => {
            // Check dimensionality
            let dim = vector_config.size;
            for vector in multi_vector.multi_vectors() {
                if vector.len() != dim {
                    return Err(OperationError::WrongVectorDimension {
                        expected_dim: dim,
                        received_dim: vector.len(),
                    });
                }
            }
            Ok(())
        }
    }
}

fn check_sparse_vector_against_config(
    vector: VectorRef,
    _vector_config: &SparseVectorDataConfig,
) -> OperationResult<()> {
    match vector {
        VectorRef::Dense(_) => Err(OperationError::WrongSparse),
        VectorRef::Sparse(_vector) => Ok(()), // TODO(sparse) check vector by config
        VectorRef::MultiDense(_) => Err(OperationError::WrongMulti),
    }
}

pub fn check_stopped(is_stopped: &AtomicBool) -> OperationResult<()> {
    if is_stopped.load(std::sync::atomic::Ordering::Relaxed) {
        return Err(OperationError::Cancelled {
            description: "Operation is stopped externally".to_string(),
        });
    }
    Ok(())
}

pub const BYTES_IN_KB: usize = 1024;
