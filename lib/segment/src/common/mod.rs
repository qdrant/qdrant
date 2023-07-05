pub mod anonymize;
pub mod arc_atomic_ref_cell_iterator;
pub mod cpu;
pub mod error_logging;
pub mod file_operations;
pub mod mmap_ops;
pub mod mmap_type;
pub mod operation_time_statistics;
pub mod rocksdb_buffered_delete_wrapper;
pub mod rocksdb_wrapper;
pub mod utils;
pub mod version;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::types::{SegmentConfig, VectorDataConfig};

pub type Flusher = Box<dyn FnOnce() -> OperationResult<()> + Send>;

/// Check that the given vector name is part of the segment config.
///
/// Returns an error if incompatible.
pub fn check_vector_name(vector_name: &str, segment_config: &SegmentConfig) -> OperationResult<()> {
    get_vector_config_or_error(vector_name, segment_config)?;
    Ok(())
}

/// Check that the given vector name and elements are compatible with the given segment config.
///
/// Returns an error if incompatible.
pub fn check_vector(
    vector_name: &str,
    vector: &[VectorElementType],
    segment_config: &SegmentConfig,
) -> OperationResult<()> {
    let vector_config = get_vector_config_or_error(vector_name, segment_config)?;
    check_vector_against_config(vector, vector_config)?;
    Ok(())
}

/// Check that the given vector name and elements are compatible with the given segment config.
///
/// Returns an error if incompatible.
pub fn check_vectors(
    vector_name: &str,
    vectors: &[&[VectorElementType]],
    segment_config: &SegmentConfig,
) -> OperationResult<()> {
    let vector_config = get_vector_config_or_error(vector_name, segment_config)?;
    for vector in vectors {
        check_vector_against_config(vector, vector_config)?;
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
        check_vector(vector_name, vector_data, segment_config)?;
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

/// Check if the given vector data is compatible with the given configuration.
///
/// Returns an error if incompatible.
fn check_vector_against_config(
    vector: &[VectorElementType],
    vector_config: &VectorDataConfig,
) -> OperationResult<()> {
    // Check dimensionality
    let dim = vector_config.size;
    if vector.len() != dim {
        return Err(OperationError::WrongVector {
            expected_dim: dim,
            received_dim: vector.len(),
        });
    }
    Ok(())
}
