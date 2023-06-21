pub mod anonymize;
pub mod arc_atomic_ref_cell_iterator;
pub mod cpu;
pub mod error_logging;
pub mod file_operations;
pub mod mmap_ops;
pub mod mmap_type;
pub mod operation_time_statistics;
pub mod rocksdb_wrapper;
pub mod utils;
pub mod version;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::types::SegmentConfig;

pub type Flusher = Box<dyn FnOnce() -> OperationResult<()> + Send>;

/// Check that the given vector name is part of the segment config.
///
/// Returns an error if incompatible.
pub fn check_vector_name(vector_name: &str, segment_config: &SegmentConfig) -> OperationResult<()> {
    if !segment_config.vector_data.contains_key(vector_name) {
        return Err(OperationError::VectorNameNotExists {
            received_name: vector_name.to_owned(),
        });
    }
    Ok(())
}

/// Check that the given named vectors are compatible with the given segment config.
///
/// Returns an error if incompatible.
pub fn check_vectors_set(
    vectors: &NamedVectors,
    segment_config: &SegmentConfig,
) -> OperationResult<()> {
    for (vector_name, vector_data) in vectors.iter() {
        check_vector(vector_name, vector_data, segment_config)?;
    }
    Ok(())
}

/// Check that the given vector name and elements are compatible with the given segment config.
///
/// Returns an error if incompatible.
#[inline]
pub fn check_vector(
    vector_name: &str,
    vector: &[VectorElementType],
    segment_config: &SegmentConfig,
) -> OperationResult<()> {
    // TODO: ? check_vector_name(vector_name, segment_config)?;

    // Grab vector data
    let vector_config = match segment_config.vector_data.get(vector_name) {
        Some(vector_data) => vector_data,
        None => {
            return Err(OperationError::VectorNameNotExists {
                received_name: vector_name.to_owned(),
            })
        }
    };

    // Check vector dimensionality
    let dim = vector_config.size;
    if vector.len() != dim {
        return Err(OperationError::WrongVector {
            expected_dim: dim,
            received_dim: vector.len(),
        });
    }

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
    for vector in vectors {
        check_vector(vector_name, vector, segment_config)?;
    }
    Ok(())
}
