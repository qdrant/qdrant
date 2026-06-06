pub mod anonymize;
pub mod buffered_update_bitslice;
pub mod error_logging;
pub mod flags;
pub mod live_reload;
pub mod macros;
pub mod memory_usage;
pub mod operation_error;
pub mod operation_time_statistics;
pub mod reciprocal_rank_fusion;
pub mod score_fusion;
pub mod utils;
pub mod validate_snapshot_archive;
pub mod vector_utils;

use std::sync::atomic::AtomicBool;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::{QueryVector, VectorRef};
use crate::types::{SegmentConfig, SparseVectorDataConfig, VectorDataConfig, VectorName};

pub type Flusher = Box<dyn FnOnce() -> OperationResult<()> + Send>;

/// Check that the given vector name is part of the segment config.
///
/// Returns an error if incompatible.
pub fn check_vector_name(
    vector_name: &VectorName,
    segment_config: &SegmentConfig,
) -> OperationResult<()> {
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
    vector_name: &VectorName,
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
        QueryVector::RecommendBestScore(reco_query)
        | QueryVector::RecommendSumScores(reco_query) => {
            reco_query.flat_iter().try_for_each(|vector| {
                check_vector_against_config(VectorRef::from(vector), vector_config)
            })?
        }
        QueryVector::Discover(discover_query) => {
            discover_query.flat_iter().try_for_each(|vector| {
                check_vector_against_config(VectorRef::from(vector), vector_config)
            })?
        }
        QueryVector::Context(context_query) => {
            context_query.flat_iter().try_for_each(|vector| {
                check_vector_against_config(VectorRef::from(vector), vector_config)
            })?
        }
        QueryVector::FeedbackNaive(feedback_query) => {
            feedback_query.flat_iter().try_for_each(|vector| {
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
        QueryVector::RecommendBestScore(reco_query)
        | QueryVector::RecommendSumScores(reco_query) => {
            reco_query.flat_iter().try_for_each(|vector| {
                check_sparse_vector_against_config(VectorRef::from(vector), vector_config)
            })?
        }
        QueryVector::Discover(discover_query) => {
            discover_query.flat_iter().try_for_each(|vector| {
                check_sparse_vector_against_config(VectorRef::from(vector), vector_config)
            })?
        }
        QueryVector::Context(context_query) => {
            context_query.flat_iter().try_for_each(|vector| {
                check_sparse_vector_against_config(VectorRef::from(vector), vector_config)
            })?
        }
        QueryVector::FeedbackNaive(feedback_query) => {
            feedback_query.flat_iter().try_for_each(|vector| {
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
    vector_name: &VectorName,
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
    vector_name: &VectorName,
    segment_config: &'a SegmentConfig,
) -> OperationResult<&'a VectorDataConfig> {
    segment_config
        .vector_data
        .get(vector_name)
        .ok_or_else(|| OperationError::vector_name_not_exists(vector_name))
}

/// Get the sparse vector config for the given name, or return a name error.
///
/// Returns an error if incompatible.
fn get_sparse_vector_config_or_error<'a>(
    vector_name: &VectorName,
    segment_config: &'a SegmentConfig,
) -> OperationResult<&'a SparseVectorDataConfig> {
    segment_config
        .sparse_vector_data
        .get(vector_name)
        .ok_or_else(|| OperationError::vector_name_not_exists(vector_name))
}

fn check_dense_values_finite(vector: &[f32]) -> OperationResult<()> {
    if let Some(index) = vector.iter().position(|value| !value.is_finite()) {
        return Err(OperationError::validation_error(format!(
            "Vector contains non-finite value at index {index}"
        )));
    }
    Ok(())
}

fn check_dense_magnitude_bound(vector: &[f32], magnitude_bound: f32) -> OperationResult<()> {
    let magnitude_squared: f32 = vector.iter().map(|value| value * value).sum();
    if magnitude_squared > magnitude_bound * magnitude_bound {
        return Err(OperationError::validation_error(format!(
            "Vector magnitude {magnitude_squared:.6} exceeds bound {magnitude_bound}"
        )));
    }
    Ok(())
}

fn check_sparse_values_finite(values: &[f32]) -> OperationResult<()> {
    if let Some(index) = values.iter().position(|value| !value.is_finite()) {
        return Err(OperationError::validation_error(format!(
            "Sparse vector contains non-finite value at index {index}"
        )));
    }
    Ok(())
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
            if vector_config.data_integrity_check_enabled() {
                check_dense_values_finite(vector)?;
            }
            if let Some(magnitude_bound) = vector_config.magnitude_bound {
                check_dense_magnitude_bound(vector, magnitude_bound)?;
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
                if vector_config.data_integrity_check_enabled() {
                    check_dense_values_finite(vector)?;
                }
                if let Some(magnitude_bound) = vector_config.magnitude_bound {
                    check_dense_magnitude_bound(vector, magnitude_bound)?;
                }
            }
            Ok(())
        }
    }
}

fn check_sparse_vector_against_config(
    vector: VectorRef,
    vector_config: &SparseVectorDataConfig,
) -> OperationResult<()> {
    match vector {
        VectorRef::Dense(_) => Err(OperationError::WrongSparse),
        VectorRef::Sparse(vector) => {
            if vector_config.data_integrity_check_enabled() {
                check_sparse_values_finite(&vector.values)?;
            }
            if let Some(magnitude_bound) = vector_config.magnitude_bound {
                check_dense_magnitude_bound(&vector.values, magnitude_bound)?;
            }
            Ok(())
        }
        VectorRef::MultiDense(_) => Err(OperationError::WrongMulti),
    }
}

#[cfg(test)]
mod tests {
    use crate::data_types::vectors::VectorRef;
    use crate::types::{Distance, Indexes, VectorDataConfig, VectorStorageType};

    use super::*;

    #[test]
    fn rejects_non_finite_dense_vector() {
        let config = VectorDataConfig {
            size: 3,
            distance: Distance::Cosine,
            storage_type: VectorStorageType::Memory,
            index: Indexes::Plain {},
            quantization_config: None,
            multivector_config: None,
            datatype: None,
            data_integrity_check: None,
            magnitude_bound: None,
        };
        let vector = vec![1.0, f32::NAN, 1.0];
        let err = check_vector_against_config(VectorRef::Dense(&vector), &config).unwrap_err();
        assert!(matches!(err, OperationError::ValidationError { .. }));
    }

    #[test]
    fn rejects_vector_exceeding_magnitude_bound() {
        let config = VectorDataConfig {
            size: 3,
            distance: Distance::Cosine,
            storage_type: VectorStorageType::Memory,
            index: Indexes::Plain {},
            quantization_config: None,
            multivector_config: None,
            datatype: None,
            data_integrity_check: None,
            magnitude_bound: Some(1.0),
        };
        let vector = vec![1.0, 1.0, 1.0];
        let err = check_vector_against_config(VectorRef::Dense(&vector), &config).unwrap_err();
        assert!(matches!(err, OperationError::ValidationError { .. }));
    }

    #[test]
    fn allows_non_finite_vector_when_integrity_check_disabled() {
        let config = VectorDataConfig {
            size: 3,
            distance: Distance::Cosine,
            storage_type: VectorStorageType::Memory,
            index: Indexes::Plain {},
            quantization_config: None,
            multivector_config: None,
            datatype: None,
            data_integrity_check: Some(false),
            magnitude_bound: None,
        };
        let vector = vec![1.0, f32::NAN, 1.0];
        check_vector_against_config(VectorRef::Dense(&vector), &config).unwrap();
    }

    #[test]
    fn rejects_non_finite_sparse_vector() {
        use crate::index::sparse_index::sparse_index_config::{
            SparseIndexConfig, SparseIndexType,
        };
        use crate::types::{SparseVectorDataConfig, SparseVectorStorageType};

        let vector = sparse::common::sparse_vector::SparseVector {
            indices: vec![0, 1],
            values: vec![1.0, f32::INFINITY],
        };
        let config = SparseVectorDataConfig {
            index: SparseIndexConfig::new(None, SparseIndexType::MutableRam, None),
            storage_type: SparseVectorStorageType::OnDisk,
            modifier: None,
            data_integrity_check: None,
            magnitude_bound: None,
        };
        let err = check_sparse_vector_against_config(VectorRef::Sparse(&vector), &config)
            .unwrap_err();
        assert!(matches!(err, OperationError::ValidationError { .. }));
    }
}

pub fn check_stopped(is_stopped: &AtomicBool) -> OperationResult<()> {
    if is_stopped.load(std::sync::atomic::Ordering::Relaxed) {
        return Err(OperationError::cancelled("Operation is stopped externally"));
    }
    Ok(())
}

pub const BYTES_IN_KB: usize = 1024;
pub const BYTES_IN_MB: usize = 1_048_576;
