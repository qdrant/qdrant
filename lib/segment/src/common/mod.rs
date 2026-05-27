pub mod anonymize;
pub mod buffered_update_bitslice;
pub mod error_logging;
pub mod flags;
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

/// Reject vectors containing non-finite components (NaN, +Inf, -Inf) and,
/// optionally, components whose absolute value exceeds `magnitude_bound`.
///
/// This is gated by the per-vector-config opt-in flag `data_integrity_check`
/// (callers pass `data_integrity_check = false` to disable the finiteness
/// check entirely, e.g. while preserving back-compat). A `None`
/// `magnitude_bound` — or any non-finite or non-positive bound — disables
/// the magnitude check independently. The bound is normalised on entry so
/// that pathological values (`NaN`, `-5.0`, `INFINITY`) cannot produce
/// silent always-pass / always-reject behaviour.
///
/// The check is single-pass and branch-light. Caller decides whether to
/// invoke it; this function only implements the policy.
fn check_vector_values(
    vector: &[f32],
    data_integrity_check: bool,
    magnitude_bound: Option<f32>,
) -> OperationResult<()> {
    // Normalise the bound: anything non-finite or non-positive is treated as
    // "no bound set". This collapses three otherwise-silent footguns into a
    // single safe behaviour:
    //
    //   Some(NaN)      -> v.abs() > NaN is always false -> nothing rejected
    //   Some(-5.0)     -> v.abs() > -5.0 is always true -> everything rejected
    //   Some(Infinity) -> v.abs() > Inf is always false -> same as no bound
    let magnitude_bound = magnitude_bound.filter(|&b| b.is_finite() && b > 0.0);

    if !data_integrity_check && magnitude_bound.is_none() {
        return Ok(());
    }
    for (i, &v) in vector.iter().enumerate() {
        if data_integrity_check {
            if v.is_nan() {
                return Err(OperationError::InvalidVectorValue {
                    reason: format!("NaN at index {i}"),
                });
            }
            if v.is_infinite() {
                let sign = if v.is_sign_positive() { "+" } else { "-" };
                return Err(OperationError::InvalidVectorValue {
                    reason: format!("{sign}Inf at index {i}"),
                });
            }
        }
        if let Some(bound) = magnitude_bound {
            if v.abs() > bound {
                return Err(OperationError::InvalidVectorValue {
                    reason: format!(
                        "|v[{i}]|={abs} exceeds magnitude_bound={bound}",
                        abs = v.abs(),
                    ),
                });
            }
        }
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
            check_vector_values(
                vector,
                vector_config.data_integrity_check,
                vector_config.magnitude_bound,
            )?;
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
                check_vector_values(
                    vector,
                    vector_config.data_integrity_check,
                    vector_config.magnitude_bound,
                )?;
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
            check_vector_values(
                &vector.values,
                vector_config.data_integrity_check,
                vector_config.magnitude_bound,
            )?;
            Ok(())
        }
        VectorRef::MultiDense(_) => Err(OperationError::WrongMulti),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_vector_values_disabled_is_no_op() {
        // With both flags off, any vector passes, including NaN / Inf / huge.
        let nan_vec = [f32::NAN, 1.0, 2.0, 3.0];
        assert!(check_vector_values(&nan_vec, false, None).is_ok());

        let inf_vec = [1.0, f32::INFINITY, 2.0, 3.0];
        assert!(check_vector_values(&inf_vec, false, None).is_ok());

        let huge_vec = [1e30, 1e30, 1e30, 1e30];
        assert!(check_vector_values(&huge_vec, false, None).is_ok());
    }

    #[test]
    fn data_integrity_check_rejects_nan() {
        let nan_first = [f32::NAN, 1.0, 2.0];
        let err = check_vector_values(&nan_first, true, None).unwrap_err();
        match err {
            OperationError::InvalidVectorValue { reason } => {
                assert!(reason.contains("NaN at index 0"), "got: {reason}");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }

        let nan_mid = [1.0, 2.0, f32::NAN, 4.0];
        let err = check_vector_values(&nan_mid, true, None).unwrap_err();
        assert!(matches!(
            err,
            OperationError::InvalidVectorValue { ref reason } if reason.contains("NaN at index 2")
        ));
    }

    #[test]
    fn data_integrity_check_rejects_pos_inf_and_neg_inf() {
        let pos = [1.0, f32::INFINITY, 3.0];
        let err = check_vector_values(&pos, true, None).unwrap_err();
        assert!(matches!(
            err,
            OperationError::InvalidVectorValue { ref reason } if reason.contains("Inf at index 1")
        ));

        let neg = [1.0, 2.0, f32::NEG_INFINITY];
        let err = check_vector_values(&neg, true, None).unwrap_err();
        assert!(matches!(
            err,
            OperationError::InvalidVectorValue { ref reason } if reason.contains("Inf at index 2")
        ));
    }

    #[test]
    fn data_integrity_check_accepts_clean_vector() {
        let clean = [0.1f32, -0.2, 0.3, -0.4];
        assert!(check_vector_values(&clean, true, None).is_ok());
    }

    #[test]
    fn magnitude_bound_rejects_above_bound() {
        let huge = [1.0, 2.0, 1e30, 4.0];
        let err = check_vector_values(&huge, false, Some(10.0)).unwrap_err();
        match err {
            OperationError::InvalidVectorValue { reason } => {
                assert!(reason.contains("|v[2]|"), "got: {reason}");
                assert!(reason.contains("magnitude_bound=10"), "got: {reason}");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }

        // Negative magnitudes count via abs().
        let huge_neg = [1.0, 2.0, -1e30, 4.0];
        assert!(check_vector_values(&huge_neg, false, Some(10.0)).is_err());
    }

    #[test]
    fn magnitude_bound_accepts_at_or_below_bound() {
        // Component equal to the bound is accepted (strict greater-than rule).
        let edge = [10.0f32, -10.0, 5.0, -5.0];
        assert!(check_vector_values(&edge, false, Some(10.0)).is_ok());
    }

    #[test]
    fn flags_independent_finite_passes_magnitude_check_alone() {
        // data_integrity_check off, magnitude_bound on: only magnitudes are
        // checked. A NaN slips through (correctly — it's not a magnitude issue
        // and the caller did not ask for the finiteness check).
        let nan = [f32::NAN, 1.0, 2.0];
        assert!(check_vector_values(&nan, false, Some(100.0)).is_ok());

        // Conversely, data_integrity_check on without magnitude_bound rejects
        // NaN/Inf but accepts a finite huge value.
        let huge = [1e30, 1e30, 1e30];
        assert!(check_vector_values(&huge, true, None).is_ok());
    }

    #[test]
    fn pathological_magnitude_bound_is_normalised_to_disabled() {
        // Three pathological bound values that would silently misbehave if
        // passed through to `v.abs() > bound`. The normalisation must treat
        // each as "no bound set".
        let huge = [1e30f32, 1e30, 1e30, 1e30];

        // Some(NaN): without normalisation, `v.abs() > NaN` is always false,
        // so nothing would be rejected — a silent always-pass.
        assert!(check_vector_values(&huge, false, Some(f32::NAN)).is_ok());

        // Some(negative): without normalisation, `v.abs() > -5.0` is always
        // true, so every vector would be rejected — a silent always-reject.
        // We assert the *clean* case is accepted, which it would not be if
        // the bound were applied as-is.
        let clean = [0.1f32, 0.2, 0.3];
        assert!(check_vector_values(&clean, false, Some(-5.0)).is_ok());

        // Some(Infinity): without normalisation, `v.abs() > Inf` is always
        // false (Inf > Inf is false), so it would behave like no bound. The
        // normalisation surfaces this by collapsing it to None explicitly.
        assert!(check_vector_values(&huge, false, Some(f32::INFINITY)).is_ok());
    }

    #[test]
    fn inf_error_message_distinguishes_positive_and_negative() {
        let pos = [1.0, f32::INFINITY, 3.0];
        let err = check_vector_values(&pos, true, None).unwrap_err();
        assert!(matches!(
            err,
            OperationError::InvalidVectorValue { ref reason } if reason.contains("+Inf at index 1")
        ));

        let neg = [1.0, 2.0, f32::NEG_INFINITY];
        let err = check_vector_values(&neg, true, None).unwrap_err();
        assert!(matches!(
            err,
            OperationError::InvalidVectorValue { ref reason } if reason.contains("-Inf at index 2")
        ));
    }
}
