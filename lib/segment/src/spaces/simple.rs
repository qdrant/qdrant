use common::types::ScoreType;

use super::metric::{Metric, MetricPostProcessing};
#[cfg(target_arch = "x86_64")]
use super::simple_avx::*;
#[cfg(target_arch = "x86_64")]
use super::simple_avx512::*;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use super::simple_neon::*;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use super::simple_sse::*;
use super::tools::is_length_zero_or_normalized;
use crate::data_types::vectors::{DenseVector, VectorElementType};
use crate::types::Distance;

#[cfg(target_arch = "x86_64")]
pub(crate) const MIN_DIM_SIZE_AVX512: usize = 64;

#[cfg(target_arch = "x86_64")]
pub(crate) const MIN_DIM_SIZE_AVX: usize = 32;

#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    all(target_arch = "aarch64", target_feature = "neon")
))]
pub(crate) const MIN_DIM_SIZE_SIMD: usize = 16;

/// Best f32 distance backend available on this CPU.
///
/// Resolved once via [`f32_simd_backend`] so hot scoring loops (HNSW construction
/// neighbor search and heuristic `score_internal`) do not re-run feature detection
/// on every vector. Preference order: AVX-512F → AVX+FMA → SSE → scalar
/// (NEON is selected separately on aarch64).
#[cfg(target_arch = "x86_64")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum F32SimdBackend {
    Avx512,
    Avx,
    Sse,
    Scalar,
}

#[cfg(target_arch = "x86_64")]
fn f32_simd_backend() -> F32SimdBackend {
    use std::sync::OnceLock;

    static BACKEND: OnceLock<F32SimdBackend> = OnceLock::new();
    *BACKEND.get_or_init(|| {
        if is_x86_feature_detected!("avx512f") {
            F32SimdBackend::Avx512
        } else if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
            F32SimdBackend::Avx
        } else if is_x86_feature_detected!("sse") {
            F32SimdBackend::Sse
        } else {
            F32SimdBackend::Scalar
        }
    })
}

#[derive(Clone)]
pub struct DotProductMetric;

#[derive(Clone)]
pub struct CosineMetric;

#[derive(Clone)]
pub struct EuclidMetric;

#[derive(Clone)]
pub struct ManhattanMetric;

impl Metric<VectorElementType> for EuclidMetric {
    fn distance() -> Distance {
        Distance::Euclid
    }

    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        #[cfg(target_arch = "x86_64")]
        {
            match f32_simd_backend() {
                F32SimdBackend::Avx512 if v1.len() >= MIN_DIM_SIZE_AVX512 => {
                    return unsafe { euclid_similarity_avx512(v1, v2) };
                }
                F32SimdBackend::Avx512 | F32SimdBackend::Avx if v1.len() >= MIN_DIM_SIZE_AVX => {
                    return unsafe { euclid_similarity_avx(v1, v2) };
                }
                F32SimdBackend::Avx512 | F32SimdBackend::Avx | F32SimdBackend::Sse
                    if v1.len() >= MIN_DIM_SIZE_SIMD =>
                {
                    return unsafe { euclid_similarity_sse(v1, v2) };
                }
                _ => {}
            }
        }

        #[cfg(all(target_arch = "x86", not(target_arch = "x86_64")))]
        {
            if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { euclid_similarity_sse(v1, v2) };
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { euclid_similarity_neon(v1, v2) };
            }
        }

        euclid_similarity(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        vector
    }
}

impl MetricPostProcessing for EuclidMetric {
    fn postprocess(score: ScoreType) -> ScoreType {
        score.abs().sqrt()
    }
}

impl Metric<VectorElementType> for ManhattanMetric {
    fn distance() -> Distance {
        Distance::Manhattan
    }

    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        #[cfg(target_arch = "x86_64")]
        {
            match f32_simd_backend() {
                F32SimdBackend::Avx512 if v1.len() >= MIN_DIM_SIZE_AVX512 => {
                    return unsafe { manhattan_similarity_avx512(v1, v2) };
                }
                F32SimdBackend::Avx512 | F32SimdBackend::Avx if v1.len() >= MIN_DIM_SIZE_AVX => {
                    return unsafe { manhattan_similarity_avx(v1, v2) };
                }
                F32SimdBackend::Avx512 | F32SimdBackend::Avx | F32SimdBackend::Sse
                    if v1.len() >= MIN_DIM_SIZE_SIMD =>
                {
                    return unsafe { manhattan_similarity_sse(v1, v2) };
                }
                _ => {}
            }
        }

        #[cfg(all(target_arch = "x86", not(target_arch = "x86_64")))]
        {
            if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { manhattan_similarity_sse(v1, v2) };
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { manhattan_similarity_neon(v1, v2) };
            }
        }

        manhattan_similarity(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        vector
    }
}

impl MetricPostProcessing for ManhattanMetric {
    fn postprocess(score: ScoreType) -> ScoreType {
        score.abs()
    }
}

impl Metric<VectorElementType> for DotProductMetric {
    fn distance() -> Distance {
        Distance::Dot
    }

    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        #[cfg(target_arch = "x86_64")]
        {
            match f32_simd_backend() {
                F32SimdBackend::Avx512 if v1.len() >= MIN_DIM_SIZE_AVX512 => {
                    return unsafe { dot_similarity_avx512(v1, v2) };
                }
                F32SimdBackend::Avx512 | F32SimdBackend::Avx if v1.len() >= MIN_DIM_SIZE_AVX => {
                    return unsafe { dot_similarity_avx(v1, v2) };
                }
                F32SimdBackend::Avx512 | F32SimdBackend::Avx | F32SimdBackend::Sse
                    if v1.len() >= MIN_DIM_SIZE_SIMD =>
                {
                    return unsafe { dot_similarity_sse(v1, v2) };
                }
                _ => {}
            }
        }

        #[cfg(all(target_arch = "x86", not(target_arch = "x86_64")))]
        {
            if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { dot_similarity_sse(v1, v2) };
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { dot_similarity_neon(v1, v2) };
            }
        }

        dot_similarity(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        vector
    }
}

impl MetricPostProcessing for DotProductMetric {
    fn postprocess(score: ScoreType) -> ScoreType {
        score
    }
}

/// Equivalent to DotProductMetric with normalization of the vectors in preprocessing.
impl Metric<VectorElementType> for CosineMetric {
    fn distance() -> Distance {
        Distance::Cosine
    }

    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        DotProductMetric::similarity(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        #[cfg(target_arch = "x86_64")]
        {
            match f32_simd_backend() {
                F32SimdBackend::Avx512 if vector.len() >= MIN_DIM_SIZE_AVX512 => {
                    return unsafe { cosine_preprocess_avx512(vector) };
                }
                F32SimdBackend::Avx512 | F32SimdBackend::Avx
                    if vector.len() >= MIN_DIM_SIZE_AVX =>
                {
                    return unsafe { cosine_preprocess_avx(vector) };
                }
                F32SimdBackend::Avx512 | F32SimdBackend::Avx | F32SimdBackend::Sse
                    if vector.len() >= MIN_DIM_SIZE_SIMD =>
                {
                    return unsafe { cosine_preprocess_sse(vector) };
                }
                _ => {}
            }
        }

        #[cfg(all(target_arch = "x86", not(target_arch = "x86_64")))]
        {
            if is_x86_feature_detected!("sse") && vector.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { cosine_preprocess_sse(vector) };
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") && vector.len() >= MIN_DIM_SIZE_SIMD
            {
                return unsafe { cosine_preprocess_neon(vector) };
            }
        }

        cosine_preprocess(vector)
    }
}

impl MetricPostProcessing for CosineMetric {
    fn postprocess(score: ScoreType) -> ScoreType {
        score
    }
}

pub fn euclid_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    -v1.iter()
        .zip(v2)
        .map(|(a, b)| (a - b).powi(2))
        .sum::<ScoreType>()
}

pub fn manhattan_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    -v1.iter()
        .zip(v2)
        .map(|(a, b)| (a - b).abs())
        .sum::<ScoreType>()
}

pub fn cosine_preprocess(vector: DenseVector) -> DenseVector {
    let mut length: f32 = vector.iter().map(|x| x * x).sum();
    if is_length_zero_or_normalized(length) {
        return vector;
    }
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

pub fn dot_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    v1.iter().zip(v2).map(|(a, b)| a * b).sum()
}

#[cfg(test)]
mod tests {
    use rand::RngExt;

    use super::*;

    #[test]
    fn test_cosine_preprocessing() {
        let res = <CosineMetric as Metric<VectorElementType>>::preprocess(vec![0.0, 0.0, 0.0, 0.0]);
        assert_eq!(res, vec![0.0, 0.0, 0.0, 0.0]);
    }

    /// If we preprocess a vector multiple times, we expect the same result.
    /// Renormalization should not produce something different.
    #[test]
    fn test_cosine_stable_preprocessing() {
        const DIM: usize = 1500;
        const ATTEMPTS: usize = 100;

        let mut rng = rand::rng();

        for attempt in 0..ATTEMPTS {
            let range = rng.random_range(-2.5..=0.0)..=rng.random_range(0.0..2.5);
            let vector: Vec<_> = (0..DIM).map(|_| rng.random_range(range.clone())).collect();

            // Preprocess and re-preprocess
            let preprocess1 = <CosineMetric as Metric<VectorElementType>>::preprocess(vector);
            let preprocess2: DenseVector =
                <CosineMetric as Metric<VectorElementType>>::preprocess(preprocess1.clone());

            // All following preprocess attempts must be the same
            assert_eq!(
                preprocess1, preprocess2,
                "renormalization is not stable (vector #{attempt})"
            );
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_dispatched_metrics_match_scalar() {
        let v1: Vec<f32> = (0..96).map(|i| i as f32 * 0.1).collect();
        let v2: Vec<f32> = (0..96).map(|i| (i as f32 * 0.07) + 1.0).collect();

        let tol = 1e-4_f32;
        assert!((DotProductMetric::similarity(&v1, &v2) - dot_similarity(&v1, &v2)).abs() < tol);
        assert!((EuclidMetric::similarity(&v1, &v2) - euclid_similarity(&v1, &v2)).abs() < tol);
        assert!(
            (ManhattanMetric::similarity(&v1, &v2) - manhattan_similarity(&v1, &v2)).abs() < tol
        );
    }
}
