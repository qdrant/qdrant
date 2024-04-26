use common::types::ScoreType;
use half::f16;
use num_traits::Float;

#[cfg(target_arch = "x86_64")]
use super::simple_avx::*;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use super::simple_neon::*;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use super::simple_sse::*;
use crate::data_types::vectors::{DenseVector, VectorElementTypeHalf};
use crate::spaces::metric::{Metric, MetricPostProcessing};
use crate::spaces::tools::is_length_zero_or_normalized;
use crate::types::Distance;

#[cfg(target_arch = "x86_64")]
pub(crate) const MIN_DIM_SIZE_AVX: usize = 32;

#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    all(target_arch = "aarch64", target_feature = "neon")
))]
pub(crate) const MIN_DIM_SIZE_SIMD: usize = 16;

#[derive(Clone)]
pub struct DotProductMetric;

#[derive(Clone)]
pub struct CosineMetric;

#[derive(Clone)]
pub struct EuclidMetric;

#[derive(Clone)]
pub struct ManhattanMetric;

impl Metric<VectorElementTypeHalf> for EuclidMetric {
    fn distance() -> Distance {
        Distance::Euclid
    }

    fn similarity(v1: &[VectorElementTypeHalf], v2: &[VectorElementTypeHalf]) -> ScoreType {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("fma")
                && v1.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { euclid_similarity_avx(v1, v2) };
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
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

        euclid_similarity_half(v1, v2)
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

impl Metric<VectorElementTypeHalf> for ManhattanMetric {
    fn distance() -> Distance {
        Distance::Manhattan
    }

    fn similarity(v1: &[VectorElementTypeHalf], v2: &[VectorElementTypeHalf]) -> ScoreType {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("fma")
                && v1.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { manhattan_similarity_avx(v1, v2) };
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
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

        manhattan_similarity_half(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        vector
    }
}

impl Metric<VectorElementTypeHalf> for DotProductMetric {
    fn distance() -> Distance {
        Distance::Dot
    }

    fn similarity(v1: &[VectorElementTypeHalf], v2: &[VectorElementTypeHalf]) -> ScoreType {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("fma")
                && v1.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { dot_similarity_avx(v1, v2) };
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
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

        dot_similarity_half(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        vector
    }
}

/// Equivalent to DotProductMetric with normalization of the vectors in preprocessing.
impl Metric<VectorElementTypeHalf> for CosineMetric {
    fn distance() -> Distance {
        Distance::Cosine
    }

    fn similarity(v1: &[VectorElementTypeHalf], v2: &[VectorElementTypeHalf]) -> ScoreType {
        DotProductMetric::similarity(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        cosine_preprocess(vector)
    }
}

pub fn euclid_similarity_half(
    v1: &[VectorElementTypeHalf],
    v2: &[VectorElementTypeHalf],
) -> ScoreType {
    f16::to_f32(-v1.iter().zip(v2).map(|(a, b)| (a - b).powi(2)).sum::<f16>())
}

pub fn manhattan_similarity_half(
    v1: &[VectorElementTypeHalf],
    v2: &[VectorElementTypeHalf],
) -> ScoreType {
    f16::to_f32(-v1.iter().zip(v2).map(|(a, b)| (a - b).abs()).sum::<f16>())
}

pub fn cosine_preprocess(vector: DenseVector) -> DenseVector {
    let mut length: f32 = vector.iter().map(|x| x * x).sum();
    if is_length_zero_or_normalized(length) {
        return vector;
    }
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

pub fn dot_similarity_half(
    v1: &[VectorElementTypeHalf],
    v2: &[VectorElementTypeHalf],
) -> ScoreType {
    f16::to_f32(v1.iter().zip(v2).map(|(a, b)| a * b).sum())
}
