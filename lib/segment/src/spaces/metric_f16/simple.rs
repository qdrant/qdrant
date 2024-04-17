use common::types::ScoreType;
use half::f16;
use half::vec::HalfFloatVecExt;

use crate::data_types::vectors::{DenseVector, TypedDenseVector};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::spaces::tools::is_length_zero_or_normalized;
use crate::types::Distance;

impl Metric<f16> for EuclidMetric {
    fn distance() -> Distance {
        Distance::Euclid
    }

    fn similarity(v1: &[f16], v2: &[f16]) -> ScoreType {
        /*
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
        */
        euclid_similarity(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> TypedDenseVector<f16> {
        TypedDenseVector::from_f32_slice(&vector)
    }
}

impl Metric<f16> for ManhattanMetric {
    fn distance() -> Distance {
        Distance::Manhattan
    }

    fn similarity(v1: &[f16], v2: &[f16]) -> ScoreType {
        /*
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
        */
        manhattan_similarity(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> TypedDenseVector<f16> {
        TypedDenseVector::from_f32_slice(&vector)
    }
}

impl Metric<f16> for DotProductMetric {
    fn distance() -> Distance {
        Distance::Dot
    }

    fn similarity(v1: &[f16], v2: &[f16]) -> ScoreType {
        /*
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
        */
        dot_similarity(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> TypedDenseVector<f16> {
        TypedDenseVector::from_f32_slice(&vector)
    }
}

impl Metric<f16> for CosineMetric {
    fn distance() -> Distance {
        Distance::Cosine
    }

    fn similarity(v1: &[f16], v2: &[f16]) -> ScoreType {
        DotProductMetric::similarity(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> TypedDenseVector<f16> {
        /*
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("fma")
                && vector.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { cosine_preprocess_avx(vector) };
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
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
        */
        cosine_preprocess(vector)
    }
}

fn euclid_similarity(v1: &[f16], v2: &[f16]) -> ScoreType {
    -v1.iter()
        .zip(v2)
        .map(|(a, b)| (a.to_f32() - b.to_f32()).powi(2))
        .sum::<ScoreType>()
}

fn manhattan_similarity(v1: &[f16], v2: &[f16]) -> ScoreType {
    -v1.iter()
        .zip(v2)
        .map(|(a, b)| (a.to_f32() - b.to_f32()).abs())
        .sum::<ScoreType>()
}

fn cosine_preprocess(vector: DenseVector) -> TypedDenseVector<f16> {
    let mut length: f32 = vector.iter().map(|x| x * x).sum();
    if is_length_zero_or_normalized(length) {
        return TypedDenseVector::from_f32_slice(&vector);
    }
    length = length.sqrt();
    vector.iter().map(|x| f16::from_f32(x / length)).collect()
}

fn dot_similarity(v1: &[f16], v2: &[f16]) -> ScoreType {
    v1.iter()
        .zip(v2)
        .map(|(a, b)| a.to_f32() * b.to_f32())
        .sum()
}
