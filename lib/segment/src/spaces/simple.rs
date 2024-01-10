use common::types::ScoreType;
#[cfg(not(feature = "use_f32"))]
use num_traits::Float as _;

use super::metric::Metric;
#[cfg(all(target_arch = "x86_64", feature = "use_f32"))]
use super::simple_avx::*;
#[cfg(all(target_arch = "aarch64", target_feature = "neon", feature = "use_f32"))]
use super::simple_neon::*;
#[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "use_f32"))]
use super::simple_sse::*;
use crate::data_types::vectors::{DenseVector, VectorElementType};
use crate::types::Distance;

#[cfg(all(target_arch = "x86_64", feature = "use_f32"))]
const MIN_DIM_SIZE_AVX: usize = 32;

#[cfg(all(
    any(
        target_arch = "x86",
        target_arch = "x86_64",
        all(target_arch = "aarch64", target_feature = "neon")
    ),
    feature = "use_f32",
))]
const MIN_DIM_SIZE_SIMD: usize = 16;

#[derive(Clone)]
pub struct DotProductMetric;

#[derive(Clone)]
pub struct CosineMetric;

#[derive(Clone)]
pub struct EuclidMetric;

#[derive(Clone)]
pub struct ManhattanMetric;

impl Metric for EuclidMetric {
    fn distance() -> Distance {
        Distance::Euclid
    }

    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        #[cfg(all(target_arch = "x86_64", feature = "use_f32"))]
        {
            if is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("fma")
                && v1.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { euclid_similarity_avx(v1, v2) };
            }
        }

        #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "use_f32"))]
        {
            if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { euclid_similarity_sse(v1, v2) };
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon", feature = "use_f32"))]
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

    fn postprocess(score: ScoreType) -> ScoreType {
        score.abs().sqrt()
    }
}

impl Metric for ManhattanMetric {
    fn distance() -> Distance {
        Distance::Manhattan
    }

    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        #[cfg(all(target_arch = "x86_64", feature = "use_f32"))]
        {
            if is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("fma")
                && v1.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { manhattan_similarity_avx(v1, v2) };
            }
        }

        #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "use_f32"))]
        {
            if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { manhattan_similarity_sse(v1, v2) };
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon", feature = "use_f32"))]
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

    fn postprocess(score: ScoreType) -> ScoreType {
        score.abs()
    }
}

impl Metric for DotProductMetric {
    fn distance() -> Distance {
        Distance::Dot
    }

    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        #[cfg(all(target_arch = "x86_64", feature = "use_f32"))]
        {
            if is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("fma")
                && v1.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { dot_similarity_avx(v1, v2) };
            }
        }

        #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "use_f32"))]
        {
            if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { dot_similarity_sse(v1, v2) };
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon", feature = "use_f32"))]
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

    fn postprocess(score: ScoreType) -> ScoreType {
        score
    }
}

impl Metric for CosineMetric {
    fn distance() -> Distance {
        Distance::Cosine
    }

    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        #[cfg(all(target_arch = "x86_64", feature = "use_f32"))]
        {
            if is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("fma")
                && v1.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { dot_similarity_avx(v1, v2) };
            }
        }

        #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "use_f32"))]
        {
            if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { dot_similarity_sse(v1, v2) };
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon", feature = "use_f32"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { dot_similarity_neon(v1, v2) };
            }
        }

        dot_similarity(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        #[cfg(all(target_arch = "x86_64", feature = "use_f32"))]
        {
            if is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("fma")
                && vector.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { cosine_preprocess_avx(vector) };
            }
        }

        #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "use_f32"))]
        {
            if is_x86_feature_detected!("sse") && vector.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { cosine_preprocess_sse(vector) };
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon", feature = "use_f32"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") && vector.len() >= MIN_DIM_SIZE_SIMD
            {
                return unsafe { cosine_preprocess_neon(vector) };
            }
        }

        cosine_preprocess(vector)
    }

    fn postprocess(score: ScoreType) -> ScoreType {
        score
    }
}

pub fn euclid_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    -v1.iter()
        .zip(v2)
        .map(|(a, b)| ScoreType::from((a - b).powi(2)))
        .sum::<ScoreType>()
}

pub fn manhattan_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    -v1.iter()
        .zip(v2)
        .map(|(a, b)| ScoreType::from((a - b).abs()))
        .sum::<ScoreType>()
}

pub fn cosine_preprocess(vector: DenseVector) -> DenseVector {
    let mut length: VectorElementType = vector.iter().map(|x| x * x).sum();
    if length < VectorElementType::EPSILON {
        return vector;
    }
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

pub fn dot_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    v1.iter().zip(v2).map(|(a, b)| ScoreType::from(a * b)).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::vector;

    #[test]
    fn test_cosine_preprocessing() {
        let res = CosineMetric::preprocess(vector![0.0, 0.0, 0.0, 0.0]);
        assert_eq!(res, vector![0.0, 0.0, 0.0, 0.0]);
    }
}
