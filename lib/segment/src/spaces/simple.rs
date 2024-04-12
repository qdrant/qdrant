use common::types::ScoreType;

use super::metric::{Metric, MetricPostProcessing};
#[cfg(target_arch = "x86_64")]
use super::simple_avx::*;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use super::simple_neon::*;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use super::simple_sse::*;
use super::tools::is_length_zero_or_normalized;
use crate::data_types::vectors::{
    DenseVector, FromVectorElement, IntoVectorElement, VectorElementType,
};
use crate::types::Distance;

#[cfg(target_arch = "x86_64")]
const MIN_DIM_SIZE_AVX: usize = 32;

#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    all(target_arch = "aarch64", target_feature = "neon")
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

impl Metric<VectorElementType> for EuclidMetric {
    fn distance() -> Distance {
        Distance::Euclid
    }

    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
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
        .map(|(a, b)| (f32::from_vector_element(*a), f32::from_vector_element(*b)))
        .map(|(a, b)| (a - b).powi(2))
        .sum::<ScoreType>()
}

pub fn manhattan_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    -v1.iter()
        .zip(v2)
        .map(|(a, b)| (f32::from_vector_element(*a), f32::from_vector_element(*b)))
        .map(|(a, b)| (a - b).abs())
        .sum::<ScoreType>()
}

pub fn cosine_preprocess(vector: DenseVector) -> DenseVector {
    let mut length: f32 = vector
        .iter()
        .map(|x| f32::from_vector_element(*x))
        .map(|x| x * x)
        .sum();
    if is_length_zero_or_normalized(length) {
        return vector;
    }
    length = length.sqrt();
    vector
        .iter()
        .map(|x| f32::from_vector_element(*x))
        .map(|x| x / length)
        .map(|x| x.into_vector_element())
        .collect()
}

pub fn dot_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    v1.iter()
        .zip(v2)
        .map(|(a, b)| (f32::from_vector_element(*a), f32::from_vector_element(*b)))
        .map(|(a, b)| a * b)
        .sum()
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;
    use crate::data_types::vectors::IntoDenseVector;

    #[test]
    fn test_cosine_preprocessing() {
        let res: DenseVector = CosineMetric::preprocess([0.0, 0.0, 0.0, 0.0].into_dense_vector());
        assert_eq!(res, [0.0, 0.0, 0.0, 0.0].into_dense_vector());
    }

    /// If we preprocess a vector multiple times, we expect the same result.
    /// Renormalization should not produce something different.
    #[test]
    fn test_cosine_stable_preprocessing() {
        const DIM: usize = 1500;
        const ATTEMPTS: usize = 100;

        let mut rng = rand::thread_rng();

        for attempt in 0..ATTEMPTS {
            let range = rng.gen_range(-2.5..=0.0)..=rng.gen_range(0.0..2.5);
            let vector: Vec<_> = (0..DIM)
                .map(|_| rng.gen_range(range.clone()).into_vector_element())
                .collect();

            // Preprocess and re-preprocess
            let preprocess1 = CosineMetric::preprocess(vector);
            let preprocess2: DenseVector = CosineMetric::preprocess(preprocess1.clone());

            // All following preprocess attempts must be the same
            assert_eq!(
                preprocess1, preprocess2,
                "renormalization is not stable (vector #{attempt})"
            );
        }
    }
}
