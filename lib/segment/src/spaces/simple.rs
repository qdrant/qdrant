use super::metric::Metric;
#[cfg(target_arch = "x86_64")]
use super::simple_avx::*;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use super::simple_neon::*;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use super::simple_sse::*;
use crate::data_types::vectors::VectorElementType;
use crate::types::{Distance, ScoreType};

#[cfg(target_arch = "x86_64")]
const MIN_DIM_SIZE_AVX: usize = 32;

#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    all(target_arch = "aarch64", target_feature = "neon")
))]
const MIN_DIM_SIZE_SIMD: usize = 16;

#[derive(Clone)]
pub struct DotProductMetric {}

#[derive(Clone)]
pub struct CosineMetric {}

#[derive(Clone)]
pub struct EuclidMetric {}

#[derive(Clone)]
pub struct JaccardMetric {}

impl Metric for EuclidMetric {
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

    fn preprocess(_vector: &[VectorElementType]) -> Option<Vec<VectorElementType>> {
        None
    }

    fn postprocess(score: ScoreType) -> ScoreType {
        score.abs().sqrt()
    }
}

impl Metric for DotProductMetric {
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

    fn preprocess(_vector: &[VectorElementType]) -> Option<Vec<VectorElementType>> {
        None
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

    fn preprocess(vector: &[VectorElementType]) -> Option<Vec<VectorElementType>> {
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

    fn postprocess(score: ScoreType) -> ScoreType {
        score
    }
}

impl Metric for JaccardMetric {
    fn distance() -> Distance {
        Distance::Jaccard
    }

    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        jaccard_similarity(v1, v2)
    }

    fn preprocess(_vector: &[VectorElementType]) -> Option<Vec<VectorElementType>> {
        None
    }

    fn postprocess(score: ScoreType) -> ScoreType {
        score
    }
}

pub fn euclid_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    let s: ScoreType = v1
        .iter()
        .copied()
        .zip(v2.iter().copied())
        .map(|(a, b)| (a - b).powi(2))
        .sum();
    -s
}

pub fn cosine_preprocess(vector: &[VectorElementType]) -> Option<Vec<VectorElementType>> {
    let mut length: f32 = vector.iter().map(|x| x * x).sum();
    if length < f32::EPSILON {
        return None;
    }
    length = length.sqrt();
    Some(vector.iter().map(|x| x / length).collect())
}

pub fn dot_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    v1.iter().zip(v2).map(|(a, b)| a * b).sum()
}

pub fn jaccard_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    assert_eq!(v1.len(), v2.len());
    assert!(v1.iter().all(|a| a > &0.0));
    assert!(v2.iter().all(|a| a > &0.0));
    if v1.is_empty() && v2.is_empty() {
        return 1.0;
    }
    let mut min_sum: f32 = 0.0;
    let mut max_sum: f32 = 0.0;
    for (xi, yi) in v1.iter().zip(v2.iter()) {
        min_sum += xi.min(*yi);
        max_sum += xi.max(*yi);
    }
    min_sum / max_sum
}

#[cfg(test)]
mod tests {
    use num_traits::abs_sub;

    use super::*;

    #[test]
    fn test_cosine_preprocessing() {
        let res = CosineMetric::preprocess(&[0.0, 0.0, 0.0, 0.0]);
        assert!(res.is_none());
    }

    #[test]
    fn test_jaccard_similarity() {
        assert_eq!(1.0, jaccard_similarity(&[], &[]));
        assert_eq!(1.0, jaccard_similarity(&[0.1], &[0.1]));
        assert_eq!(1.0, jaccard_similarity(&[0.1, 0.2, 0.3], &[0.1, 0.2, 0.3]));

        // min_sum = (min(0.2, 0.3) + min(0.2, 0.2) + min(0.2, 0.1))
        // max_sum = (max(0.2, 0.3) + max(0.2, 0.2) + max(0.2, 0.1))
        // similarity = 0.5 / 0.7 = 0.7142
        let expected: f32 = 0.5 / 0.7;
        let result: f32 = jaccard_similarity(&[0.2, 0.2, 0.2], &[0.3, 0.2, 0.1]);
        assert!(abs_sub(expected, result) < f32::EPSILON);
    }
}
