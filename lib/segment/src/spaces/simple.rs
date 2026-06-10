use common::types::ScoreType;

use super::metric::{Metric, MetricPostProcessing};
#[cfg(target_arch = "x86_64")]
use super::simple_avx::*;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use super::simple_neon::*;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use super::simple_sse::*;
use super::tools::is_length_zero_or_normalized;
use crate::data_types::vectors::{DenseVector, TypedDenseVector, VectorElementType};
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

    fn query_similarity(
        query: &TypedDenseVector<VectorElementType>,
        vector: &[VectorElementType],
    ) -> ScoreType {
        Self::similarity(query, vector)
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

    fn query_similarity(
        query: &TypedDenseVector<VectorElementType>,
        vector: &[VectorElementType],
    ) -> ScoreType {
        Self::similarity(query, vector)
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

    fn query_similarity(
        query: &TypedDenseVector<VectorElementType>,
        vector: &[VectorElementType],
    ) -> ScoreType {
        Self::similarity(query, vector)
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

    fn query_similarity(
        query: &TypedDenseVector<VectorElementType>,
        vector: &[VectorElementType],
    ) -> ScoreType {
        Self::similarity(query, vector)
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
    v1.iter().zip(v2).map(|(a, b)| (a * b) as f64).sum::<f64>() as ScoreType
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

    /// Test that dot product is invariant under coordinate permutation.
    /// This is a fundamental mathematical property: dot(v1, v2) = dot(perm(v1), perm(v2))
    /// for any permutation of coordinates.
    ///
    /// We use values around 1e7 which are large enough to cause order-dependent
    /// rounding in f32 accumulation, but small enough that the f64 accumulation
    /// used in our implementation handles them exactly.
    #[test]
    fn test_dot_product_permutation_invariance() {
        // Values that cause catastrophic cancellation in f32 but not f64.
        // f32 has ~7 decimal digits of precision, so 1e7 + 1.0 loses the 1.0
        // in f32, but f64 has ~15 digits and handles this sum exactly.
        let query = vec![1e7, 1.0, -1e7];
        let vector = vec![1.0, 1.0, 1.0];
        let perm = [0, 2, 1]; // permutation indices

        let apply_perm = |v: &[f32]| -> Vec<f32> { perm.iter().map(|&i| v[i]).collect() };

        let base_score = dot_similarity(&query, &vector);
        let perm_score = dot_similarity(&apply_perm(&query), &apply_perm(&vector));

        // The scores should be identical (same dot product value)
        assert_eq!(
            base_score, perm_score,
            "Dot product must be invariant under coordinate permutation"
        );

        // Additional test with more dimensions and a different permutation
        let query2 = vec![1e7, -1e7, 1e7, -1e7];
        let vector2 = vec![1.0, 1.0, 1.0, 1.0];
        let perm2 = [3, 1, 0, 2];

        let apply_perm2 = |v: &[f32]| -> Vec<f32> { perm2.iter().map(|&i| v[i]).collect() };

        let base_score2 = dot_similarity(&query2, &vector2);
        let perm_score2 = dot_similarity(&apply_perm2(&query2), &apply_perm2(&vector2));

        assert_eq!(
            base_score2, perm_score2,
            "Dot product must be invariant under coordinate permutation (test 2)"
        );
    }
}
