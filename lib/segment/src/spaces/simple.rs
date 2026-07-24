use common::types::ScoreType;

use super::metric::{Metric, MetricPostProcessing};
#[cfg(target_arch = "x86_64")]
use super::simple_avx::*;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use super::simple_neon::*;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use super::simple_sse::*;
use super::tools::is_length_zero_or_normalized;
use crate::data_types::vectors::{DenseVector, VectorElementType};
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

#[inline]
fn trim_common_trailing_zeros<'a>(
    v1: &'a [VectorElementType],
    v2: &'a [VectorElementType],
) -> (&'a [VectorElementType], &'a [VectorElementType]) {
    debug_assert_eq!(v1.len(), v2.len());

    let mut len = v1.len();
    while len > 0 && v1[len - 1] == 0.0 && v2[len - 1] == 0.0 {
        len -= 1;
    }

    (&v1[..len], &v2[..len])
}

impl Metric<VectorElementType> for EuclidMetric {
    fn distance() -> Distance {
        Distance::Euclid
    }

    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        let (v1, v2) = trim_common_trailing_zeros(v1, v2);

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

    #[test]
    fn test_euclid_zero_padding_does_not_change_order_across_simd_boundary() {
        let query = vec![0.0; 31];
        let point_1 = vec![
            -7379.2085,
            78561.6797,
            351176.656,
            -179895.969,
            -37065.5938,
            266713.188,
            403120.125,
            -2049.87158,
            -419890.938,
            35755.168,
            -313292.0,
            320771.656,
            53339.3438,
            -349924.688,
            239633.781,
            310356.625,
            -279830.062,
            276455.562,
            117963.992,
            -201381.141,
            -99853.5312,
            222634.391,
            -56914.293,
            -111477.445,
            102780.82,
            -25055.0332,
            -231102.703,
            -80738.5391,
            75535.4922,
            -319243.594,
            184434.984,
        ];
        let point_2 = vec![
            -7335.02246,
            78627.1797,
            351145.281,
            -179854.953,
            -37026.6406,
            266792.688,
            403092.594,
            -2089.85986,
            -419901.125,
            35719.1875,
            -313275.938,
            320861.75,
            53338.5195,
            -350038.656,
            239602.297,
            310319.875,
            -279785.062,
            276427.0,
            117995.641,
            -201402.891,
            -99835.2422,
            222626.906,
            -56885.3555,
            -111412.375,
            102726.219,
            -25111.8809,
            -231136.031,
            -80752.4141,
            75534.0859,
            -319195.75,
            184394.719,
        ];

        let base_1 = <EuclidMetric as Metric<VectorElementType>>::similarity(&query, &point_1);
        let base_2 = <EuclidMetric as Metric<VectorElementType>>::similarity(&query, &point_2);

        let padded_query = pad_zero(query);
        let padded_point_1 = pad_zero(point_1);
        let padded_point_2 = pad_zero(point_2);

        let padded_1 =
            <EuclidMetric as Metric<VectorElementType>>::similarity(&padded_query, &padded_point_1);
        let padded_2 =
            <EuclidMetric as Metric<VectorElementType>>::similarity(&padded_query, &padded_point_2);

        assert_eq!(base_1, padded_1);
        assert_eq!(base_2, padded_2);
        assert!(
            base_1 > base_2,
            "point 1 should be closer before zero padding: {base_1} <= {base_2}"
        );
        assert!(
            padded_1 > padded_2,
            "point 1 should remain closer after zero padding: {padded_1} <= {padded_2}"
        );
    }

    fn pad_zero(mut vector: DenseVector) -> DenseVector {
        vector.push(0.0);
        vector
    }
}
