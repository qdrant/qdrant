use common::types::ScoreType;
use half::f16;

use crate::data_types::vectors::{DenseVector, VectorElementTypeHalf};
use crate::spaces::metric::Metric;
#[cfg(target_arch = "x86_64")]
use crate::spaces::metric_f16::avx::dot::avx_dot_similarity_half;
#[cfg(all(target_arch = "aarch64", target_feature = "neon", not(windows)))]
use crate::spaces::metric_f16::neon::dot::neon_dot_similarity_half;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use crate::spaces::metric_f16::sse::dot::sse_dot_similarity_half;
#[cfg(target_arch = "x86_64")]
use crate::spaces::simple::MIN_DIM_SIZE_AVX;
use crate::spaces::simple::{DotProductMetric, MIN_DIM_SIZE_SIMD};
use crate::types::Distance;

impl Metric<VectorElementTypeHalf> for DotProductMetric {
    fn distance() -> Distance {
        Distance::Dot
    }

    fn similarity(v1: &[VectorElementTypeHalf], v2: &[VectorElementTypeHalf]) -> ScoreType {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("fma")
                && is_x86_feature_detected!("f16c")
                && v1.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { avx_dot_similarity_half(v1, v2) };
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { sse_dot_similarity_half(v1, v2) };
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon", not(windows)))]
        {
            if std::arch::is_aarch64_feature_detected!("neon")
                && std::arch::is_aarch64_feature_detected!("fp16")
                && v1.len() >= MIN_DIM_SIZE_SIMD
            {
                return unsafe { neon_dot_similarity_half(v1, v2) };
            }
        }

        dot_similarity_half(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        vector
    }
}

pub fn dot_similarity_half(
    v1: &[VectorElementTypeHalf],
    v2: &[VectorElementTypeHalf],
) -> ScoreType {
    v1.iter()
        .zip(v2)
        .map(|(a, b)| f16::to_f32(*a) * f16::to_f32(*b))
        .sum::<f32>()
}

#[cfg(test)]
mod tests {
    use half::f16;
    use rand::RngExt;

    use super::*;
    use crate::spaces::metric::Metric;
    #[cfg(target_arch = "x86_64")]
    use crate::spaces::simple::MIN_DIM_SIZE_AVX;
    use crate::spaces::simple::MIN_DIM_SIZE_SIMD;

    #[test]
    fn test_property_zero_padding_dot_f16() {
        let mut thresholds = vec![
            MIN_DIM_SIZE_SIMD - 1,
            MIN_DIM_SIZE_SIMD,
            MIN_DIM_SIZE_SIMD + 1,
        ];
        #[cfg(target_arch = "x86_64")]
        {
            thresholds.extend([MIN_DIM_SIZE_AVX - 1, MIN_DIM_SIZE_AVX, MIN_DIM_SIZE_AVX + 1]);
        }

        let mut rng = rand::rng();
        for &dim in &thresholds {
            let q: Vec<f16> = (0..dim)
                .map(|_| f16::from_f32(rng.random_range(-2.0..2.0)))
                .collect();
            let p1: Vec<f16> = (0..dim)
                .map(|_| f16::from_f32(rng.random_range(-2.0..2.0)))
                .collect();
            let p2: Vec<f16> = (0..dim)
                .map(|_| f16::from_f32(rng.random_range(-2.0..2.0)))
                .collect();

            let score1_orig = DotProductMetric::similarity(&q, &p1);
            let score2_orig = DotProductMetric::similarity(&q, &p2);

            let mut q_pad = q.clone();
            q_pad.extend([f16::ZERO; 3]);
            let mut p1_pad = p1.clone();
            p1_pad.extend([f16::ZERO; 3]);
            let mut p2_pad = p2.clone();
            p2_pad.extend([f16::ZERO; 3]);

            let score1_pad = DotProductMetric::similarity(&q_pad, &p1_pad);
            let score2_pad = DotProductMetric::similarity(&q_pad, &p2_pad);

            // FMA (AVX) vs non-FMA (SSE/Scalar) causes slight rounding differences
            assert!(
                (score1_orig - score1_pad).abs() < 1e-4,
                "dim {} mismatch: {} != {}",
                dim,
                score1_orig,
                score1_pad
            );
            assert!(
                (score2_orig - score2_pad).abs() < 1e-4,
                "dim {} mismatch: {} != {}",
                dim,
                score2_orig,
                score2_pad
            );
            // Only assert ordering if difference is large enough that FMA rounding doesn't flip it
            if (score1_orig - score2_orig).abs() > 1e-4 {
                assert_eq!(
                    score1_orig > score2_orig,
                    score1_pad > score2_pad,
                    "dim {}",
                    dim
                );
            }
        }
    }
}
