use common::types::ScoreType;
use half::f16;

use crate::data_types::vectors::{DenseVector, VectorElementTypeHalf};
use crate::spaces::metric::Metric;
#[cfg(target_arch = "x86_64")]
use crate::spaces::metric_f16::avx::euclid::avx_euclid_similarity_half;
#[cfg(all(target_arch = "aarch64", target_feature = "neon", not(windows)))]
use crate::spaces::metric_f16::neon::euclid::neon_euclid_similarity_half;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use crate::spaces::metric_f16::sse::euclid::sse_euclid_similarity_half;
#[cfg(target_arch = "x86_64")]
use crate::spaces::simple::MIN_DIM_SIZE_AVX;
use crate::spaces::simple::{EuclidMetric, MIN_DIM_SIZE_SIMD};
use crate::types::Distance;

impl Metric<VectorElementTypeHalf> for EuclidMetric {
    fn distance() -> Distance {
        Distance::Euclid
    }

    fn similarity(v1: &[VectorElementTypeHalf], v2: &[VectorElementTypeHalf]) -> ScoreType {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("fma")
                && is_x86_feature_detected!("f16c")
                && v1.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { avx_euclid_similarity_half(v1, v2) };
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { sse_euclid_similarity_half(v1, v2) };
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon", not(windows)))]
        {
            if std::arch::is_aarch64_feature_detected!("neon")
                && std::arch::is_aarch64_feature_detected!("fp16")
                && v1.len() >= MIN_DIM_SIZE_SIMD
            {
                return unsafe { neon_euclid_similarity_half(v1, v2) };
            }
        }

        euclid_similarity_half(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        vector
    }
}

pub fn euclid_similarity_half(
    v1: &[VectorElementTypeHalf],
    v2: &[VectorElementTypeHalf],
) -> ScoreType {
    -v1.iter()
        .zip(v2)
        .map(|(a, b)| (f16::to_f32(*a) - f16::to_f32(*b)).powi(2))
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
    fn test_zero_padding_euclid_f16() {
        let q_f32 = [
            -4.209245, -2.181154, 8.041031, -6.90443, -5.979087, -7.764691, 2.43967, -2.609285,
            7.315669, -1.894766, 8.510455, 1.632248, 4.300124, 3.52239, -2.642133,
        ];
        let p25_f32 = [
            -6.579045, 5.685214, -6.686334, 6.708689, -8.171157, -5.623098, 5.946339, -5.827834,
            7.591856, -0.720474, 9.05477, -2.384521, 6.732021, 0.727686, -3.877739,
        ];
        let p52_f32 = [
            -4.751741, 1.815279, 9.647952, -9.226715, -6.049182, 6.029042, 7.616709, -9.554953,
            9.285732, -6.945487, -2.528692, 8.803377, 2.039239, -1.263509, 1.162315,
        ];

        let to_f16 = |v: &[f32]| -> Vec<f16> { v.iter().copied().map(f16::from_f32).collect() };

        let q = to_f16(&q_f32);
        let p25 = to_f16(&p25_f32);
        let p52 = to_f16(&p52_f32);

        let score25_15d = EuclidMetric::similarity(&q, &p25);
        let score52_15d = EuclidMetric::similarity(&q, &p52);

        assert!(score52_15d > score25_15d);

        let mut q_18 = q.clone();
        q_18.extend([f16::ZERO; 3]);
        let mut p25_18 = p25.clone();
        p25_18.extend([f16::ZERO; 3]);
        let mut p52_18 = p52.clone();
        p52_18.extend([f16::ZERO; 3]);

        let score25_18d = EuclidMetric::similarity(&q_18, &p25_18);
        let score52_18d = EuclidMetric::similarity(&q_18, &p52_18);

        assert!(score52_18d > score25_18d);

        // Allow tiny epsilon difference for FMA vs SSE vs Scalar boundaries
        assert!(
            (score25_15d - score25_18d).abs() < 1e-4,
            "score25 mismatch: 15d={} vs 18d={}",
            score25_15d,
            score25_18d
        );
        assert!(
            (score52_15d - score52_18d).abs() < 1e-4,
            "score52 mismatch: 15d={} vs 18d={}",
            score52_15d,
            score52_18d
        );
    }

    #[test]
    fn test_property_zero_padding_euclid_f16() {
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

            let score1_orig = EuclidMetric::similarity(&q, &p1);
            let score2_orig = EuclidMetric::similarity(&q, &p2);

            let mut q_pad = q.clone();
            q_pad.extend([f16::ZERO; 3]);
            let mut p1_pad = p1.clone();
            p1_pad.extend([f16::ZERO; 3]);
            let mut p2_pad = p2.clone();
            p2_pad.extend([f16::ZERO; 3]);

            let score1_pad = EuclidMetric::similarity(&q_pad, &p1_pad);
            let score2_pad = EuclidMetric::similarity(&q_pad, &p2_pad);

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
