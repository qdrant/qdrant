use common::types::ScoreType;

use crate::data_types::vectors::{DenseVector, VectorElementTypeByte};
use crate::spaces::metric::Metric;
#[cfg(target_arch = "x86_64")]
use crate::spaces::metric_uint::avx2::manhattan::avx_manhattan_similarity_bytes;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use crate::spaces::metric_uint::neon::manhattan::neon_manhattan_similarity_bytes;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use crate::spaces::metric_uint::sse2::manhattan::sse_manhattan_similarity_bytes;
#[cfg(target_arch = "x86_64")]
use crate::spaces::simple::MIN_DIM_SIZE_AVX;
use crate::spaces::simple::{MIN_DIM_SIZE_SIMD, ManhattanMetric};
use crate::types::Distance;

impl Metric<VectorElementTypeByte> for ManhattanMetric {
    fn distance() -> Distance {
        Distance::Manhattan
    }

    fn similarity(v1: &[VectorElementTypeByte], v2: &[VectorElementTypeByte]) -> ScoreType {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("avx2")
                && is_x86_feature_detected!("fma")
                && v1.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { avx_manhattan_similarity_bytes(v1, v2) };
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("sse")
                && is_x86_feature_detected!("sse2")
                && v1.len() >= MIN_DIM_SIZE_SIMD
            {
                return unsafe { sse_manhattan_similarity_bytes(v1, v2) };
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { neon_manhattan_similarity_bytes(v1, v2) };
            }
        }

        manhattan_similarity_bytes(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        vector
    }
}

pub fn manhattan_similarity_bytes(
    v1: &[VectorElementTypeByte],
    v2: &[VectorElementTypeByte],
) -> ScoreType {
    -v1.iter()
        .zip(v2)
        .map(|(a, b)| (i32::from(*a) - i32::from(*b)).abs())
        .sum::<i32>() as ScoreType
}
