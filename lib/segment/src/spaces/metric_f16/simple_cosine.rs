use common::types::ScoreType;

use super::simple_dot::dot_similarity_half;
use crate::data_types::vectors::{DenseVector, VectorElementTypeHalf};
use crate::spaces::metric::Metric;
#[cfg(target_arch = "x86_64")]
use crate::spaces::metric_f16::avx::dot::avx_dot_similarity_half;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use crate::spaces::metric_f16::neon::dot::neon_dot_similarity_half;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use crate::spaces::metric_f16::sse::dot::sse_dot_similarity_half;
#[cfg(target_arch = "x86_64")]
use crate::spaces::simple::MIN_DIM_SIZE_AVX;
use crate::spaces::simple::{cosine_preprocess, CosineMetric, MIN_DIM_SIZE_SIMD};
#[cfg(target_arch = "x86_64")]
use crate::spaces::simple_avx::*;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use crate::spaces::simple_neon::*;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use crate::spaces::simple_sse::*;
use crate::types::Distance;

impl Metric<VectorElementTypeHalf> for CosineMetric {
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

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { neon_dot_similarity_half(v1, v2) };
            }
        }

        dot_similarity_half(v1, v2)
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
