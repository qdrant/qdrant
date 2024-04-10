use common::types::ScoreType;

use crate::data_types::vectors::{DenseVector, TypedDenseVector, VectorElementTypeByte};
use crate::spaces::metric::Metric;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use crate::spaces::metric_uint::neon::neon_cosine::neon_cosine_similarity_bytes;
use crate::spaces::simple::{CosineMetric, MIN_DIM_SIZE_SIMD};
use crate::types::Distance;

impl Metric<VectorElementTypeByte> for CosineMetric {
    fn distance() -> Distance {
        Distance::Cosine
    }

    fn similarity(v1: &[VectorElementTypeByte], v2: &[VectorElementTypeByte]) -> ScoreType {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("fma")
                && v1.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { avx_cosine_similarity_bytes(v1, v2) };
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { sse_cosine_similarity_bytes(v1, v2) };
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") && v1.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { neon_cosine_similarity_bytes(v1, v2) };
            }
        }

        cosine_similarity_bytes(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> TypedDenseVector<VectorElementTypeByte> {
        vector
            .into_iter()
            .map(|x| x as VectorElementTypeByte)
            .collect()
    }
}

pub fn cosine_similarity_bytes(
    v1: &[VectorElementTypeByte],
    v2: &[VectorElementTypeByte],
) -> ScoreType {
    let mut dot_product = 0;
    let mut norm1 = 0;
    let mut norm2 = 0;

    for (a, b) in v1.iter().zip(v2) {
        dot_product += (*a as i32) * (*b as i32);
        norm1 += (*a as i32) * (*a as i32);
        norm2 += (*b as i32) * (*b as i32);
    }

    if norm1 == 0 || norm2 == 0 {
        return 0.0;
    }

    dot_product as ScoreType / ((norm1 as ScoreType * norm2 as ScoreType).sqrt())
}
