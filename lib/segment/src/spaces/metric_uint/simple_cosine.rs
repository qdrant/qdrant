use common::types::ScoreType;

use crate::data_types::vectors::{DenseVector, VectorElementTypeByte};
use crate::spaces::metric::Metric;
#[cfg(target_arch = "x86_64")]
use crate::spaces::metric_uint::avx2::cosine::avx_cosine_similarity_bytes;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use crate::spaces::metric_uint::neon::cosine::neon_cosine_similarity_bytes;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use crate::spaces::metric_uint::sse2::cosine::sse_cosine_similarity_bytes;
#[cfg(target_arch = "x86_64")]
use crate::spaces::simple::MIN_DIM_SIZE_AVX;
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
                && is_x86_feature_detected!("avx2")
                && is_x86_feature_detected!("fma")
                && v1.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { avx_cosine_similarity_bytes(v1, v2) };
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("sse")
                && is_x86_feature_detected!("sse2")
                && v1.len() >= MIN_DIM_SIZE_SIMD
            {
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

    fn preprocess(vector: DenseVector) -> DenseVector {
        vector
    }
}

pub fn cosine_similarity_bytes(
    v1: &[VectorElementTypeByte],
    v2: &[VectorElementTypeByte],
) -> ScoreType {
    // All three accumulators are single, so each overflows `i32` from
    // dimension `33_027` on; the accepted maximum is `65_536`.
    let mut dot_product = 0i64;
    let mut norm1 = 0i64;
    let mut norm2 = 0i64;

    for (a, b) in v1.iter().zip(v2) {
        let a = i64::from(*a);
        let b = i64::from(*b);
        dot_product += a * b;
        norm1 += a * a;
        norm2 += b * b;
    }

    if norm1 == 0 || norm2 == 0 {
        return 0.0;
    }

    dot_product as ScoreType / (norm1 as ScoreType * norm2 as ScoreType).sqrt()
}

#[test]
fn test_zero() {
    let v1: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0];
    let v2: Vec<u8> = vec![255, 255, 0, 254, 253, 252, 251, 250];

    assert_eq!(cosine_similarity_bytes(&v1, &v2), 0.0);
    assert_eq!(cosine_similarity_bytes(&v2, &v1), 0.0);
    assert_eq!(cosine_similarity_bytes(&v1, &v1), 0.0);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// All three accumulators are single, so each leaves `i32` from dimension
    /// `33_027` on. The wrapped `dot_product` turns negative, which made a
    /// vector look anti-correlated *with itself*. `VectorParams.size` accepts up
    /// to `65_536` (`lib/api/build.rs`), where the exact sum is `1.98 * i32::MAX`.
    #[test]
    fn test_max_values_do_not_overflow() {
        // unit vector, a small size, the last dimension below the i32 boundary,
        // and the highest accepted dimension
        for dim in [1, 128, 1536, 33_026, 65_536] {
            let vector = vec![255u8; dim];
            assert_eq!(
                cosine_similarity_bytes(&vector, &vector),
                1.0,
                "a vector must be perfectly similar to itself, dim {dim}",
            );
        }
    }

    /// The same overflow in the byte dot product: the score wrapped negative
    /// instead of staying a positive sum of `u8` products.
    #[test]
    fn test_dot_max_values_do_not_overflow() {
        for dim in [1, 128, 1536, 33_026, 65_536] {
            let vector = vec![255u8; dim];
            let expected = 255.0 * 255.0 * dim as ScoreType;
            assert_eq!(
                crate::spaces::metric_uint::simple_dot::dot_similarity_bytes(&vector, &vector),
                expected,
                "dot product must accumulate without overflow, dim {dim}",
            );
        }
    }
}
