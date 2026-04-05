//! x86_64 SSSE3 implementation of 4-bit codebook dot product.
//!
//! Processes 16 coordinates per iteration:
//! 1. Load 8 packed bytes (16 × 4-bit indices)
//! 2. Extract nibbles into 16 u8 indices
//! 3. Use `_mm_shuffle_epi8` to look up i16 centroid bytes (lo/hi tables)
//! 4. Recombine bytes into 16 i16 centroid values
//! 5. `_mm_madd_epi16` with i16 query values → i32 pair sums
//! 6. Convert to f32 and accumulate

use super::{SimdCodebook4, SimdQuery4};

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// SSSE3-accelerated 4-bit codebook dot product.
///
/// # Safety
/// Requires SSSE3 support. Caller must verify with `is_x86_feature_detected!("ssse3")`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
pub unsafe fn codebook_dot_ssse3(
    packed: &[u8],
    codebook: &SimdCodebook4,
    query: &SimdQuery4,
    padded_dim: usize,
) -> f32 {
    // Load centroid byte tables into SSE registers (stay in registers for entire loop)
    let c_lo = _mm_loadu_si128(codebook.centroid_bytes_lo.as_ptr() as *const __m128i);
    let c_hi = _mm_loadu_si128(codebook.centroid_bytes_hi.as_ptr() as *const __m128i);
    let nibble_mask = _mm_set1_epi8(0x0F);

    let mut acc = _mm_setzero_ps();
    let num_iters = padded_dim / 16;

    for iter in 0..num_iters {
        let byte_offset = iter * 8; // 16 coords × 4 bits / 8 = 8 bytes
        let query_offset = iter * 16;

        // 1. Load 8 bytes of packed 4-bit indices into lower 64 bits
        let packed_bytes =
            _mm_loadl_epi64(packed.as_ptr().add(byte_offset) as *const __m128i);

        // 2. Extract nibbles
        // High nibble of each byte = even-indexed coordinate
        let hi_nibbles = _mm_and_si128(_mm_srli_epi16(packed_bytes, 4), nibble_mask);
        // Low nibble of each byte = odd-indexed coordinate
        let lo_nibbles = _mm_and_si128(packed_bytes, nibble_mask);

        // 3. Interleave to sequential order: [idx0, idx1, idx2, idx3, ...]
        let indices = _mm_unpacklo_epi8(hi_nibbles, lo_nibbles);

        // 4. Look up centroid bytes using 16-entry table lookup
        let looked_up_lo = _mm_shuffle_epi8(c_lo, indices);
        let looked_up_hi = _mm_shuffle_epi8(c_hi, indices);

        // 5. Combine lo/hi bytes into i16 centroid values
        // unpacklo: [lo[0],hi[0],lo[1],hi[1],...] = 8 i16 values for coords 0-7
        let centroids_0_7 = _mm_unpacklo_epi8(looked_up_lo, looked_up_hi);
        // unpackhi: 8 i16 values for coords 8-15
        let centroids_8_15 = _mm_unpackhi_epi8(looked_up_lo, looked_up_hi);

        // 6. Load 16 i16 query values (two __m128i of 8 i16 each)
        let q_0_7 = _mm_loadu_si128(
            query.values.as_ptr().add(query_offset) as *const __m128i,
        );
        let q_8_15 = _mm_loadu_si128(
            query.values.as_ptr().add(query_offset + 8) as *const __m128i,
        );

        // 7. Multiply-accumulate: madd(a,b) = [a0*b0+a1*b1, a2*b2+a3*b3, ...] → 4 i32
        let prod_0 = _mm_madd_epi16(centroids_0_7, q_0_7);
        let prod_1 = _mm_madd_epi16(centroids_8_15, q_8_15);

        // 8. Convert i32 → f32 and accumulate (avoids i32 overflow across iterations)
        acc = _mm_add_ps(acc, _mm_cvtepi32_ps(prod_0));
        acc = _mm_add_ps(acc, _mm_cvtepi32_ps(prod_1));
    }

    // Horizontal sum of 4 f32 lanes
    let sum = hsum128_ps(acc);

    // Convert integer dot product back to f32
    sum / (codebook.centroid_scale * query.query_scale)
}

/// Horizontal sum of 4 f32 values in an __m128 register.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
#[inline]
unsafe fn hsum128_ps(x: __m128) -> f32 {
    // [a, b, c, d] → [c, d, a, b]
    let hi = _mm_movehl_ps(x, x);
    // [a+c, b+d, ...]
    let sum = _mm_add_ps(x, hi);
    // shuffle to get b+d into position 0
    let shuf = _mm_shuffle_ps(sum, sum, 0x01);
    // [a+c+b+d, ...]
    let sum = _mm_add_ss(sum, shuf);
    _mm_cvtss_f32(sum)
}
