//! x86_64 SIMD implementations of 4-bit codebook dot product.
//!
//! Two kernels:
//! - AVX2: processes 32 coordinates per iteration, i64 accumulation
//! - SSSE3: processes 16 coordinates per iteration, i64 accumulation
//!
//! Both convert to float only once after the loop.

use super::{SimdCodebook4, SimdQuery4};

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

// ---------------------------------------------------------------------------
// AVX2 kernel — 32 coords/iter
// ---------------------------------------------------------------------------

/// AVX2-accelerated 4-bit codebook dot product (32 coords per iteration).
///
/// # Safety
/// Requires AVX2 support. Caller must verify with `is_x86_feature_detected!("avx2")`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn codebook_dot_avx2(
    packed: &[u8],
    codebook: &SimdCodebook4,
    query: &SimdQuery4,
    padded_dim: usize,
) -> f32 {
    unsafe {
        // Broadcast 16-byte centroid tables to both 128-bit lanes
        let c_lo_128 =
            _mm_loadu_si128(codebook.centroid_bytes_lo.as_ptr() as *const __m128i);
        let c_hi_128 =
            _mm_loadu_si128(codebook.centroid_bytes_hi.as_ptr() as *const __m128i);
        let c_lo = _mm256_broadcastsi128_si256(c_lo_128);
        let c_hi = _mm256_broadcastsi128_si256(c_hi_128);
        let nibble_mask = _mm_set1_epi8(0x0F);

        // 4 × i64 accumulator
        let mut acc = _mm256_setzero_si256();
        let num_iters = padded_dim / 32;

        for iter in 0..num_iters {
            let byte_offset = iter * 16; // 32 coords × 4 bits / 8 = 16 bytes
            let query_offset = iter * 32;

            // Load 16 packed bytes (32 × 4-bit indices)
            let packed_128 =
                _mm_loadu_si128(packed.as_ptr().add(byte_offset) as *const __m128i);

            // Extract nibbles (128-bit)
            let hi_nib = _mm_and_si128(_mm_srli_epi16(packed_128, 4), nibble_mask);
            let lo_nib = _mm_and_si128(packed_128, nibble_mask);

            // Interleave to sequential indices (128-bit → two halves)
            let indices_lo = _mm_unpacklo_epi8(hi_nib, lo_nib); // coords 0-15
            let indices_hi = _mm_unpackhi_epi8(hi_nib, lo_nib); // coords 16-31

            // Combine into 256-bit: [coords 0-15 | coords 16-31]
            let indices_256 = _mm256_inserti128_si256(
                _mm256_castsi128_si256(indices_lo),
                indices_hi,
                1,
            );

            // 256-bit table lookup (each lane independently)
            let lu_lo = _mm256_shuffle_epi8(c_lo, indices_256);
            let lu_hi = _mm256_shuffle_epi8(c_hi, indices_256);

            // Combine lo/hi bytes → i16 centroid values
            // unpacklo per lane: 8 i16 for coords 0-7 | 8 i16 for coords 16-23
            let cent_a = _mm256_unpacklo_epi8(lu_lo, lu_hi);
            // unpackhi per lane: 8 i16 for coords 8-15 | 8 i16 for coords 24-31
            let cent_b = _mm256_unpackhi_epi8(lu_lo, lu_hi);

            // Load 32 query i16 values as 2 × 256-bit (sequential)
            let q_seq_lo = _mm256_loadu_si256(
                query.values.as_ptr().add(query_offset) as *const __m256i,
            ); // q[0..15]
            let q_seq_hi = _mm256_loadu_si256(
                query.values.as_ptr().add(query_offset + 16) as *const __m256i,
            ); // q[16..31]

            // Permute to match centroid layout:
            // q_a = [q[0..7] | q[16..23]], q_b = [q[8..15] | q[24..31]]
            let q_a = _mm256_permute2x128_si256(q_seq_lo, q_seq_hi, 0x20);
            let q_b = _mm256_permute2x128_si256(q_seq_lo, q_seq_hi, 0x31);

            // madd: pairs of i16 → i32 (8 i32 per result)
            let prod_a = _mm256_madd_epi16(cent_a, q_a);
            let prod_b = _mm256_madd_epi16(cent_b, q_b);

            // Widen i32 → i64 and accumulate (4 widenings per 32 coords)
            let a_lo = _mm256_castsi256_si128(prod_a);
            let a_hi = _mm256_extracti128_si256(prod_a, 1);
            acc = _mm256_add_epi64(acc, _mm256_cvtepi32_epi64(a_lo));
            acc = _mm256_add_epi64(acc, _mm256_cvtepi32_epi64(a_hi));

            let b_lo = _mm256_castsi256_si128(prod_b);
            let b_hi = _mm256_extracti128_si256(prod_b, 1);
            acc = _mm256_add_epi64(acc, _mm256_cvtepi32_epi64(b_lo));
            acc = _mm256_add_epi64(acc, _mm256_cvtepi32_epi64(b_hi));
        }

        // Horizontal sum of 4 i64 lanes
        let lo128 = _mm256_castsi256_si128(acc);
        let hi128 = _mm256_extracti128_si256(acc, 1);
        let sum128 = _mm_add_epi64(lo128, hi128);
        let hi64 = _mm_srli_si128(sum128, 8);
        let final128 = _mm_add_epi64(sum128, hi64);
        let sum = _mm_cvtsi128_si64(final128);

        // Scalar tail for remaining coords (< 32)
        let sum =
            scalar_tail(packed, codebook, query, num_iters * 32, padded_dim, sum);

        (sum as f64 / (codebook.centroid_scale as f64 * query.query_scale as f64)) as f32
    }
}

// ---------------------------------------------------------------------------
// SSSE3 kernel — 16 coords/iter
// ---------------------------------------------------------------------------

/// SSSE3-accelerated 4-bit codebook dot product (16 coords per iteration).
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
    unsafe {
        let c_lo =
            _mm_loadu_si128(codebook.centroid_bytes_lo.as_ptr() as *const __m128i);
        let c_hi =
            _mm_loadu_si128(codebook.centroid_bytes_hi.as_ptr() as *const __m128i);
        let nibble_mask = _mm_set1_epi8(0x0F);

        // 4 × i64 accumulator (2 × __m128i)
        let mut acc_0 = _mm_setzero_si128();
        let mut acc_1 = _mm_setzero_si128();
        let num_iters = padded_dim / 16;

        for iter in 0..num_iters {
            let byte_offset = iter * 8;
            let query_offset = iter * 16;

            // Load 8 packed bytes, extract nibbles, interleave to 16 indices
            let packed_bytes =
                _mm_loadl_epi64(packed.as_ptr().add(byte_offset) as *const __m128i);
            let hi_nibbles =
                _mm_and_si128(_mm_srli_epi16(packed_bytes, 4), nibble_mask);
            let lo_nibbles = _mm_and_si128(packed_bytes, nibble_mask);
            let indices = _mm_unpacklo_epi8(hi_nibbles, lo_nibbles);

            // Table lookup → centroid bytes → combine to i16
            let looked_up_lo = _mm_shuffle_epi8(c_lo, indices);
            let looked_up_hi = _mm_shuffle_epi8(c_hi, indices);
            let centroids_0_7 = _mm_unpacklo_epi8(looked_up_lo, looked_up_hi);
            let centroids_8_15 = _mm_unpackhi_epi8(looked_up_lo, looked_up_hi);

            // Load query values
            let q_0_7 = _mm_loadu_si128(
                query.values.as_ptr().add(query_offset) as *const __m128i,
            );
            let q_8_15 = _mm_loadu_si128(
                query.values.as_ptr().add(query_offset + 8) as *const __m128i,
            );

            // madd → 2 × i32x4
            let prod_0 = _mm_madd_epi16(centroids_0_7, q_0_7);
            let prod_1 = _mm_madd_epi16(centroids_8_15, q_8_15);

            // Widen i32 → i64 via sign extension (SSE2) and accumulate
            let sign_0 = _mm_srai_epi32(prod_0, 31);
            acc_0 = _mm_add_epi64(acc_0, _mm_unpacklo_epi32(prod_0, sign_0));
            acc_1 = _mm_add_epi64(acc_1, _mm_unpackhi_epi32(prod_0, sign_0));

            let sign_1 = _mm_srai_epi32(prod_1, 31);
            acc_0 = _mm_add_epi64(acc_0, _mm_unpacklo_epi32(prod_1, sign_1));
            acc_1 = _mm_add_epi64(acc_1, _mm_unpackhi_epi32(prod_1, sign_1));
        }

        // Horizontal sum of 4 i64 lanes
        let total = _mm_add_epi64(acc_0, acc_1);
        let hi = _mm_srli_si128(total, 8);
        let final128 = _mm_add_epi64(total, hi);
        let sum = _mm_cvtsi128_si64(final128);

        // Scalar tail for remaining coords (< 16)
        let sum =
            scalar_tail(packed, codebook, query, num_iters * 16, padded_dim, sum);

        (sum as f64 / (codebook.centroid_scale as f64 * query.query_scale as f64)) as f32
    }
}

// ---------------------------------------------------------------------------
// Scalar tail (shared)
// ---------------------------------------------------------------------------

fn scalar_tail(
    packed: &[u8],
    codebook: &SimdCodebook4,
    query: &SimdQuery4,
    start_coord: usize,
    padded_dim: usize,
    mut sum: i64,
) -> i64 {
    let centroids_i16 = super::reconstruct_centroids_i16(codebook);
    for k in (start_coord / 2)..(padded_dim / 2) {
        let byte = packed[k];
        let coord = k * 2;
        sum += query.values[coord] as i64 * centroids_i16[(byte >> 4) as usize] as i64;
        sum += query.values[coord + 1] as i64 * centroids_i16[(byte & 0x0F) as usize] as i64;
    }
    sum
}
