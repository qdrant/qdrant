//! x86_64 SIMD implementations of 4-bit and 2-bit codebook dot product.
//!
//! 4-bit kernels: AVX2 (32 coords/iter), SSSE3 (16 coords/iter).
//! 2-bit kernels: fused even/odd with shared accumulator.
//! All accumulate in i64 and convert to float once at the end.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

use super::{SimdCodebook2, SimdCodebook4, SimdQuery1, SimdQuery2, SimdQuery4};

// ============================================================================
// Helpers
// ============================================================================

/// Horizontal sum of 4 i64 lanes in a __m256i.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn hsum_i64_avx2(acc: __m256i) -> i64 {
    unsafe {
        let lo128 = _mm256_castsi256_si128(acc);
        let hi128 = _mm256_extracti128_si256(acc, 1);
        let sum128 = _mm_add_epi64(lo128, hi128);
        let hi64 = _mm_srli_si128(sum128, 8);
        _mm_cvtsi128_si64(_mm_add_epi64(sum128, hi64))
    }
}

/// Horizontal sum of 4 i64 lanes in 2 × __m128i.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
#[inline]
unsafe fn hsum_i64_sse(acc_0: __m128i, acc_1: __m128i) -> i64 {
    unsafe {
        let total = _mm_add_epi64(acc_0, acc_1);
        let hi = _mm_srli_si128(total, 8);
        _mm_cvtsi128_si64(_mm_add_epi64(total, hi))
    }
}

/// Widen 4 × i32 → 4 × i64 (SSE2 sign extension) and add to two i64 accumulators.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
#[inline]
unsafe fn widen_add_i32_to_i64_sse(prod: __m128i, acc_0: &mut __m128i, acc_1: &mut __m128i) {
    unsafe {
        let sign = _mm_srai_epi32(prod, 31);
        *acc_0 = _mm_add_epi64(*acc_0, _mm_unpacklo_epi32(prod, sign));
        *acc_1 = _mm_add_epi64(*acc_1, _mm_unpackhi_epi32(prod, sign));
    }
}

/// Extract nibble indices from packed bytes into a 128-bit register.
/// Returns 16 × u8 indices in sequential order [idx0, idx1, ...].
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
#[inline]
unsafe fn extract_nibbles_128(packed_ptr: *const u8, nibble_mask: __m128i) -> __m128i {
    unsafe {
        let packed_bytes = _mm_loadl_epi64(packed_ptr as *const __m128i);
        let hi = _mm_and_si128(_mm_srli_epi16(packed_bytes, 4), nibble_mask);
        let lo = _mm_and_si128(packed_bytes, nibble_mask);
        _mm_unpacklo_epi8(hi, lo)
    }
}

// ============================================================================
// 4-bit AVX2 — 32 coords/iter
// ============================================================================

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn codebook_dot_avx2(
    packed: &[u8],
    codebook: &SimdCodebook4,
    query: &SimdQuery4,
    padded_dim: usize,
) -> f32 {
    unsafe {
        let c_lo = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            codebook.centroid_bytes_lo.as_ptr() as *const __m128i,
        ));
        let c_hi = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            codebook.centroid_bytes_hi.as_ptr() as *const __m128i,
        ));
        let nibble_mask = _mm_set1_epi8(0x0F);
        let mut acc = _mm256_setzero_si256();
        let num_iters = padded_dim / 32;

        for iter in 0..num_iters {
            let packed_128 = _mm_loadu_si128(packed.as_ptr().add(iter * 16) as *const __m128i);
            let hi_nib = _mm_and_si128(_mm_srli_epi16(packed_128, 4), nibble_mask);
            let lo_nib = _mm_and_si128(packed_128, nibble_mask);
            let indices_256 = _mm256_inserti128_si256(
                _mm256_castsi128_si256(_mm_unpacklo_epi8(hi_nib, lo_nib)),
                _mm_unpackhi_epi8(hi_nib, lo_nib),
                1,
            );

            let lu_lo = _mm256_shuffle_epi8(c_lo, indices_256);
            let lu_hi = _mm256_shuffle_epi8(c_hi, indices_256);
            let cent_a = _mm256_unpacklo_epi8(lu_lo, lu_hi);
            let cent_b = _mm256_unpackhi_epi8(lu_lo, lu_hi);

            let q_off = iter * 32;
            let q_seq_lo = _mm256_loadu_si256(query.values.as_ptr().add(q_off) as *const __m256i);
            let q_seq_hi =
                _mm256_loadu_si256(query.values.as_ptr().add(q_off + 16) as *const __m256i);
            let q_a = _mm256_permute2x128_si256(q_seq_lo, q_seq_hi, 0x20);
            let q_b = _mm256_permute2x128_si256(q_seq_lo, q_seq_hi, 0x31);

            let prod_a = _mm256_madd_epi16(cent_a, q_a);
            let prod_b = _mm256_madd_epi16(cent_b, q_b);

            acc = _mm256_add_epi64(acc, _mm256_cvtepi32_epi64(_mm256_castsi256_si128(prod_a)));
            acc = _mm256_add_epi64(
                acc,
                _mm256_cvtepi32_epi64(_mm256_extracti128_si256(prod_a, 1)),
            );
            acc = _mm256_add_epi64(acc, _mm256_cvtepi32_epi64(_mm256_castsi256_si128(prod_b)));
            acc = _mm256_add_epi64(
                acc,
                _mm256_cvtepi32_epi64(_mm256_extracti128_si256(prod_b, 1)),
            );
        }

        let sum = hsum_i64_avx2(acc);
        let sum = scalar_tail_4bit(packed, codebook, query, num_iters * 32, padded_dim, sum);
        (sum as f64 / (codebook.centroid_scale as f64 * query.query_scale as f64)) as f32
    }
}

// ============================================================================
// 4-bit SSSE3 — 16 coords/iter
// ============================================================================

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
pub unsafe fn codebook_dot_ssse3(
    packed: &[u8],
    codebook: &SimdCodebook4,
    query: &SimdQuery4,
    padded_dim: usize,
) -> f32 {
    unsafe {
        let c_lo = _mm_loadu_si128(codebook.centroid_bytes_lo.as_ptr() as *const __m128i);
        let c_hi = _mm_loadu_si128(codebook.centroid_bytes_hi.as_ptr() as *const __m128i);
        let nibble_mask = _mm_set1_epi8(0x0F);
        let mut acc_0 = _mm_setzero_si128();
        let mut acc_1 = _mm_setzero_si128();
        let num_iters = padded_dim / 16;

        for iter in 0..num_iters {
            let indices = extract_nibbles_128(packed.as_ptr().add(iter * 8), nibble_mask);
            let lu_lo = _mm_shuffle_epi8(c_lo, indices);
            let lu_hi = _mm_shuffle_epi8(c_hi, indices);
            let cent_0_7 = _mm_unpacklo_epi8(lu_lo, lu_hi);
            let cent_8_15 = _mm_unpackhi_epi8(lu_lo, lu_hi);

            let q_off = iter * 16;
            let q_0_7 = _mm_loadu_si128(query.values.as_ptr().add(q_off) as *const __m128i);
            let q_8_15 = _mm_loadu_si128(query.values.as_ptr().add(q_off + 8) as *const __m128i);

            widen_add_i32_to_i64_sse(_mm_madd_epi16(cent_0_7, q_0_7), &mut acc_0, &mut acc_1);
            widen_add_i32_to_i64_sse(_mm_madd_epi16(cent_8_15, q_8_15), &mut acc_0, &mut acc_1);
        }

        let sum = hsum_i64_sse(acc_0, acc_1);
        let sum = scalar_tail_4bit(packed, codebook, query, num_iters * 16, padded_dim, sum);
        (sum as f64 / (codebook.centroid_scale as f64 * query.query_scale as f64)) as f32
    }
}

fn scalar_tail_4bit(
    packed: &[u8],
    codebook: &SimdCodebook4,
    query: &SimdQuery4,
    start_coord: usize,
    padded_dim: usize,
    mut sum: i64,
) -> i64 {
    let centroids_i16 =
        super::reconstruct_centroids_i16(&codebook.centroid_bytes_lo, &codebook.centroid_bytes_hi);
    for k in (start_coord / 2)..(padded_dim / 2) {
        let byte = packed[k];
        let coord = k * 2;
        sum += query.values[coord] as i64 * centroids_i16[(byte >> 4) as usize] as i64;
        sum += query.values[coord + 1] as i64 * centroids_i16[(byte & 0x0F) as usize] as i64;
    }
    sum
}

// ============================================================================
// 2-bit fused AVX2 — 64 coords/iter (32 nibbles = 16 bytes)
// ============================================================================

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn codebook_dot_2bit_avx2(
    packed: &[u8],
    codebook: &SimdCodebook2,
    query: &SimdQuery2,
    padded_dim: usize,
) -> f32 {
    unsafe {
        let ce_lo = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            codebook.even_lo.as_ptr() as *const __m128i
        ));
        let ce_hi = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            codebook.even_hi.as_ptr() as *const __m128i
        ));
        let co_lo = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            codebook.odd_lo.as_ptr() as *const __m128i
        ));
        let co_hi = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            codebook.odd_hi.as_ptr() as *const __m128i
        ));
        let nibble_mask = _mm_set1_epi8(0x0F);

        let mut acc = _mm256_setzero_si256();
        let num_nibbles = padded_dim / 2;
        let num_iters = num_nibbles / 32; // 32 nibbles = 16 bytes per iter

        for iter in 0..num_iters {
            let packed_128 = _mm_loadu_si128(packed.as_ptr().add(iter * 16) as *const __m128i);
            let hi_nib = _mm_and_si128(_mm_srli_epi16(packed_128, 4), nibble_mask);
            let lo_nib = _mm_and_si128(packed_128, nibble_mask);
            let indices_256 = _mm256_inserti128_si256(
                _mm256_castsi128_si256(_mm_unpacklo_epi8(hi_nib, lo_nib)),
                _mm_unpackhi_epi8(hi_nib, lo_nib),
                1,
            );

            // Even lookup
            let elu = _mm256_shuffle_epi8(ce_lo, indices_256);
            let ehu = _mm256_shuffle_epi8(ce_hi, indices_256);
            let ecent_a = _mm256_unpacklo_epi8(elu, ehu);
            let ecent_b = _mm256_unpackhi_epi8(elu, ehu);

            // Odd lookup
            let olu = _mm256_shuffle_epi8(co_lo, indices_256);
            let ohu = _mm256_shuffle_epi8(co_hi, indices_256);
            let ocent_a = _mm256_unpacklo_epi8(olu, ohu);
            let ocent_b = _mm256_unpackhi_epi8(olu, ohu);

            // Load even query
            let qe_off = iter * 32;
            let qe_lo256 =
                _mm256_loadu_si256(query.even_values.as_ptr().add(qe_off) as *const __m256i);
            let qe_hi256 =
                _mm256_loadu_si256(query.even_values.as_ptr().add(qe_off + 16) as *const __m256i);
            let qe_a = _mm256_permute2x128_si256(qe_lo256, qe_hi256, 0x20);
            let qe_b = _mm256_permute2x128_si256(qe_lo256, qe_hi256, 0x31);

            // Load odd query
            let qo_lo256 =
                _mm256_loadu_si256(query.odd_values.as_ptr().add(qe_off) as *const __m256i);
            let qo_hi256 =
                _mm256_loadu_si256(query.odd_values.as_ptr().add(qe_off + 16) as *const __m256i);
            let qo_a = _mm256_permute2x128_si256(qo_lo256, qo_hi256, 0x20);
            let qo_b = _mm256_permute2x128_si256(qo_lo256, qo_hi256, 0x31);

            // madd even and odd separately (can't add i32 results — would overflow)
            let eprod_a = _mm256_madd_epi16(ecent_a, qe_a);
            let eprod_b = _mm256_madd_epi16(ecent_b, qe_b);
            let oprod_a = _mm256_madd_epi16(ocent_a, qo_a);
            let oprod_b = _mm256_madd_epi16(ocent_b, qo_b);

            // Widen each i32 → i64 separately, then accumulate
            acc = _mm256_add_epi64(acc, _mm256_cvtepi32_epi64(_mm256_castsi256_si128(eprod_a)));
            acc = _mm256_add_epi64(
                acc,
                _mm256_cvtepi32_epi64(_mm256_extracti128_si256(eprod_a, 1)),
            );
            acc = _mm256_add_epi64(acc, _mm256_cvtepi32_epi64(_mm256_castsi256_si128(eprod_b)));
            acc = _mm256_add_epi64(
                acc,
                _mm256_cvtepi32_epi64(_mm256_extracti128_si256(eprod_b, 1)),
            );
            acc = _mm256_add_epi64(acc, _mm256_cvtepi32_epi64(_mm256_castsi256_si128(oprod_a)));
            acc = _mm256_add_epi64(
                acc,
                _mm256_cvtepi32_epi64(_mm256_extracti128_si256(oprod_a, 1)),
            );
            acc = _mm256_add_epi64(acc, _mm256_cvtepi32_epi64(_mm256_castsi256_si128(oprod_b)));
            acc = _mm256_add_epi64(
                acc,
                _mm256_cvtepi32_epi64(_mm256_extracti128_si256(oprod_b, 1)),
            );
        }

        let sum = hsum_i64_avx2(acc);
        let sum = scalar_tail_2bit(packed, codebook, query, num_iters * 32, num_nibbles, sum);
        (sum as f64 / (codebook.centroid_scale as f64 * query.query_scale as f64)) as f32
    }
}

// ============================================================================
// 2-bit fused SSSE3 — 32 coords/iter (16 nibbles = 8 bytes)
// ============================================================================

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
pub unsafe fn codebook_dot_2bit_ssse3(
    packed: &[u8],
    codebook: &SimdCodebook2,
    query: &SimdQuery2,
    padded_dim: usize,
) -> f32 {
    unsafe {
        let ce_lo = _mm_loadu_si128(codebook.even_lo.as_ptr() as *const __m128i);
        let ce_hi = _mm_loadu_si128(codebook.even_hi.as_ptr() as *const __m128i);
        let co_lo = _mm_loadu_si128(codebook.odd_lo.as_ptr() as *const __m128i);
        let co_hi = _mm_loadu_si128(codebook.odd_hi.as_ptr() as *const __m128i);
        let nibble_mask = _mm_set1_epi8(0x0F);

        let mut acc_0 = _mm_setzero_si128();
        let mut acc_1 = _mm_setzero_si128();
        let num_nibbles = padded_dim / 2;
        let num_iters = num_nibbles / 16;

        for iter in 0..num_iters {
            let indices = extract_nibbles_128(packed.as_ptr().add(iter * 8), nibble_mask);

            // Even lookup
            let ecent_0_7 = _mm_unpacklo_epi8(
                _mm_shuffle_epi8(ce_lo, indices),
                _mm_shuffle_epi8(ce_hi, indices),
            );
            let ecent_8_15 = _mm_unpackhi_epi8(
                _mm_shuffle_epi8(ce_lo, indices),
                _mm_shuffle_epi8(ce_hi, indices),
            );

            // Odd lookup
            let ocent_0_7 = _mm_unpacklo_epi8(
                _mm_shuffle_epi8(co_lo, indices),
                _mm_shuffle_epi8(co_hi, indices),
            );
            let ocent_8_15 = _mm_unpackhi_epi8(
                _mm_shuffle_epi8(co_lo, indices),
                _mm_shuffle_epi8(co_hi, indices),
            );

            let q_off = iter * 16;
            let qe_0_7 = _mm_loadu_si128(query.even_values.as_ptr().add(q_off) as *const __m128i);
            let qe_8_15 =
                _mm_loadu_si128(query.even_values.as_ptr().add(q_off + 8) as *const __m128i);
            let qo_0_7 = _mm_loadu_si128(query.odd_values.as_ptr().add(q_off) as *const __m128i);
            let qo_8_15 =
                _mm_loadu_si128(query.odd_values.as_ptr().add(q_off + 8) as *const __m128i);

            // Widen each madd result to i64 separately (can't add i32 — would overflow)
            widen_add_i32_to_i64_sse(_mm_madd_epi16(ecent_0_7, qe_0_7), &mut acc_0, &mut acc_1);
            widen_add_i32_to_i64_sse(_mm_madd_epi16(ecent_8_15, qe_8_15), &mut acc_0, &mut acc_1);
            widen_add_i32_to_i64_sse(_mm_madd_epi16(ocent_0_7, qo_0_7), &mut acc_0, &mut acc_1);
            widen_add_i32_to_i64_sse(_mm_madd_epi16(ocent_8_15, qo_8_15), &mut acc_0, &mut acc_1);
        }

        let sum = hsum_i64_sse(acc_0, acc_1);
        let sum = scalar_tail_2bit(packed, codebook, query, num_iters * 16, num_nibbles, sum);
        (sum as f64 / (codebook.centroid_scale as f64 * query.query_scale as f64)) as f32
    }
}

fn scalar_tail_2bit(
    packed: &[u8],
    codebook: &SimdCodebook2,
    query: &SimdQuery2,
    start_nib: usize,
    num_nibbles: usize,
    mut sum: i64,
) -> i64 {
    let even_i16 = super::reconstruct_centroids_i16(&codebook.even_lo, &codebook.even_hi);
    let odd_i16 = super::reconstruct_centroids_i16(&codebook.odd_lo, &codebook.odd_hi);
    for nib_idx in start_nib..num_nibbles {
        let byte = packed[nib_idx / 2];
        let nibble = if nib_idx % 2 == 0 {
            (byte >> 4) as usize
        } else {
            (byte & 0x0F) as usize
        };
        sum += query.even_values[nib_idx] as i64 * even_i16[nibble] as i64;
        sum += query.odd_values[nib_idx] as i64 * odd_i16[nibble] as i64;
    }
    sum
}

// ============================================================================
// 1-bit AND+popcount kernel — 64 dims/iter using scalar popcnt
// ============================================================================

/// # Safety
/// Requires popcnt support (available on all x86_64 with SSE4.2+).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "popcnt")]
pub unsafe fn and_popcount_u16_x86(packed: &[u8], query: &SimdQuery1) -> (u64, u32) {
    unsafe {
        let mut s1 = 0u64;
        let mut popcnt_v = 0u32;
        for chunk in 0..query.num_chunks {
            let v = *(packed.as_ptr().add(chunk * 8) as *const u64);
            popcnt_v += _popcnt_u64(v) as u32;
            let base = chunk * 16;
            for b in 0..16u64 {
                s1 += (_popcnt_u64(v & query.planes[base + b as usize]) as u64) << b;
            }
        }
        (s1, popcnt_v)
    }
}

// ============================================================================
// 1-bit XOR+popcount for score_internal
// ============================================================================

/// # Safety
/// Requires popcnt support.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "popcnt")]
pub unsafe fn xor_popcount_x86(packed1: &[u8], packed2: &[u8], num_bytes: usize) -> u32 {
    unsafe {
        let mut count = 0u32;
        let num_u64 = num_bytes / 8;
        for i in 0..num_u64 {
            let off = i * 8;
            let a = *(packed1.as_ptr().add(off) as *const u64);
            let b = *(packed2.as_ptr().add(off) as *const u64);
            count += _popcnt_u64(a ^ b) as u32;
        }
        for i in (num_u64 * 8)..num_bytes {
            count += (*packed1.as_ptr().add(i) ^ *packed2.as_ptr().add(i)).count_ones();
        }
        count
    }
}
