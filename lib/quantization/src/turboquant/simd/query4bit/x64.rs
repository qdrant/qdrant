//! x86_64 kernels for the symmetric 4-bit paths (`score_4bit_internal*`).
//!
//! The codebook is stored as unsigned u8 ∈ [0, 255] (`CODEBOOK_U8`); the
//! kernels XOR it with 0x80 to recover the signed form in-register.

use super::{CODEBOOK_U8, centroid_dot};

#[target_feature(enable = "sse2")]
unsafe fn hsum_i32_sse(v: core::arch::x86_64::__m128i) -> i32 {
    use core::arch::x86_64::*;
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0x4E));
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0xB1));
    _mm_cvtsi128_si32(v)
}

// ------------------------------------------------------------------
// score_4bit_internal — both operands signed, so we can't reuse
// `maddubs` / `VPDPBUSD`.  The honest path widens the signed codebook
// bytes to i16 and uses `madd_epi16` (signed × signed → i32 pair-sum);
// AVX-512 VNNI's `VPDPWSSD` is the fused equivalent on ZMM.
//
// We load `CODEBOOK_U8` and XOR with 0x80 to recover the signed i8 form
// (= c_u − 128).  The resulting `c_signed` lives in [−128, 127], so:
//   per product:       |c_a · c_b| ≤ 128·128 = 16 384
//   madd_epi16 pair:   ≤ 2·16 384 = 32 768 < i32::MAX ✓
//   i32 acc at 64K:    ≤ 16 384·16 384 = 268 M ≪ i32::MAX
// Far away from saturation in every intermediate.
// ------------------------------------------------------------------

/// SSE4.1 + SSSE3 implementation of [`super::score_4bit_internal`].
///
/// # Safety
/// CPU must support `ssse3` and `sse4.1`.
#[target_feature(enable = "sse4.1,ssse3")]
pub unsafe fn score_4bit_internal_sse(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_sse: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let codebook_i8 = _mm_xor_si128(
            _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>()),
            _mm_set1_epi8(-128i8),
        );
        let nibble_mask = _mm_set1_epi8(0x0F);
        let mut acc = _mm_setzero_si128();

        let n_full = a.len() / 8;
        for i in 0..n_full {
            let va = _mm_loadl_epi64(a.as_ptr().add(i * 8).cast::<__m128i>());
            let va_lo = _mm_and_si128(va, nibble_mask);
            let va_hi = _mm_and_si128(_mm_srli_epi16(va, 4), nibble_mask);
            let a_idx = _mm_unpacklo_epi8(va_lo, va_hi);
            let c_a_i8 = _mm_shuffle_epi8(codebook_i8, a_idx);

            let vb = _mm_loadl_epi64(b.as_ptr().add(i * 8).cast::<__m128i>());
            let vb_lo = _mm_and_si128(vb, nibble_mask);
            let vb_hi = _mm_and_si128(_mm_srli_epi16(vb, 4), nibble_mask);
            let b_idx = _mm_unpacklo_epi8(vb_lo, vb_hi);
            let c_b_i8 = _mm_shuffle_epi8(codebook_i8, b_idx);

            let c_a_lo = _mm_cvtepi8_epi16(c_a_i8);
            let c_a_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_a_i8, 8));
            let c_b_lo = _mm_cvtepi8_epi16(c_b_i8);
            let c_b_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_b_i8, 8));

            let prod_lo = _mm_madd_epi16(c_a_lo, c_b_lo);
            let prod_hi = _mm_madd_epi16(c_a_hi, c_b_hi);
            acc = _mm_add_epi32(acc, _mm_add_epi32(prod_lo, prod_hi));
        }

        let simd_bytes = n_full * 8;
        let sum = i64::from(hsum_i32_sse(acc))
            + super::score_4bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        centroid_dot(sum as f32)
    }
}

/// AVX2 implementation of [`super::score_4bit_internal`].  32 elements per
/// iteration (16 bytes from each source) using YMM widening and
/// `_mm256_madd_epi16`.
///
/// # Safety
/// CPU must support `avx2`.
#[target_feature(enable = "avx2")]
pub unsafe fn score_4bit_internal_avx2(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_avx2: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let codebook_i8_128 = _mm_xor_si128(
            _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>()),
            _mm_set1_epi8(-128i8),
        );
        let codebook_i8 = _mm256_broadcastsi128_si256(codebook_i8_128);
        let nibble_mask = _mm_set1_epi8(0x0F);
        let mut acc = _mm256_setzero_si256();

        let n_iters = a.len() / 16;
        for i in 0..n_iters {
            let va = _mm_loadu_si128(a.as_ptr().add(16 * i).cast::<__m128i>());
            let va_lo = _mm_and_si128(va, nibble_mask);
            let va_hi = _mm_and_si128(_mm_srli_epi16(va, 4), nibble_mask);
            let a_idx_0 = _mm_unpacklo_epi8(va_lo, va_hi);
            let a_idx_1 = _mm_unpackhi_epi8(va_lo, va_hi);
            let a_idx_256 = _mm256_inserti128_si256(_mm256_castsi128_si256(a_idx_0), a_idx_1, 1);
            let c_a_i8 = _mm256_shuffle_epi8(codebook_i8, a_idx_256);

            let vb = _mm_loadu_si128(b.as_ptr().add(16 * i).cast::<__m128i>());
            let vb_lo = _mm_and_si128(vb, nibble_mask);
            let vb_hi = _mm_and_si128(_mm_srli_epi16(vb, 4), nibble_mask);
            let b_idx_0 = _mm_unpacklo_epi8(vb_lo, vb_hi);
            let b_idx_1 = _mm_unpackhi_epi8(vb_lo, vb_hi);
            let b_idx_256 = _mm256_inserti128_si256(_mm256_castsi128_si256(b_idx_0), b_idx_1, 1);
            let c_b_i8 = _mm256_shuffle_epi8(codebook_i8, b_idx_256);

            // Widen i8×32 into two i16×16 halves.
            let c_a_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_a_i8));
            let c_a_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_a_i8, 1));
            let c_b_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_b_i8));
            let c_b_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_b_i8, 1));

            let prod_lo = _mm256_madd_epi16(c_a_lo, c_b_lo);
            let prod_hi = _mm256_madd_epi16(c_a_hi, c_b_hi);
            acc = _mm256_add_epi32(acc, _mm256_add_epi32(prod_lo, prod_hi));
        }

        let acc_lo = _mm256_castsi256_si128(acc);
        let acc_hi = _mm256_extracti128_si256(acc, 1);
        let simd_bytes = n_iters * 16;
        let sum = i64::from(hsum_i32_sse(_mm_add_epi32(acc_lo, acc_hi)))
            + super::score_4bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        centroid_dot(sum as f32)
    }
}

/// AVX-512 VNNI implementation of [`super::score_4bit_internal`].  Uses
/// `VPDPWSSD` — the signed-signed fused counterpart of `VPDPBUSD` —
/// available as part of AVX-512 VNNI on Ice Lake Xeon+, Sapphire Rapids,
/// and Zen 4+.  32 elements per iteration: widen 32 i8 → 32 i16 in one
/// ZMM each, then a single `dpwssd` accumulates 16 pair-products into
/// 16 i32 lanes.
///
/// # Safety
/// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`.
#[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
pub unsafe fn score_4bit_internal_avx512_vnni(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_avx512_vnni: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let codebook_i8_128 = _mm_xor_si128(
            _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>()),
            _mm_set1_epi8(-128i8),
        );
        let codebook_i8_256 = _mm256_broadcastsi128_si256(codebook_i8_128);
        let nibble_mask = _mm_set1_epi8(0x0F);
        let mut acc = _mm512_setzero_si512();

        // 32 elements (= 16 bytes from each source) per iter.  Shuffle stays
        // on 256-bit (32 codebook values fit exactly), then we widen to 512.
        let n_iters = a.len() / 16;
        for i in 0..n_iters {
            let va = _mm_loadu_si128(a.as_ptr().add(16 * i).cast::<__m128i>());
            let va_lo = _mm_and_si128(va, nibble_mask);
            let va_hi = _mm_and_si128(_mm_srli_epi16(va, 4), nibble_mask);
            let a_idx_0 = _mm_unpacklo_epi8(va_lo, va_hi);
            let a_idx_1 = _mm_unpackhi_epi8(va_lo, va_hi);
            let a_idx_256 = _mm256_inserti128_si256(_mm256_castsi128_si256(a_idx_0), a_idx_1, 1);
            let c_a_i8_256 = _mm256_shuffle_epi8(codebook_i8_256, a_idx_256);

            let vb = _mm_loadu_si128(b.as_ptr().add(16 * i).cast::<__m128i>());
            let vb_lo = _mm_and_si128(vb, nibble_mask);
            let vb_hi = _mm_and_si128(_mm_srli_epi16(vb, 4), nibble_mask);
            let b_idx_0 = _mm_unpacklo_epi8(vb_lo, vb_hi);
            let b_idx_1 = _mm_unpackhi_epi8(vb_lo, vb_hi);
            let b_idx_256 = _mm256_inserti128_si256(_mm256_castsi128_si256(b_idx_0), b_idx_1, 1);
            let c_b_i8_256 = _mm256_shuffle_epi8(codebook_i8_256, b_idx_256);

            // Widen 32 i8 → 32 i16, one ZMM each.
            let c_a_i16 = _mm512_cvtepi8_epi16(c_a_i8_256);
            let c_b_i16 = _mm512_cvtepi8_epi16(c_b_i8_256);

            // VPDPWSSD: acc[lane] += a[2·lane]·b[2·lane] + a[2·lane+1]·b[2·lane+1]
            // with every operand interpreted as signed i16.
            acc = _mm512_dpwssd_epi32(acc, c_a_i16, c_b_i16);
        }

        let acc_256_lo = _mm512_castsi512_si256(acc);
        let acc_256_hi = _mm512_extracti64x4_epi64(acc, 1);
        let acc_256 = _mm256_add_epi32(acc_256_lo, acc_256_hi);
        let acc_128 = _mm_add_epi32(
            _mm256_castsi256_si128(acc_256),
            _mm256_extracti128_si256(acc_256, 1),
        );
        let simd_bytes = n_iters * 16;
        let sum = i64::from(hsum_i32_sse(acc_128))
            + super::score_4bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        centroid_dot(sum as f32)
    }
}

// ------------------------------------------------------------------
// score_4bit_internal_folded — TQ+ symmetric path. Each side's signed
// codebook value is scaled by its coordinate's Q15 factor with
// `mulhrs_epi16`, the same rounding as `fold_q15`, which keeps it within
// [−128, 127]; `madd_epi16` then pair-sums the products into i32 lanes:
//   per product:   |q_a · q_b| ≤ 128 · 128 = 16 384
//   total:         ≤ 2¹⁶ · 16 384 = 2³⁰ at `FOLDED_MAX_DIM` ✓
// ------------------------------------------------------------------

/// SSE4.1 + SSSE3 implementation of [`super::score_4bit_internal_folded`].
///
/// # Safety
/// CPU must support `ssse3` and `sse4.1`.
#[target_feature(enable = "sse4.1,ssse3")]
pub unsafe fn score_4bit_internal_folded_sse(a: &[u8], b: &[u8], factors: &[i16]) -> i64 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_folded_sse: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    assert_eq!(
        factors.len(),
        2 * a.len(),
        "score_4bit_internal_folded_sse: factors length {} != 2 · a.len() {}",
        factors.len(),
        2 * a.len(),
    );

    unsafe {
        let codebook_i8 = _mm_xor_si128(
            _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>()),
            _mm_set1_epi8(-128i8),
        );
        let nibble_mask = _mm_set1_epi8(0x0F);
        let mut acc = _mm_setzero_si128();

        // 8 bytes from each source = 16 coords per iter.
        let n_full = a.len() / 8;
        for i in 0..n_full {
            let va = _mm_loadl_epi64(a.as_ptr().add(i * 8).cast::<__m128i>());
            let va_lo = _mm_and_si128(va, nibble_mask);
            let va_hi = _mm_and_si128(_mm_srli_epi16(va, 4), nibble_mask);
            let c_a_i8 = _mm_shuffle_epi8(codebook_i8, _mm_unpacklo_epi8(va_lo, va_hi));

            let vb = _mm_loadl_epi64(b.as_ptr().add(i * 8).cast::<__m128i>());
            let vb_lo = _mm_and_si128(vb, nibble_mask);
            let vb_hi = _mm_and_si128(_mm_srli_epi16(vb, 4), nibble_mask);
            let c_b_i8 = _mm_shuffle_epi8(codebook_i8, _mm_unpacklo_epi8(vb_lo, vb_hi));

            let f_lo = _mm_loadu_si128(factors.as_ptr().add(16 * i).cast::<__m128i>());
            let f_hi = _mm_loadu_si128(factors.as_ptr().add(16 * i + 8).cast::<__m128i>());

            let q_a_lo = _mm_mulhrs_epi16(_mm_cvtepi8_epi16(c_a_i8), f_lo);
            let q_a_hi = _mm_mulhrs_epi16(_mm_cvtepi8_epi16(_mm_srli_si128(c_a_i8, 8)), f_hi);
            let q_b_lo = _mm_mulhrs_epi16(_mm_cvtepi8_epi16(c_b_i8), f_lo);
            let q_b_hi = _mm_mulhrs_epi16(_mm_cvtepi8_epi16(_mm_srli_si128(c_b_i8, 8)), f_hi);

            let products = _mm_add_epi32(
                _mm_madd_epi16(q_a_lo, q_b_lo),
                _mm_madd_epi16(q_a_hi, q_b_hi),
            );
            acc = _mm_add_epi32(acc, products);
        }

        let simd_bytes = n_full * 8;
        i64::from(hsum_i32_sse(acc))
            + super::score_4bit_internal_folded_scalar(
                &a[simd_bytes..],
                &b[simd_bytes..],
                &factors[2 * simd_bytes..],
            )
    }
}

/// The 32 codes of the 16 bytes at `bytes` as their signed codebook values, in coord order.
///
/// # Safety
/// CPU must support `avx2`; `bytes` must be valid for 16 bytes.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn unpack_32_codes_avx2(
    codebook_i8: core::arch::x86_64::__m256i,
    bytes: *const u8,
) -> core::arch::x86_64::__m256i {
    use core::arch::x86_64::*;
    unsafe {
        let nibble_mask = _mm_set1_epi8(0x0F);
        let v = _mm_loadu_si128(bytes.cast::<__m128i>());
        let v_lo = _mm_and_si128(v, nibble_mask);
        let v_hi = _mm_and_si128(_mm_srli_epi16(v, 4), nibble_mask);
        let idx_0 = _mm_unpacklo_epi8(v_lo, v_hi);
        let idx_1 = _mm_unpackhi_epi8(v_lo, v_hi);
        let idx = _mm256_inserti128_si256(_mm256_castsi128_si256(idx_0), idx_1, 1);
        _mm256_shuffle_epi8(codebook_i8, idx)
    }
}

/// The signed codebook in both 128-bit lanes, for [`unpack_32_codes_avx2`].
///
/// # Safety
/// CPU must support `avx2`.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn signed_codebook_avx2() -> core::arch::x86_64::__m256i {
    use core::arch::x86_64::*;
    unsafe {
        let codebook_i8_128 = _mm_xor_si128(
            _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>()),
            _mm_set1_epi8(-128i8),
        );
        _mm256_broadcastsi128_si256(codebook_i8_128)
    }
}

/// AVX2 implementation of [`super::score_4bit_internal_folded`]. 32 coords
/// per iteration.
///
/// # Safety
/// CPU must support `avx2`.
#[target_feature(enable = "avx2")]
pub unsafe fn score_4bit_internal_folded_avx2(a: &[u8], b: &[u8], factors: &[i16]) -> i64 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_folded_avx2: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    assert_eq!(
        factors.len(),
        2 * a.len(),
        "score_4bit_internal_folded_avx2: factors length {} != 2 · a.len() {}",
        factors.len(),
        2 * a.len(),
    );

    unsafe {
        let codebook_i8 = signed_codebook_avx2();
        let mut acc = _mm256_setzero_si256();

        // 16 bytes from each source = 32 coords per iter.
        let n_iters = a.len() / 16;
        for i in 0..n_iters {
            let c_a_i8 = unpack_32_codes_avx2(codebook_i8, a.as_ptr().add(16 * i));
            let c_b_i8 = unpack_32_codes_avx2(codebook_i8, b.as_ptr().add(16 * i));

            let f_lo = _mm256_loadu_si256(factors.as_ptr().add(32 * i).cast::<__m256i>());
            let f_hi = _mm256_loadu_si256(factors.as_ptr().add(32 * i + 16).cast::<__m256i>());

            let q_a_lo =
                _mm256_mulhrs_epi16(_mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_a_i8)), f_lo);
            let q_a_hi = _mm256_mulhrs_epi16(
                _mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_a_i8, 1)),
                f_hi,
            );
            let q_b_lo =
                _mm256_mulhrs_epi16(_mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_b_i8)), f_lo);
            let q_b_hi = _mm256_mulhrs_epi16(
                _mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_b_i8, 1)),
                f_hi,
            );

            let products = _mm256_add_epi32(
                _mm256_madd_epi16(q_a_lo, q_b_lo),
                _mm256_madd_epi16(q_a_hi, q_b_hi),
            );
            acc = _mm256_add_epi32(acc, products);
        }

        let acc_lo = _mm256_castsi256_si128(acc);
        let acc_hi = _mm256_extracti128_si256(acc, 1);
        let simd_bytes = n_iters * 16;
        i64::from(hsum_i32_sse(_mm_add_epi32(acc_lo, acc_hi)))
            + super::score_4bit_internal_folded_scalar(
                &a[simd_bytes..],
                &b[simd_bytes..],
                &factors[2 * simd_bytes..],
            )
    }
}

/// [`super::fold_codes`] for whole groups of 32 codes (16 bytes). Returns the
/// positions done; the values are the ones [`super::score_4bit_internal_folded`]
/// multiplies, XORed with `flip`.
///
/// # Safety
/// CPU must support `avx2`. `codes` must hold `out.len()` codes and `factors`
/// `out.len()` factors.
#[target_feature(enable = "avx2")]
pub(super) unsafe fn fold_codes_avx2(
    codes: &[u8],
    factors: &[i16],
    out: &mut [u8],
    flip: u8,
) -> usize {
    use core::arch::x86_64::*;

    let groups = out.len() / 32;
    // SAFETY: group `g` reads 16 bytes of `codes` and 32 factors, and writes 32 positions, all
    // within the slices for `g < groups` by the caller's lengths
    unsafe {
        let codebook_i8 = signed_codebook_avx2();
        let flip = _mm256_set1_epi8(flip.cast_signed());
        for g in 0..groups {
            let c = unpack_32_codes_avx2(codebook_i8, codes.as_ptr().add(16 * g));
            let f_lo = _mm256_loadu_si256(factors.as_ptr().add(32 * g).cast::<__m256i>());
            let f_hi = _mm256_loadu_si256(factors.as_ptr().add(32 * g + 16).cast::<__m256i>());
            let q_lo = _mm256_mulhrs_epi16(_mm256_cvtepi8_epi16(_mm256_castsi256_si128(c)), f_lo);
            let q_hi =
                _mm256_mulhrs_epi16(_mm256_cvtepi8_epi16(_mm256_extracti128_si256(c, 1)), f_hi);
            // `packs` works per 128-bit lane: positions 0..8, 16..24, 8..16, 24..32, by 64 bits
            let packed = _mm256_permute4x64_epi64(_mm256_packs_epi16(q_lo, q_hi), 0b11_01_10_00);
            let packed = _mm256_xor_si256(packed, flip);
            _mm256_storeu_si256(out.as_mut_ptr().add(32 * g).cast::<__m256i>(), packed);
        }
    }
    groups * 32
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::super::shared::pack_codes;
    use super::super::shared::{PARITY_DIMS, random_inputs};
    use super::super::{
        codebook_signed_i64, fold_codes, score_4bit_internal_folded_scalar,
        score_4bit_internal_scalar,
    };
    use super::{
        score_4bit_internal_avx2, score_4bit_internal_avx512_vnni, score_4bit_internal_folded_avx2,
        score_4bit_internal_folded_sse, score_4bit_internal_sse,
    };
    use crate::turboquant::simd::fold_q15;

    /// Build deterministic non-negative Q15 factors of length `2 · vec_bytes`
    /// for parity tests of the folded kernels.
    fn random_factors(rng: &mut StdRng, vec_bytes: usize) -> Vec<i16> {
        use rand::RngExt;
        (0..2 * vec_bytes)
            .map(|_| rng.random_range(0..=i16::MAX))
            .collect()
    }

    /// Parity: each x86 `score_4bit_internal` variant must reproduce the
    /// scalar reference bit-exactly.  Both sides compute `Σ c_signed_a ·
    /// c_signed_b / c_scale²` with deterministic ordering, so the f32
    /// outputs are identical.
    #[test]
    fn test_score_sse_matches_scalar() {
        if !std::is_x86_feature_detected!("ssse3") || !std::is_x86_feature_detected!("sse4.1") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let scalar = score_4bit_internal_scalar(&vec_a, &vec_b);
            let sse = unsafe { score_4bit_internal_sse(&vec_a, &vec_b) };
            assert_eq!(
                scalar, sse,
                "score: scalar {scalar} != sse {sse} at dim {dim}"
            );
        }
    }

    #[test]
    fn test_score_avx2_matches_scalar() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let scalar = score_4bit_internal_scalar(&vec_a, &vec_b);
            let avx2 = unsafe { score_4bit_internal_avx2(&vec_a, &vec_b) };
            assert_eq!(
                scalar, avx2,
                "score: scalar {scalar} != avx2 {avx2} at dim {dim}"
            );
        }
    }

    #[test]
    fn test_score_avx512_vnni_matches_scalar() {
        if !(std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512bw")
            && std::is_x86_feature_detected!("avx512vnni"))
        {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let scalar = score_4bit_internal_scalar(&vec_a, &vec_b);
            let vnni512 = unsafe { score_4bit_internal_avx512_vnni(&vec_a, &vec_b) };
            assert_eq!(
                scalar, vnni512,
                "score: scalar {scalar} != avx512_vnni {vnni512} at dim {dim}"
            );
        }
    }

    /// Parity for the folded kernel: every x86 backend must match the
    /// scalar reference bit-exactly across our matryoshka corner-case dims.
    #[test]
    fn test_score_folded_sse_matches_scalar() {
        if !std::is_x86_feature_detected!("ssse3") || !std::is_x86_feature_detected!("sse4.1") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(0xBEEF);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let factors = random_factors(&mut rng, vec_a.len());
            let scalar = score_4bit_internal_folded_scalar(&vec_a, &vec_b, &factors);
            let sse = unsafe { score_4bit_internal_folded_sse(&vec_a, &vec_b, &factors) };
            assert_eq!(
                scalar, sse,
                "folded: scalar {scalar} != sse {sse} at dim {dim}"
            );
        }
    }

    #[test]
    fn test_score_folded_avx2_matches_scalar() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(0xBEEF);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let factors = random_factors(&mut rng, vec_a.len());
            let scalar = score_4bit_internal_folded_scalar(&vec_a, &vec_b, &factors);
            let avx2 = unsafe { score_4bit_internal_folded_avx2(&vec_a, &vec_b, &factors) };
            assert_eq!(
                scalar, avx2,
                "folded: scalar {scalar} != avx2 {avx2} at dim {dim}"
            );
        }
    }

    /// The decode for the matrix operands gives the values the kernels multiply.
    #[test]
    fn test_fold_codes_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(0xF01D);
        for &dim in PARITY_DIMS {
            let (_, codes) = random_inputs(&mut rng, dim);
            let factors = random_factors(&mut rng, codes.len());
            for flip in [0, 0x80] {
                let want: Vec<u8> = (0..dim)
                    .map(|i| {
                        let code = (codes[i / 2] >> (4 * (i % 2))) & 0x0F;
                        (fold_q15(codebook_signed_i64(code), factors[i]) as i8).cast_unsigned()
                            ^ flip
                    })
                    .collect();
                let mut got = vec![0; dim];
                fold_codes(&codes, &factors, &mut got, flip);
                assert_eq!(got, want, "fold_codes at dim {dim}, flip {flip:#x}");
            }
        }
    }

    /// Saturation-safety for the folded kernel: every x86 backend at 64K
    /// dims must match the i64 scalar reference under worst-case inputs
    /// (largest-magnitude codebook value and factor).
    #[test]
    fn test_score_folded_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![0; dim];
        let vec_a = pack_codes(&indices, 4);
        let vec_b = pack_codes(&indices, 4);
        let factors: Vec<i16> = vec![i16::MAX; dim];

        let scalar = score_4bit_internal_folded_scalar(&vec_a, &vec_b, &factors);

        unsafe {
            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                let sse = score_4bit_internal_folded_sse(&vec_a, &vec_b, &factors);
                assert_eq!(scalar, sse, "folded score sse disagrees at dim={dim}");
            }
            if std::is_x86_feature_detected!("avx2") {
                let avx2 = score_4bit_internal_folded_avx2(&vec_a, &vec_b, &factors);
                assert_eq!(scalar, avx2, "folded score avx2 disagrees at dim={dim}");
            }
        }
    }

    #[test]
    fn test_score_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![15; dim]; // CODEBOOK_U8[15] = 255 → signed 127
        let vec_a = pack_codes(&indices, 4);
        let vec_b = pack_codes(&indices, 4);

        let scalar = score_4bit_internal_scalar(&vec_a, &vec_b);

        unsafe {
            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                let sse = score_4bit_internal_sse(&vec_a, &vec_b);
                assert_eq!(scalar, sse, "score sse disagrees at dim={dim}");
            }
            if std::is_x86_feature_detected!("avx2") {
                let avx2 = score_4bit_internal_avx2(&vec_a, &vec_b);
                assert_eq!(scalar, avx2, "score avx2 disagrees at dim={dim}");
            }
            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512vnni")
            {
                let vnni = score_4bit_internal_avx512_vnni(&vec_a, &vec_b);
                assert_eq!(scalar, vnni, "score avx512_vnni disagrees at dim={dim}");
            }
        }
    }
}
