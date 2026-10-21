//! x86_64 kernels for the symmetric 2-bit paths (`score_2bit_internal*`).
//!
//! The codebook is the unsigned `CODEBOOK_U8`; the 2-bit → centroid unpack
//! uses a pair-table trick analogous to [`super::arm`]:
//!
//!   * `PAIR_TABLE_EVEN_U8[nibble] = CODEBOOK_U8[nibble & 0b11]`
//!   * `PAIR_TABLE_ODD_U8[nibble]  = CODEBOOK_U8[(nibble >> 2) & 0b11]`
//!
//! Per 4 packed data bytes: split low/high nibbles, `pshufb` both pair tables
//! with the nibble indices, interleave via `punpcklbw` → 16 centroid bytes
//! in natural dim order.

use super::{CODEBOOK_U8, centroid_dot};

/// `PAIR_TABLE_EVEN_U8[nibble]` = `CODEBOOK_U8[nibble & 0b11]`.
const PAIR_TABLE_EVEN_U8: [u8; 16] = {
    let mut tbl = [0_u8; 16];
    let mut k = 0;
    while k < 16 {
        tbl[k] = CODEBOOK_U8[k & 0b11];
        k += 1;
    }
    tbl
};

/// `PAIR_TABLE_ODD_U8[nibble]` = `CODEBOOK_U8[(nibble >> 2) & 0b11]`.
const PAIR_TABLE_ODD_U8: [u8; 16] = {
    let mut tbl = [0_u8; 16];
    let mut k = 0;
    while k < 16 {
        tbl[k] = CODEBOOK_U8[(k >> 2) & 0b11];
        k += 1;
    }
    tbl
};

/// Unpack 4 packed data bytes into a natural-order `__m128i` of 16 centroid
/// `u8` values (one per code).
#[inline]
#[target_feature(enable = "sse4.1,ssse3")]
unsafe fn unpack_16_codes_sse(bytes4: *const u8) -> core::arch::x86_64::__m128i {
    use core::arch::x86_64::*;
    unsafe {
        // Load 4 bytes into low 32 bits.
        let data = _mm_cvtsi32_si128(bytes4.cast::<i32>().read_unaligned());

        let low_mask = _mm_set1_epi8(0x0F);
        let lo_nibs = _mm_and_si128(data, low_mask);
        let hi_nibs = _mm_and_si128(_mm_srli_epi16(data, 4), low_mask);

        // Interleave low/high nibbles of the first 4 lanes:
        //   [lo(b0), hi(b0), lo(b1), hi(b1), lo(b2), hi(b2), lo(b3), hi(b3), …]
        let pair_indices = _mm_unpacklo_epi8(lo_nibs, hi_nibs);

        let t_even = _mm_loadu_si128(PAIR_TABLE_EVEN_U8.as_ptr().cast::<__m128i>());
        let t_odd = _mm_loadu_si128(PAIR_TABLE_ODD_U8.as_ptr().cast::<__m128i>());
        let c_even = _mm_shuffle_epi8(t_even, pair_indices);
        let c_odd = _mm_shuffle_epi8(t_odd, pair_indices);

        // Natural dim order: [c0, c1, c2, c3, c4, ..., c15].
        _mm_unpacklo_epi8(c_even, c_odd)
    }
}

#[target_feature(enable = "sse2")]
unsafe fn hsum_i32_sse(v: core::arch::x86_64::__m128i) -> i32 {
    use core::arch::x86_64::*;
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0x4E));
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0xB1));
    _mm_cvtsi128_si32(v)
}

// ------------------------------------------------------------------
// score_2bit_internal — both operands signed, same widen-to-i16
// pattern as query4bit's score_4bit_internal.  XOR 0x80 converts the
// CODEBOOK_U8 pair-table bytes back to signed i8 in-register.
// ------------------------------------------------------------------

/// SSE4.1 + SSSE3 implementation of [`super::score_2bit_internal`].
///
/// # Safety
/// CPU must support `ssse3` and `sse4.1`.
#[target_feature(enable = "sse4.1,ssse3")]
pub unsafe fn score_2bit_internal_sse(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_2bit_internal_sse: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let shift = _mm_set1_epi8(-128_i8);
        let mut acc = _mm_setzero_si128();
        let n_full = a.len() / 4;
        for i in 0..n_full {
            let c_a_u8 = unpack_16_codes_sse(a.as_ptr().add(i * 4));
            let c_b_u8 = unpack_16_codes_sse(b.as_ptr().add(i * 4));
            let c_a = _mm_xor_si128(c_a_u8, shift);
            let c_b = _mm_xor_si128(c_b_u8, shift);

            let c_a_lo = _mm_cvtepi8_epi16(c_a);
            let c_a_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_a, 8));
            let c_b_lo = _mm_cvtepi8_epi16(c_b);
            let c_b_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_b, 8));

            let prod_lo = _mm_madd_epi16(c_a_lo, c_b_lo);
            let prod_hi = _mm_madd_epi16(c_a_hi, c_b_hi);
            acc = _mm_add_epi32(acc, _mm_add_epi32(prod_lo, prod_hi));
        }
        let simd_bytes = n_full * 4;
        let acc_i64 = i64::from(hsum_i32_sse(acc))
            + super::score_2bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        centroid_dot(acc_i64 as f32)
    }
}

/// AVX2 implementation of [`super::score_2bit_internal`].
///
/// # Safety
/// CPU must support `avx2`, `ssse3`, `sse4.1`.
#[target_feature(enable = "avx2,sse4.1,ssse3")]
pub unsafe fn score_2bit_internal_avx2(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(a.len(), b.len());

    unsafe {
        let shift256 = _mm256_set1_epi8(-128_i8);
        let mut acc = _mm256_setzero_si256();

        let n_chunks = a.len() / 4;
        let n_pairs = n_chunks / 2;

        for p in 0..n_pairs {
            let off_a0 = 4 * (2 * p);
            let off_a1 = 4 * (2 * p + 1);
            let c_a = _mm256_set_m128i(
                unpack_16_codes_sse(a.as_ptr().add(off_a1)),
                unpack_16_codes_sse(a.as_ptr().add(off_a0)),
            );
            let c_b = _mm256_set_m128i(
                unpack_16_codes_sse(b.as_ptr().add(off_a1)),
                unpack_16_codes_sse(b.as_ptr().add(off_a0)),
            );
            let c_a = _mm256_xor_si256(c_a, shift256);
            let c_b = _mm256_xor_si256(c_b, shift256);

            let c_a_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_a));
            let c_a_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_a, 1));
            let c_b_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_b));
            let c_b_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_b, 1));

            let prod_lo = _mm256_madd_epi16(c_a_lo, c_b_lo);
            let prod_hi = _mm256_madd_epi16(c_a_hi, c_b_hi);
            acc = _mm256_add_epi32(acc, _mm256_add_epi32(prod_lo, prod_hi));
        }

        let mut acc_sse = _mm_add_epi32(
            _mm256_castsi256_si128(acc),
            _mm256_extracti128_si256(acc, 1),
        );
        if n_chunks % 2 == 1 {
            let off = 4 * (2 * n_pairs);
            let shift = _mm_set1_epi8(-128_i8);
            let c_a = _mm_xor_si128(unpack_16_codes_sse(a.as_ptr().add(off)), shift);
            let c_b = _mm_xor_si128(unpack_16_codes_sse(b.as_ptr().add(off)), shift);
            let c_a_lo = _mm_cvtepi8_epi16(c_a);
            let c_a_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_a, 8));
            let c_b_lo = _mm_cvtepi8_epi16(c_b);
            let c_b_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_b, 8));
            acc_sse = _mm_add_epi32(acc_sse, _mm_madd_epi16(c_a_lo, c_b_lo));
            acc_sse = _mm_add_epi32(acc_sse, _mm_madd_epi16(c_a_hi, c_b_hi));
        }
        let simd_bytes = n_chunks * 4;
        let acc_i64 = i64::from(hsum_i32_sse(acc_sse))
            + super::score_2bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        centroid_dot(acc_i64 as f32)
    }
}

// ------------------------------------------------------------------
// score_2bit_internal_folded — TQ+ symmetric path. Each side's signed
// codebook value is scaled by its coordinate's Q15 factor with
// `mulhrs_epi16`, the same rounding as `fold_q15`, which keeps it within
// [−128, 127]; `madd_epi16` then pair-sums the products into i32 lanes:
//   per product:   |q_a · q_b| ≤ 128 · 128 = 16 384
//   total:         ≤ 2¹⁶ · 16 384 = 2³⁰ at `FOLDED_MAX_DIM` ✓
// ------------------------------------------------------------------

/// The folded products of the 16 codes of `a` and `b` at 4 bytes each, with the 16 `factors`,
/// pair-summed into 4 i32 lanes.
#[inline]
#[target_feature(enable = "sse4.1,ssse3")]
unsafe fn folded_chunk_sse(
    a: *const u8,
    b: *const u8,
    factors: *const i16,
) -> core::arch::x86_64::__m128i {
    use core::arch::x86_64::*;
    unsafe {
        let shift = _mm_set1_epi8(-128_i8);
        let c_a = _mm_xor_si128(unpack_16_codes_sse(a), shift);
        let c_b = _mm_xor_si128(unpack_16_codes_sse(b), shift);
        let f_lo = _mm_loadu_si128(factors.cast::<__m128i>());
        let f_hi = _mm_loadu_si128(factors.add(8).cast::<__m128i>());

        let q_a_lo = _mm_mulhrs_epi16(_mm_cvtepi8_epi16(c_a), f_lo);
        let q_a_hi = _mm_mulhrs_epi16(_mm_cvtepi8_epi16(_mm_srli_si128(c_a, 8)), f_hi);
        let q_b_lo = _mm_mulhrs_epi16(_mm_cvtepi8_epi16(c_b), f_lo);
        let q_b_hi = _mm_mulhrs_epi16(_mm_cvtepi8_epi16(_mm_srli_si128(c_b, 8)), f_hi);
        _mm_add_epi32(
            _mm_madd_epi16(q_a_lo, q_b_lo),
            _mm_madd_epi16(q_a_hi, q_b_hi),
        )
    }
}

/// SSE4.1 + SSSE3 implementation of [`super::score_2bit_internal_folded`].
///
/// # Safety
/// CPU must support `ssse3` and `sse4.1`.
#[target_feature(enable = "sse4.1,ssse3")]
pub unsafe fn score_2bit_internal_folded_sse(a: &[u8], b: &[u8], factors: &[i16]) -> i64 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_2bit_internal_folded_sse: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    assert_eq!(
        factors.len(),
        4 * a.len(),
        "score_2bit_internal_folded_sse: factors length {} != 4 · a.len() {}",
        factors.len(),
        4 * a.len(),
    );

    unsafe {
        let mut acc = _mm_setzero_si128();

        // 4 bytes from each source = 16 coords per iter.
        let n_full = a.len() / 4;
        for i in 0..n_full {
            let chunk = folded_chunk_sse(
                a.as_ptr().add(4 * i),
                b.as_ptr().add(4 * i),
                factors.as_ptr().add(16 * i),
            );
            acc = _mm_add_epi32(acc, chunk);
        }

        let simd_bytes = n_full * 4;
        i64::from(hsum_i32_sse(acc))
            + super::score_2bit_internal_folded_scalar(
                &a[simd_bytes..],
                &b[simd_bytes..],
                &factors[4 * simd_bytes..],
            )
    }
}

/// AVX2 implementation of [`super::score_2bit_internal_folded`]. 32 coords
/// per iteration (two 16-coord chunks through one ymm `mulhrs` + `madd`).
///
/// # Safety
/// CPU must support `avx2`, `ssse3`, `sse4.1`.
#[target_feature(enable = "avx2,sse4.1,ssse3")]
pub unsafe fn score_2bit_internal_folded_avx2(a: &[u8], b: &[u8], factors: &[i16]) -> i64 {
    use core::arch::x86_64::*;

    assert_eq!(a.len(), b.len());
    assert_eq!(factors.len(), 4 * a.len());

    unsafe {
        let shift256 = _mm256_set1_epi8(-128_i8);
        let mut acc = _mm256_setzero_si256();

        let n_chunks = a.len() / 4;
        let n_pairs = n_chunks / 2;

        // Process pairs of 4-byte chunks (32 coords) per iteration.
        for p in 0..n_pairs {
            let off_0 = 4 * (2 * p);
            let off_1 = 4 * (2 * p + 1);
            let c_a = _mm256_set_m128i(
                unpack_16_codes_sse(a.as_ptr().add(off_1)),
                unpack_16_codes_sse(a.as_ptr().add(off_0)),
            );
            let c_b = _mm256_set_m128i(
                unpack_16_codes_sse(b.as_ptr().add(off_1)),
                unpack_16_codes_sse(b.as_ptr().add(off_0)),
            );
            let c_a = _mm256_xor_si256(c_a, shift256);
            let c_b = _mm256_xor_si256(c_b, shift256);

            // Factors are in coord order: (off_0, off_1) span `[16·(2p)..16·(2p+2)]`.
            let f_lo = _mm256_loadu_si256(factors.as_ptr().add(32 * p).cast::<__m256i>());
            let f_hi = _mm256_loadu_si256(factors.as_ptr().add(32 * p + 16).cast::<__m256i>());

            let q_a_lo =
                _mm256_mulhrs_epi16(_mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_a)), f_lo);
            let q_a_hi =
                _mm256_mulhrs_epi16(_mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_a, 1)), f_hi);
            let q_b_lo =
                _mm256_mulhrs_epi16(_mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_b)), f_lo);
            let q_b_hi =
                _mm256_mulhrs_epi16(_mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_b, 1)), f_hi);

            let products = _mm256_add_epi32(
                _mm256_madd_epi16(q_a_lo, q_b_lo),
                _mm256_madd_epi16(q_a_hi, q_b_hi),
            );
            acc = _mm256_add_epi32(acc, products);
        }

        let mut acc_128 = _mm_add_epi32(
            _mm256_castsi256_si128(acc),
            _mm256_extracti128_si256(acc, 1),
        );

        // Trailing single chunk if `n_chunks` is odd.
        if n_chunks % 2 == 1 {
            let off = 4 * (2 * n_pairs);
            let chunk = folded_chunk_sse(
                a.as_ptr().add(off),
                b.as_ptr().add(off),
                factors.as_ptr().add(16 * (2 * n_pairs)),
            );
            acc_128 = _mm_add_epi32(acc_128, chunk);
        }

        let simd_bytes = n_chunks * 4;
        i64::from(hsum_i32_sse(acc_128))
            + super::score_2bit_internal_folded_scalar(
                &a[simd_bytes..],
                &b[simd_bytes..],
                &factors[4 * simd_bytes..],
            )
    }
}

/// [`super::fold_codes`] for whole groups of 32 codes (8 bytes). Returns the
/// positions done; the values are the ones [`super::score_2bit_internal_folded`]
/// multiplies, XORed with `flip`.
///
/// # Safety
/// CPU must support `avx2`, `ssse3`, `sse4.1`. `codes` must hold `out.len()`
/// codes and `factors` `out.len()` factors.
#[target_feature(enable = "avx2,sse4.1,ssse3")]
pub(super) unsafe fn fold_codes_avx2(
    codes: &[u8],
    factors: &[i16],
    out: &mut [u8],
    flip: u8,
) -> usize {
    use core::arch::x86_64::*;

    let groups = out.len() / 32;
    // SAFETY: group `g` reads 8 bytes of `codes` and 32 factors, and writes 32 positions, all
    // within the slices for `g < groups` by the caller's lengths
    unsafe {
        let shift256 = _mm256_set1_epi8(-128_i8);
        let flip = _mm256_set1_epi8(flip.cast_signed());
        for g in 0..groups {
            let c = _mm256_set_m128i(
                unpack_16_codes_sse(codes.as_ptr().add(8 * g + 4)),
                unpack_16_codes_sse(codes.as_ptr().add(8 * g)),
            );
            let c = _mm256_xor_si256(c, shift256);
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

/// AVX-512 + VNNI implementation — uses `VPDPWSSD` for fused i16×i16 MAC.
///
/// # Safety
/// CPU must support `avx512f`, `avx512bw`, `avx512vnni`, `ssse3`, `sse4.1`.
#[target_feature(enable = "avx512f,avx512bw,avx512vnni,sse4.1,ssse3")]
pub unsafe fn score_2bit_internal_avx512_vnni(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(a.len(), b.len());

    unsafe {
        // Full-width 512-bit VNNI.  Process 4 chunks (64 codes) per iter.
        let shift512 = _mm512_set1_epi8(-128_i8);
        let mut acc = _mm512_setzero_si512();

        let n_chunks = a.len() / 4;
        let n_quads = n_chunks / 4;

        for q in 0..n_quads {
            let base = 4 * q;
            let c_a_0 = unpack_16_codes_sse(a.as_ptr().add(4 * base));
            let c_a_1 = unpack_16_codes_sse(a.as_ptr().add(4 * (base + 1)));
            let c_a_2 = unpack_16_codes_sse(a.as_ptr().add(4 * (base + 2)));
            let c_a_3 = unpack_16_codes_sse(a.as_ptr().add(4 * (base + 3)));
            let c_b_0 = unpack_16_codes_sse(b.as_ptr().add(4 * base));
            let c_b_1 = unpack_16_codes_sse(b.as_ptr().add(4 * (base + 1)));
            let c_b_2 = unpack_16_codes_sse(b.as_ptr().add(4 * (base + 2)));
            let c_b_3 = unpack_16_codes_sse(b.as_ptr().add(4 * (base + 3)));

            let c_a_ab = _mm256_set_m128i(c_a_1, c_a_0);
            let c_a_cd = _mm256_set_m128i(c_a_3, c_a_2);
            let c_a = _mm512_inserti64x4(_mm512_castsi256_si512(c_a_ab), c_a_cd, 1);
            let c_b_ab = _mm256_set_m128i(c_b_1, c_b_0);
            let c_b_cd = _mm256_set_m128i(c_b_3, c_b_2);
            let c_b = _mm512_inserti64x4(_mm512_castsi256_si512(c_b_ab), c_b_cd, 1);

            // Unwind the `+128` shift to recover signed i8.
            let c_a = _mm512_xor_si512(c_a, shift512);
            let c_b = _mm512_xor_si512(c_b, shift512);

            // Widen i8 → i16 in both halves of each ZMM.
            let c_a_lo = _mm512_cvtepi8_epi16(_mm512_castsi512_si256(c_a));
            let c_a_hi = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(c_a, 1));
            let c_b_lo = _mm512_cvtepi8_epi16(_mm512_castsi512_si256(c_b));
            let c_b_hi = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(c_b, 1));

            acc = _mm512_dpwssd_epi32(acc, c_a_lo, c_b_lo);
            acc = _mm512_dpwssd_epi32(acc, c_a_hi, c_b_hi);
        }

        let mut acc_total = i64::from(_mm512_reduce_add_epi32(acc));

        // Tail (0..3 chunks) via SSE `madd_epi16` — no avx512vl needed.
        let tail_start = n_quads * 4;
        let shift_128 = _mm_set1_epi8(-128_i8);
        for p in tail_start..n_chunks {
            let c_a = _mm_xor_si128(unpack_16_codes_sse(a.as_ptr().add(4 * p)), shift_128);
            let c_b = _mm_xor_si128(unpack_16_codes_sse(b.as_ptr().add(4 * p)), shift_128);
            let c_a_lo = _mm_cvtepi8_epi16(c_a);
            let c_a_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_a, 8));
            let c_b_lo = _mm_cvtepi8_epi16(c_b);
            let c_b_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_b, 8));
            let prod = _mm_add_epi32(
                _mm_madd_epi16(c_a_lo, c_b_lo),
                _mm_madd_epi16(c_a_hi, c_b_hi),
            );
            acc_total += i64::from(hsum_i32_sse(prod));
        }

        // Scalar tail for any bytes past the last SIMD chunk (a.len() % 4).
        let simd_bytes = n_chunks * 4;
        acc_total += super::score_2bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        centroid_dot(acc_total as f32)
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::super::shared::pack_codes;
    use super::super::shared::{PARITY_DIMS, random_inputs};
    use super::super::{
        codebook_signed_i64, fold_codes, score_2bit_internal_folded_scalar,
        score_2bit_internal_scalar,
    };
    use super::*;
    use crate::turboquant::simd::fold_q15;

    /// Build deterministic non-negative Q15 factors of length `4 · vec_bytes`
    /// for parity tests of the 2-bit folded kernel.
    fn random_factors(rng: &mut StdRng, vec_bytes: usize) -> Vec<i16> {
        use rand::RngExt;
        (0..4 * vec_bytes)
            .map(|_| rng.random_range(0..=i16::MAX))
            .collect()
    }

    #[test]
    fn test_score_sse_matches_scalar() {
        if !std::is_x86_feature_detected!("ssse3") || !std::is_x86_feature_detected!("sse4.1") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let scalar = score_2bit_internal_scalar(&vec_a, &vec_b);
            let sse = unsafe { score_2bit_internal_sse(&vec_a, &vec_b) };
            assert_eq!(
                scalar.to_bits(),
                sse.to_bits(),
                "score sse mismatch at dim {dim}",
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
            let scalar = score_2bit_internal_scalar(&vec_a, &vec_b);
            let got = unsafe { score_2bit_internal_avx2(&vec_a, &vec_b) };
            assert_eq!(
                scalar.to_bits(),
                got.to_bits(),
                "score avx2 mismatch at dim {dim}",
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
            let scalar = score_2bit_internal_scalar(&vec_a, &vec_b);
            let got = unsafe { score_2bit_internal_avx512_vnni(&vec_a, &vec_b) };
            assert_eq!(
                scalar.to_bits(),
                got.to_bits(),
                "score avx512 mismatch at dim {dim}",
            );
        }
    }

    /// Parity for the 2-bit folded kernel: every x86 backend must match
    /// the scalar reference bit-exactly across our matryoshka corner-case dims.
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
            let scalar = score_2bit_internal_folded_scalar(&vec_a, &vec_b, &factors);
            let sse = unsafe { score_2bit_internal_folded_sse(&vec_a, &vec_b, &factors) };
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
            let scalar = score_2bit_internal_folded_scalar(&vec_a, &vec_b, &factors);
            let avx2 = unsafe { score_2bit_internal_folded_avx2(&vec_a, &vec_b, &factors) };
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
                        let code = (codes[i / 4] >> (2 * (i % 4))) & 0x03;
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

    /// Saturation-safety for the 2-bit folded kernel at 64K dims: every
    /// x86 backend must match the i64 scalar reference under worst-case
    /// inputs (largest-magnitude codebook value and factor).
    #[test]
    fn test_score_folded_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![0; dim];
        let vec_a = pack_codes(&indices, 2);
        let vec_b = pack_codes(&indices, 2);
        let factors: Vec<i16> = vec![i16::MAX; dim];

        let scalar = score_2bit_internal_folded_scalar(&vec_a, &vec_b, &factors);

        unsafe {
            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                let sse = score_2bit_internal_folded_sse(&vec_a, &vec_b, &factors);
                assert_eq!(scalar, sse, "folded score sse disagrees at dim={dim}");
            }
            if std::is_x86_feature_detected!("avx2") {
                let avx2 = score_2bit_internal_folded_avx2(&vec_a, &vec_b, &factors);
                assert_eq!(scalar, avx2, "folded score avx2 disagrees at dim={dim}");
            }
        }
    }

    #[test]
    fn test_score_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![3; dim];
        let vec_a = pack_codes(&indices, 2);
        let vec_b = pack_codes(&indices, 2);
        let scalar = score_2bit_internal_scalar(&vec_a, &vec_b);

        unsafe {
            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                assert_eq!(
                    scalar.to_bits(),
                    score_2bit_internal_sse(&vec_a, &vec_b).to_bits()
                );
            }
            if std::is_x86_feature_detected!("avx2") {
                assert_eq!(
                    scalar.to_bits(),
                    score_2bit_internal_avx2(&vec_a, &vec_b).to_bits()
                );
            }
            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512vnni")
            {
                assert_eq!(
                    scalar.to_bits(),
                    score_2bit_internal_avx512_vnni(&vec_a, &vec_b).to_bits(),
                );
            }
        }
    }
}
