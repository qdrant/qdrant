//! x86_64 popcount paths for [`super::score_1bit_internal`].
//!
//! Three backends, picked at runtime by the dispatcher in `mod.rs`:
//!
//! * **SSE4.1 / SSSE3** — `pshufb`-based nibble-lookup popcount (Muła): split
//!   each byte into its two nibbles, `pshufb` a 16-entry popcount table,
//!   sum.  `psadbw` against zero horizontally sums 8 bytes into a u16 lane,
//!   which we accumulate into a u64 pair.
//!
//! * **AVX2** — same Muła trick on 32-byte YMM registers.
//!
//! * **AVX-512 VPOPCNTDQ** — `vpopcntq` in hardware: 8 × u64 popcounts per
//!   instruction, summed into a 512-bit u64 accumulator and reduced with
//!   `_mm512_reduce_add_epi64`.
//!
//! All three use a u64 accumulator so no intermediate can saturate at any
//! reasonable vector size (see `test_score_*_overflow_safety_16k`).

/// 16-byte popcount-of-nibble lookup table.  Index = nibble value,
/// value = number of 1-bits.  Broadcast to YMM/ZMM as needed.
const NIBBLE_POPCNT: [i8; 16] = [0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4];

/// SSE4.1 + SSSE3 implementation of [`super::score_1bit_internal`].
///
/// # Safety
/// CPU must support `ssse3` and `sse4.1`.
#[target_feature(enable = "sse4.1,ssse3")]
pub unsafe fn score_1bit_internal_sse(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_1bit_internal_sse: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let lookup = _mm_loadu_si128(NIBBLE_POPCNT.as_ptr().cast::<__m128i>());
        let low_mask = _mm_set1_epi8(0x0F);
        let zero = _mm_setzero_si128();
        let mut acc = _mm_setzero_si128();

        let chunks = a.len() / 16;
        for i in 0..chunks {
            let va = _mm_loadu_si128(a.as_ptr().add(i * 16).cast::<__m128i>());
            let vb = _mm_loadu_si128(b.as_ptr().add(i * 16).cast::<__m128i>());
            let x = _mm_xor_si128(va, vb);

            let lo = _mm_and_si128(x, low_mask);
            let hi = _mm_and_si128(_mm_srli_epi16(x, 4), low_mask);
            let cnt_lo = _mm_shuffle_epi8(lookup, lo);
            let cnt_hi = _mm_shuffle_epi8(lookup, hi);
            // Per-byte popcount ≤ 8; two halves summed ≤ 16 per u8 — still
            // fits u8 so `_mm_add_epi8` is safe.
            let cnt = _mm_add_epi8(cnt_lo, cnt_hi);

            // `psadbw(cnt, 0)` horizontally sums 8 bytes into each u64 lane
            // (max 8 · 16 = 128 per lane per chunk — zero overflow risk).
            let sum64 = _mm_sad_epu8(cnt, zero);
            acc = _mm_add_epi64(acc, sum64);
        }

        let lo = _mm_cvtsi128_si64(acc) as u64;
        let hi = _mm_cvtsi128_si64(_mm_unpackhi_epi64(acc, acc)) as u64;
        let mut popcnt = lo + hi;

        let tail_start = chunks * 16;
        for i in tail_start..a.len() {
            popcnt += u64::from((a[i] ^ b[i]).count_ones());
        }

        super::popcount_to_score(a.len(), popcnt)
    }
}

/// AVX2 implementation of [`super::score_1bit_internal`].
///
/// # Safety
/// CPU must support `avx2`.
#[target_feature(enable = "avx2")]
pub unsafe fn score_1bit_internal_avx2(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_1bit_internal_avx2: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        // Broadcast the 16-byte lookup into both 128-bit halves — pshufb
        // operates per-lane, so each half needs its own copy of the table.
        let lookup_half = _mm_loadu_si128(NIBBLE_POPCNT.as_ptr().cast::<__m128i>());
        let lookup = _mm256_set_m128i(lookup_half, lookup_half);
        let low_mask = _mm256_set1_epi8(0x0F);
        let zero = _mm256_setzero_si256();
        let mut acc = _mm256_setzero_si256();

        let chunks = a.len() / 32;
        for i in 0..chunks {
            let va = _mm256_loadu_si256(a.as_ptr().add(i * 32).cast::<__m256i>());
            let vb = _mm256_loadu_si256(b.as_ptr().add(i * 32).cast::<__m256i>());
            let x = _mm256_xor_si256(va, vb);

            let lo = _mm256_and_si256(x, low_mask);
            let hi = _mm256_and_si256(_mm256_srli_epi16(x, 4), low_mask);
            let cnt_lo = _mm256_shuffle_epi8(lookup, lo);
            let cnt_hi = _mm256_shuffle_epi8(lookup, hi);
            let cnt = _mm256_add_epi8(cnt_lo, cnt_hi);

            let sum64 = _mm256_sad_epu8(cnt, zero);
            acc = _mm256_add_epi64(acc, sum64);
        }

        // Reduce 4 × u64 → scalar u64 via two 128-bit halves.
        let lo128 = _mm256_castsi256_si128(acc);
        let hi128 = _mm256_extracti128_si256(acc, 1);
        let sum128 = _mm_add_epi64(lo128, hi128);
        let lo = _mm_cvtsi128_si64(sum128) as u64;
        let hi = _mm_cvtsi128_si64(_mm_unpackhi_epi64(sum128, sum128)) as u64;
        let mut popcnt = lo + hi;

        let tail_start = chunks * 32;
        for i in tail_start..a.len() {
            popcnt += u64::from((a[i] ^ b[i]).count_ones());
        }

        super::popcount_to_score(a.len(), popcnt)
    }
}

/// AVX-512 VPOPCNTDQ implementation of [`super::score_1bit_internal`].
///
/// # Safety
/// CPU must support `avx512f` and `avx512vpopcntdq`.
#[target_feature(enable = "avx512f,avx512vpopcntdq")]
pub unsafe fn score_1bit_internal_avx512_vpopcntdq(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_1bit_internal_avx512_vpopcntdq: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let mut acc = _mm512_setzero_si512();
        let chunks = a.len() / 64;
        for i in 0..chunks {
            let va = _mm512_loadu_si512(a.as_ptr().add(i * 64).cast::<__m512i>());
            let vb = _mm512_loadu_si512(b.as_ptr().add(i * 64).cast::<__m512i>());
            let x = _mm512_xor_si512(va, vb);
            let cnt = _mm512_popcnt_epi64(x);
            acc = _mm512_add_epi64(acc, cnt);
        }
        let mut popcnt = _mm512_reduce_add_epi64(acc) as u64;

        let tail_start = chunks * 64;
        for i in tail_start..a.len() {
            popcnt += u64::from((a[i] ^ b[i]).count_ones());
        }

        super::popcount_to_score(a.len(), popcnt)
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::score_1bit_internal_scalar;
    use super::super::shared::{PARITY_BYTE_LENS, random_bytes};
    use super::*;

    #[test]
    fn test_score_sse_matches_scalar() {
        if !std::is_x86_feature_detected!("ssse3") || !std::is_x86_feature_detected!("sse4.1") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &byte_len in PARITY_BYTE_LENS {
            let a = random_bytes(&mut rng, byte_len);
            let b = random_bytes(&mut rng, byte_len);
            let scalar = score_1bit_internal_scalar(&a, &b);
            let got = unsafe { score_1bit_internal_sse(&a, &b) };
            assert_eq!(
                scalar.to_bits(),
                got.to_bits(),
                "sse mismatch at byte_len={byte_len}",
            );
        }
    }

    #[test]
    fn test_score_avx2_matches_scalar() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &byte_len in PARITY_BYTE_LENS {
            let a = random_bytes(&mut rng, byte_len);
            let b = random_bytes(&mut rng, byte_len);
            let scalar = score_1bit_internal_scalar(&a, &b);
            let got = unsafe { score_1bit_internal_avx2(&a, &b) };
            assert_eq!(
                scalar.to_bits(),
                got.to_bits(),
                "avx2 mismatch at byte_len={byte_len}",
            );
        }
    }

    #[test]
    fn test_score_avx512_vpopcntdq_matches_scalar() {
        if !(std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512vpopcntdq"))
        {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &byte_len in PARITY_BYTE_LENS {
            let a = random_bytes(&mut rng, byte_len);
            let b = random_bytes(&mut rng, byte_len);
            let scalar = score_1bit_internal_scalar(&a, &b);
            let got = unsafe { score_1bit_internal_avx512_vpopcntdq(&a, &b) };
            assert_eq!(
                scalar.to_bits(),
                got.to_bits(),
                "avx512 mismatch at byte_len={byte_len}",
            );
        }
    }

    /// Overflow safety at 16 KiB (131 072 bits) with `a = all 0xFF`,
    /// `b = all 0x00` — every bit disagrees, so `popcnt = n_bits`.  Each
    /// SIMD path must match scalar (u64 throughout) exactly; a mismatch
    /// would mean an intermediate `u16`/`u32`/u64 lane overflowed.
    #[test]
    fn test_score_overflow_safety_16k() {
        let byte_len = 16 * 1024;
        let a = vec![0xFF_u8; byte_len];
        let b = vec![0x00_u8; byte_len];
        let scalar = score_1bit_internal_scalar(&a, &b);

        unsafe {
            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                let sse = score_1bit_internal_sse(&a, &b);
                assert_eq!(scalar.to_bits(), sse.to_bits(), "sse overflow at 16k");
            }
            if std::is_x86_feature_detected!("avx2") {
                let avx2 = score_1bit_internal_avx2(&a, &b);
                assert_eq!(scalar.to_bits(), avx2.to_bits(), "avx2 overflow at 16k");
            }
            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512vpopcntdq")
            {
                let avx512 = score_1bit_internal_avx512_vpopcntdq(&a, &b);
                assert_eq!(scalar.to_bits(), avx512.to_bits(), "avx512 overflow at 16k");
            }
        }
    }
}
