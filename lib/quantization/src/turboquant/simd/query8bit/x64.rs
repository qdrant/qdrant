//! x86_64 kernels for the symmetric 8-bit path (`score_8bit_internal`).
//!
//! Codes are stored `u = s + 128`; `u ^ 0x80` is the signed grid value `s`.
//! SSE / AVX2 sign-extend both operands to i16 and use `madd_epi16`
//! (`|s_a · s_b| ≤ 128²`, a pair sum ≤ 32 768 — exact in i32).  AVX-512
//! VNNI multiplies `u_a` against `s_b` with `VPDPBUSD` and subtracts
//! `128 · Σ s_b`, which it gets from a `VPSADBW` of `u_b`.

use core::arch::x86_64::*;

use super::score_8bit_internal_integer;

#[inline]
#[target_feature(enable = "sse2")]
unsafe fn hsum_i32_sse(v: __m128i) -> i32 {
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0x4E));
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0xB1));
    _mm_cvtsi128_si32(v)
}

/// SSE4.1 implementation of [`super::score_8bit_internal`].
///
/// # Safety
/// CPU must support `sse4.1`; `a.len() == b.len()`.
#[target_feature(enable = "sse4.1")]
pub unsafe fn score_8bit_internal_sse(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    const BLOCK: usize = 16;
    unsafe {
        let flip = _mm_set1_epi8(-128);
        let mut acc = [_mm_setzero_si128(); 2];
        let blocks = a.len() / BLOCK;
        for i in 0..blocks {
            let sa = _mm_xor_si128(_mm_loadu_si128(a.as_ptr().add(i * BLOCK).cast()), flip);
            let sb = _mm_xor_si128(_mm_loadu_si128(b.as_ptr().add(i * BLOCK).cast()), flip);
            let lo = _mm_madd_epi16(_mm_cvtepi8_epi16(sa), _mm_cvtepi8_epi16(sb));
            let hi = _mm_madd_epi16(
                _mm_cvtepi8_epi16(_mm_srli_si128(sa, 8)),
                _mm_cvtepi8_epi16(_mm_srli_si128(sb, 8)),
            );
            acc[0] = _mm_add_epi32(acc[0], lo);
            acc[1] = _mm_add_epi32(acc[1], hi);
        }
        let simd = i64::from(hsum_i32_sse(_mm_add_epi32(acc[0], acc[1])));
        let tail = blocks * BLOCK;
        (simd + score_8bit_internal_integer(&a[tail..], &b[tail..])) as f32
    }
}

/// AVX2 implementation of [`super::score_8bit_internal`].
///
/// # Safety
/// CPU must support `avx2`; `a.len() == b.len()`.
#[target_feature(enable = "avx2")]
pub unsafe fn score_8bit_internal_avx2(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    const BLOCK: usize = 32;
    unsafe {
        let flip = _mm256_set1_epi8(-128);
        let mut acc = [_mm256_setzero_si256(); 2];
        let blocks = a.len() / BLOCK;
        for i in 0..blocks {
            let sa = _mm256_xor_si256(_mm256_loadu_si256(a.as_ptr().add(i * BLOCK).cast()), flip);
            let sb = _mm256_xor_si256(_mm256_loadu_si256(b.as_ptr().add(i * BLOCK).cast()), flip);
            let lo = _mm256_madd_epi16(
                _mm256_cvtepi8_epi16(_mm256_castsi256_si128(sa)),
                _mm256_cvtepi8_epi16(_mm256_castsi256_si128(sb)),
            );
            let hi = _mm256_madd_epi16(
                _mm256_cvtepi8_epi16(_mm256_extracti128_si256(sa, 1)),
                _mm256_cvtepi8_epi16(_mm256_extracti128_si256(sb, 1)),
            );
            acc[0] = _mm256_add_epi32(acc[0], lo);
            acc[1] = _mm256_add_epi32(acc[1], hi);
        }
        let sum = _mm256_add_epi32(acc[0], acc[1]);
        let sum = _mm_add_epi32(
            _mm256_castsi256_si128(sum),
            _mm256_extracti128_si256(sum, 1),
        );
        let simd = i64::from(hsum_i32_sse(sum));
        let tail = blocks * BLOCK;
        (simd + score_8bit_internal_integer(&a[tail..], &b[tail..])) as f32
    }
}

/// AVX-512 VNNI implementation of [`super::score_8bit_internal`]: the tail
/// is a masked load, whose zero lanes add nothing to either sum.
///
/// # Safety
/// CPU must support `avx512f`, `avx512bw` and `avx512vnni`;
/// `a.len() == b.len()`.
#[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
pub unsafe fn score_8bit_internal_avx512_vnni(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    const BLOCK: usize = 64;
    unsafe {
        // Σ u_a · s_b in two chains to hide the VPDPBUSD latency, and Σ u_b
        // in 64-bit lanes.
        let mut acc = [_mm512_setzero_si512(); 2];
        let mut sum_b = _mm512_setzero_si512();

        let blocks = a.len() / BLOCK;
        for i in 0..blocks {
            let ua = _mm512_loadu_si512(a.as_ptr().add(i * BLOCK).cast());
            let ub = _mm512_loadu_si512(b.as_ptr().add(i * BLOCK).cast());
            vnni_step(&mut acc[i & 1], &mut sum_b, ua, ub);
        }
        let tail = a.len() % BLOCK;
        if tail > 0 {
            let mask: __mmask64 = (1 << tail) - 1;
            let offset = blocks * BLOCK;
            let ua = _mm512_maskz_loadu_epi8(mask, a.as_ptr().add(offset).cast());
            let ub = _mm512_maskz_loadu_epi8(mask, b.as_ptr().add(offset).cast());
            // Zero lanes flip to s_b = −128 but meet u_a = 0.
            vnni_step(&mut acc[0], &mut sum_b, ua, ub);
        }

        let dot = i64::from(_mm512_reduce_add_epi32(_mm512_add_epi32(acc[0], acc[1])));
        let sum_ub = _mm512_reduce_add_epi64(sum_b);
        // Σ s_b = Σ u_b − 128 · n;  Σ s_a s_b = Σ u_a s_b − 128 · Σ s_b.
        let sum_sb = sum_ub - 128 * a.len() as i64;
        (dot - 128 * sum_sb) as f32
    }
}

/// One block of [`score_8bit_internal_avx512_vnni`].
#[inline]
#[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
unsafe fn vnni_step(acc: &mut __m512i, sum_b: &mut __m512i, ua: __m512i, ub: __m512i) {
    let sb = _mm512_xor_si512(ub, _mm512_set1_epi8(-128));
    *acc = _mm512_dpbusd_epi32(*acc, ua, sb);
    *sum_b = _mm512_add_epi64(*sum_b, _mm512_sad_epu8(ub, _mm512_setzero_si512()));
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::prelude::StdRng;

    use super::*;
    use crate::turboquant::simd::shared::random_bytes;

    #[test]
    fn test_kernels_match_scalar() {
        let mut rng = StdRng::seed_from_u64(5);
        let has_vnni = std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512bw")
            && std::is_x86_feature_detected!("avx512vnni");
        let dims = (0..=130).chain([255, 256, 257, 384, 1000, 1536, 4096, 65_536]);
        for dim in dims {
            let a = random_bytes(&mut rng, dim);
            let b = random_bytes(&mut rng, dim);
            let expected = score_8bit_internal_integer(&a, &b) as f32;
            unsafe {
                if std::is_x86_feature_detected!("sse4.1") {
                    assert_eq!(score_8bit_internal_sse(&a, &b), expected, "sse dim={dim}");
                }
                if std::is_x86_feature_detected!("avx2") {
                    assert_eq!(score_8bit_internal_avx2(&a, &b), expected, "avx2 dim={dim}");
                }
                if has_vnni {
                    let vnni = score_8bit_internal_avx512_vnni(&a, &b);
                    assert_eq!(vnni, expected, "vnni dim={dim}");
                }
            }
        }
    }
}
