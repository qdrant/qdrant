//! x86_64 kernels for 16-bit scoring (`score_16bit_internal`).
//!
//! `madd_epi16` multiplies the `i16` codes pairwise and adds each pair into
//! an i32 lane.  A pair of full-grid products already takes almost all of
//! the lane (see the module docs), so every pair sum is converted to f32
//! and accumulated there.  The conversion costs about as much as an integer
//! add: the kernel runs within a few percent of an i32 accumulator that would
//! overflow.  Codes past the last full block go through the exact integer
//! kernel (SSE / AVX2) or a masked load (AVX-512).

use core::arch::x86_64::*;

use super::score_16bit_internal_integer;

#[inline]
#[target_feature(enable = "sse")]
unsafe fn hsum_ps_sse(v: __m128) -> f32 {
    let v = _mm_add_ps(v, _mm_movehl_ps(v, v));
    let v = _mm_add_ss(v, _mm_shuffle_ps(v, v, 0x55));
    _mm_cvtss_f32(v)
}

/// SSE4.1 implementation of [`super::score_16bit_internal`].
///
/// # Safety
/// CPU must support `sse4.1`; `a.len() == b.len()`.
#[target_feature(enable = "sse4.1")]
pub unsafe fn score_16bit_internal_sse(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    // 8 codes per register, two independent chains.
    const BLOCK: usize = 16;
    unsafe {
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let mut acc = [_mm_setzero_ps(); 2];
        let blocks = a.len() / BLOCK;
        for i in 0..blocks {
            let va = _mm_loadu_si128(pa.add(i * BLOCK).cast());
            let vb = _mm_loadu_si128(pb.add(i * BLOCK).cast());
            let p = _mm_cvtepi32_ps(_mm_madd_epi16(va, vb));
            acc[i & 1] = _mm_add_ps(acc[i & 1], p);
        }
        let tail = blocks * BLOCK;
        hsum_ps_sse(_mm_add_ps(acc[0], acc[1]))
            + score_16bit_internal_integer(&a[tail..], &b[tail..]) as f32
    }
}

/// AVX2 implementation of [`super::score_16bit_internal`].
///
/// # Safety
/// CPU must support `avx2`; `a.len() == b.len()`.
#[target_feature(enable = "avx2")]
pub unsafe fn score_16bit_internal_avx2(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    // 16 codes per register; the main loop runs 4 independent chains.
    const BLOCK: usize = 32;
    unsafe {
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let len = a.len();
        let mut acc = [_mm256_setzero_ps(); 4];
        let mut off = 0;
        while off + 4 * BLOCK <= len {
            for (k, acc) in acc.iter_mut().enumerate() {
                let va = _mm256_loadu_si256(pa.add(off + k * BLOCK).cast());
                let vb = _mm256_loadu_si256(pb.add(off + k * BLOCK).cast());
                *acc = _mm256_add_ps(*acc, _mm256_cvtepi32_ps(_mm256_madd_epi16(va, vb)));
            }
            off += 4 * BLOCK;
        }
        while off + BLOCK <= len {
            let va = _mm256_loadu_si256(pa.add(off).cast());
            let vb = _mm256_loadu_si256(pb.add(off).cast());
            acc[0] = _mm256_add_ps(acc[0], _mm256_cvtepi32_ps(_mm256_madd_epi16(va, vb)));
            off += BLOCK;
        }
        let sum = _mm256_add_ps(_mm256_add_ps(acc[0], acc[1]), _mm256_add_ps(acc[2], acc[3]));
        let sum = _mm_add_ps(_mm256_castps256_ps128(sum), _mm256_extractf128_ps(sum, 1));
        hsum_ps_sse(sum) + score_16bit_internal_integer(&a[off..], &b[off..]) as f32
    }
}

/// AVX-512BW implementation of [`super::score_16bit_internal`]: two chains
/// of 32 codes, then one masked block for the tail, whose zero lanes add
/// nothing.
///
/// # Safety
/// CPU must support `avx512f` and `avx512bw`; `a.len() == b.len()`.
#[target_feature(enable = "avx512f,avx512bw")]
pub unsafe fn score_16bit_internal_avx512(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    const BLOCK: usize = 64;
    unsafe {
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let len = a.len();
        let mut acc = [_mm512_setzero_ps(); 2];
        let mut off = 0;
        while off + 2 * BLOCK <= len {
            for (k, acc) in acc.iter_mut().enumerate() {
                let va = _mm512_loadu_si512(pa.add(off + k * BLOCK).cast());
                let vb = _mm512_loadu_si512(pb.add(off + k * BLOCK).cast());
                *acc = _mm512_add_ps(*acc, _mm512_cvtepi32_ps(_mm512_madd_epi16(va, vb)));
            }
            off += 2 * BLOCK;
        }
        if off < len {
            // At most two blocks left: one full, one masked.
            if off + BLOCK <= len {
                let va = _mm512_loadu_si512(pa.add(off).cast());
                let vb = _mm512_loadu_si512(pb.add(off).cast());
                acc[0] = _mm512_add_ps(acc[0], _mm512_cvtepi32_ps(_mm512_madd_epi16(va, vb)));
                off += BLOCK;
            }
            let codes = (len - off) / 2;
            if codes > 0 {
                let mask: __mmask32 = (1u32 << codes) - 1;
                let va = _mm512_maskz_loadu_epi16(mask, pa.add(off).cast());
                let vb = _mm512_maskz_loadu_epi16(mask, pb.add(off).cast());
                acc[1] = _mm512_add_ps(acc[1], _mm512_cvtepi32_ps(_mm512_madd_epi16(va, vb)));
            }
        }
        _mm512_reduce_add_ps(_mm512_add_ps(acc[0], acc[1]))
    }
}
