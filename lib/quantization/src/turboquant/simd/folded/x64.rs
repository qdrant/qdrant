//! x86_64 kernels of [`super`]: AVX-512 (BW and VNNI) and AVX2. Each reads the whole blocks of
//! its codes in [block order](super#block-order); the callers add the dimensions after them.
//!
//! The folded values stay within ±128 (`|c| ≤ 128`, `D' ≤ 1` in Q15), so a product is at most
//! 2¹⁴, and the sum of all `FOLDED_MAX_DIM` = 2¹⁶ of them stays below 2³¹: neither the i32
//! lanes nor the i32 reductions overflow.

use core::arch::x86_64::*;

use super::{BLOCK_BYTES, BlockedFactors};

/// 16-bit lanes of a 512-bit vector.
const LANES_512: usize = 32;
/// 16-bit lanes of a 256-bit vector.
const LANES_256: usize = 16;

// ---- AVX-512 ----

#[inline]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn load_512(values: &[i16], position: usize) -> __m512i {
    debug_assert!(position + LANES_512 <= values.len());
    unsafe { _mm512_loadu_si512(values.as_ptr().add(position).cast()) }
}

/// The 32 bytes of block `block` of `codes`, zero-extended to 16-bit lanes.
#[inline]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn block_words(codes: &[u8], block: usize) -> __m512i {
    debug_assert!((block + 1) * BLOCK_BYTES <= codes.len());
    unsafe {
        let bytes = _mm256_loadu_si256(codes.as_ptr().add(block * BLOCK_BYTES).cast());
        _mm512_cvtepu8_epi16(bytes)
    }
}

/// Signed codebook values of the 4-bit codes of one block: the low nibbles, then the high ones.
#[inline]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn unpack_4bit_avx512(codebook: __m512i, codes: &[u8], block: usize) -> [__m512i; 2] {
    unsafe {
        let words = block_words(codes, block);
        let low = _mm512_and_si512(words, _mm512_set1_epi16(0x0F));
        let high = _mm512_srli_epi16(words, 4);
        [
            _mm512_permutexvar_epi16(low, codebook),
            _mm512_permutexvar_epi16(high, codebook),
        ]
    }
}

/// Signed codebook values of the 2-bit codes of one block, by bit offset.
#[inline]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn unpack_2bit_avx512(codebook: __m512i, codes: &[u8], block: usize) -> [__m512i; 4] {
    unsafe {
        let words = block_words(codes, block);
        let mask = _mm512_set1_epi16(0x03);
        [
            _mm512_permutexvar_epi16(_mm512_and_si512(words, mask), codebook),
            _mm512_permutexvar_epi16(
                _mm512_and_si512(_mm512_srli_epi16(words, 2), mask),
                codebook,
            ),
            _mm512_permutexvar_epi16(
                _mm512_and_si512(_mm512_srli_epi16(words, 4), mask),
                codebook,
            ),
            _mm512_permutexvar_epi16(_mm512_srli_epi16(words, 6), codebook),
        ]
    }
}

#[inline]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn codebook_512(factors: &BlockedFactors) -> __m512i {
    unsafe { _mm512_loadu_si512(factors.codebook_words.as_ptr().cast()) }
}

#[inline]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn reduce_512(accumulators: &[__m512i]) -> i64 {
    let mut sum = _mm512_setzero_si512();
    for &acc in accumulators {
        sum = _mm512_add_epi32(sum, acc);
    }
    i64::from(_mm512_reduce_add_epi32(sum))
}

/// Folds the whole blocks of 4-bit `codes` into `values`.
///
/// # Safety
///
/// The CPU must support AVX-512 BW; `codes`, `values` and the factors must cover the factors'
/// blocks.
#[target_feature(enable = "avx512f,avx512bw")]
pub(super) unsafe fn fold_4bit_avx512(factors: &BlockedFactors, codes: &[u8], values: &mut [i16]) {
    unsafe {
        let codebook = codebook_512(factors);
        for block in 0..factors.layout.blocks() {
            let codes = unpack_4bit_avx512(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (2 * block + part) * LANES_512;
                let folded = _mm512_mulhrs_epi16(code, load_512(&factors.factors, position));
                _mm512_storeu_si512(values.as_mut_ptr().add(position).cast(), folded);
            }
        }
    }
}

/// Folds the whole blocks of 2-bit `codes` into `values`.
///
/// # Safety
///
/// As [`fold_4bit_avx512`].
#[target_feature(enable = "avx512f,avx512bw")]
pub(super) unsafe fn fold_2bit_avx512(factors: &BlockedFactors, codes: &[u8], values: &mut [i16]) {
    unsafe {
        let codebook = codebook_512(factors);
        for block in 0..factors.layout.blocks() {
            let codes = unpack_2bit_avx512(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (4 * block + part) * LANES_512;
                let folded = _mm512_mulhrs_epi16(code, load_512(&factors.factors, position));
                _mm512_storeu_si512(values.as_mut_ptr().add(position).cast(), folded);
            }
        }
    }
}

/// `Σ query · q(codes)` over the whole blocks of 4-bit `codes`, one accumulator per half of a
/// block, so that consecutive `vpdpwssd` don't wait for each other.
///
/// # Safety
///
/// The CPU must support AVX-512 BW and VNNI; `codes`, `query` and the factors must cover the
/// factors' blocks.
#[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
pub(super) unsafe fn score_4bit_avx512(
    query: &[i16],
    factors: &BlockedFactors,
    codes: &[u8],
) -> i64 {
    unsafe {
        let codebook = codebook_512(factors);
        let mut acc = [_mm512_setzero_si512(); 2];
        for block in 0..factors.layout.blocks() {
            let codes = unpack_4bit_avx512(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (2 * block + part) * LANES_512;
                let folded = _mm512_mulhrs_epi16(code, load_512(&factors.factors, position));
                acc[part] = _mm512_dpwssd_epi32(acc[part], load_512(query, position), folded);
            }
        }
        reduce_512(&acc)
    }
}

/// `Σ query · q(codes)` over the whole blocks of 2-bit `codes`, one accumulator per bit offset.
///
/// # Safety
///
/// As [`score_4bit_avx512`].
#[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
pub(super) unsafe fn score_2bit_avx512(
    query: &[i16],
    factors: &BlockedFactors,
    codes: &[u8],
) -> i64 {
    unsafe {
        let codebook = codebook_512(factors);
        let mut acc = [_mm512_setzero_si512(); 4];
        for block in 0..factors.layout.blocks() {
            let codes = unpack_2bit_avx512(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (4 * block + part) * LANES_512;
                let folded = _mm512_mulhrs_epi16(code, load_512(&factors.factors, position));
                acc[part] = _mm512_dpwssd_epi32(acc[part], load_512(query, position), folded);
            }
        }
        reduce_512(&acc)
    }
}

/// `Σ q(a) · q(b)` over the whole blocks of 4-bit `a` and `b`, both folded per pair.
///
/// # Safety
///
/// The CPU must support AVX-512 BW and VNNI; `a`, `b` and the factors must cover the factors'
/// blocks.
#[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
pub(super) unsafe fn score_symmetric_4bit_avx512(
    factors: &BlockedFactors,
    a: &[u8],
    b: &[u8],
) -> i64 {
    unsafe {
        let codebook = codebook_512(factors);
        let mut acc = [_mm512_setzero_si512(); 2];
        for block in 0..factors.layout.blocks() {
            let codes_a = unpack_4bit_avx512(codebook, a, block);
            let codes_b = unpack_4bit_avx512(codebook, b, block);
            for part in 0..2 {
                let factor = load_512(&factors.factors, (2 * block + part) * LANES_512);
                acc[part] = _mm512_dpwssd_epi32(
                    acc[part],
                    _mm512_mulhrs_epi16(codes_a[part], factor),
                    _mm512_mulhrs_epi16(codes_b[part], factor),
                );
            }
        }
        reduce_512(&acc)
    }
}

/// `Σ q(a) · q(b)` over the whole blocks of 2-bit `a` and `b`, both folded per pair.
///
/// # Safety
///
/// As [`score_symmetric_4bit_avx512`].
#[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
pub(super) unsafe fn score_symmetric_2bit_avx512(
    factors: &BlockedFactors,
    a: &[u8],
    b: &[u8],
) -> i64 {
    unsafe {
        let codebook = codebook_512(factors);
        let mut acc = [_mm512_setzero_si512(); 4];
        for block in 0..factors.layout.blocks() {
            let codes_a = unpack_2bit_avx512(codebook, a, block);
            let codes_b = unpack_2bit_avx512(codebook, b, block);
            for part in 0..4 {
                let factor = load_512(&factors.factors, (4 * block + part) * LANES_512);
                acc[part] = _mm512_dpwssd_epi32(
                    acc[part],
                    _mm512_mulhrs_epi16(codes_a[part], factor),
                    _mm512_mulhrs_epi16(codes_b[part], factor),
                );
            }
        }
        reduce_512(&acc)
    }
}

// ---- AVX2 ----

#[inline]
#[target_feature(enable = "avx2")]
unsafe fn load_256(values: &[i16], position: usize) -> __m256i {
    debug_assert!(position + LANES_256 <= values.len());
    unsafe { _mm256_loadu_si256(values.as_ptr().add(position).cast()) }
}

/// The two 128-bit halves of 32 signed bytes, widened to 16-bit lanes.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn widen_256(bytes: __m256i) -> [__m256i; 2] {
    [
        _mm256_cvtepi8_epi16(_mm256_castsi256_si128(bytes)),
        _mm256_cvtepi8_epi16(_mm256_extracti128_si256(bytes, 1)),
    ]
}

#[inline]
#[target_feature(enable = "avx2")]
unsafe fn codebook_256(factors: &BlockedFactors) -> __m256i {
    unsafe { _mm256_broadcastsi128_si256(_mm_loadu_si128(factors.codebook_bytes.as_ptr().cast())) }
}

/// Signed codebook values of the 4-bit codes of one block: the low nibbles, then the high ones,
/// 16 per vector.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn unpack_4bit_avx2(codebook: __m256i, codes: &[u8], block: usize) -> [__m256i; 4] {
    debug_assert!((block + 1) * BLOCK_BYTES <= codes.len());
    unsafe {
        let bytes = _mm256_loadu_si256(codes.as_ptr().add(block * BLOCK_BYTES).cast());
        let mask = _mm256_set1_epi8(0x0F);
        let low = _mm256_shuffle_epi8(codebook, _mm256_and_si256(bytes, mask));
        let high = _mm256_shuffle_epi8(
            codebook,
            _mm256_and_si256(_mm256_srli_epi16(bytes, 4), mask),
        );
        let [low_0, low_1] = widen_256(low);
        let [high_0, high_1] = widen_256(high);
        [low_0, low_1, high_0, high_1]
    }
}

/// Signed codebook values of the 2-bit codes in the low bits of each byte of `shifted`.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn lookup_2bit_avx2(codebook: __m256i, shifted: __m256i) -> __m256i {
    _mm256_shuffle_epi8(codebook, _mm256_and_si256(shifted, _mm256_set1_epi8(0x03)))
}

/// Signed codebook values of the 2-bit codes of one block, by bit offset, 16 per vector.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn unpack_2bit_avx2(codebook: __m256i, codes: &[u8], block: usize) -> [__m256i; 8] {
    debug_assert!((block + 1) * BLOCK_BYTES <= codes.len());
    unsafe {
        let bytes = _mm256_loadu_si256(codes.as_ptr().add(block * BLOCK_BYTES).cast());
        let [v0, v1] = widen_256(lookup_2bit_avx2(codebook, bytes));
        let [v2, v3] = widen_256(lookup_2bit_avx2(codebook, _mm256_srli_epi16(bytes, 2)));
        let [v4, v5] = widen_256(lookup_2bit_avx2(codebook, _mm256_srli_epi16(bytes, 4)));
        let [v6, v7] = widen_256(lookup_2bit_avx2(codebook, _mm256_srli_epi16(bytes, 6)));
        [v0, v1, v2, v3, v4, v5, v6, v7]
    }
}

/// Folds the whole blocks of 4-bit `codes` into `values`.
///
/// # Safety
///
/// The CPU must support AVX2; `codes`, `values` and the factors must cover the factors' blocks.
#[target_feature(enable = "avx2")]
pub(super) unsafe fn fold_4bit_avx2(factors: &BlockedFactors, codes: &[u8], values: &mut [i16]) {
    unsafe {
        let codebook = codebook_256(factors);
        for block in 0..factors.layout.blocks() {
            let codes = unpack_4bit_avx2(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (4 * block + part) * LANES_256;
                let folded = _mm256_mulhrs_epi16(code, load_256(&factors.factors, position));
                _mm256_storeu_si256(values.as_mut_ptr().add(position).cast(), folded);
            }
        }
    }
}

/// Folds the whole blocks of 2-bit `codes` into `values`.
///
/// # Safety
///
/// As [`fold_4bit_avx2`].
#[target_feature(enable = "avx2")]
pub(super) unsafe fn fold_2bit_avx2(factors: &BlockedFactors, codes: &[u8], values: &mut [i16]) {
    unsafe {
        let codebook = codebook_256(factors);
        for block in 0..factors.layout.blocks() {
            let codes = unpack_2bit_avx2(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (8 * block + part) * LANES_256;
                let folded = _mm256_mulhrs_epi16(code, load_256(&factors.factors, position));
                _mm256_storeu_si256(values.as_mut_ptr().add(position).cast(), folded);
            }
        }
    }
}

#[inline]
#[target_feature(enable = "avx2")]
unsafe fn reduce_256(accumulators: &[__m256i]) -> i64 {
    let mut sum = _mm256_setzero_si256();
    for &acc in accumulators {
        sum = _mm256_add_epi32(sum, acc);
    }
    let sum = _mm_add_epi32(
        _mm256_castsi256_si128(sum),
        _mm256_extracti128_si256(sum, 1),
    );
    let sum = _mm_add_epi32(sum, _mm_shuffle_epi32(sum, 0b01_00_11_10));
    let sum = _mm_add_epi32(sum, _mm_shuffle_epi32(sum, 0b10_11_00_01));
    i64::from(_mm_cvtsi128_si32(sum))
}

/// `Σ query · q(codes)` over the whole blocks of 4-bit `codes`.
///
/// # Safety
///
/// The CPU must support AVX2; `codes`, `query` and the factors must cover the factors' blocks.
#[target_feature(enable = "avx2")]
pub(super) unsafe fn score_4bit_avx2(query: &[i16], factors: &BlockedFactors, codes: &[u8]) -> i64 {
    unsafe {
        let codebook = codebook_256(factors);
        let mut acc = [_mm256_setzero_si256(); 2];
        for block in 0..factors.layout.blocks() {
            let codes = unpack_4bit_avx2(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (4 * block + part) * LANES_256;
                let folded = _mm256_mulhrs_epi16(code, load_256(&factors.factors, position));
                let products = _mm256_madd_epi16(load_256(query, position), folded);
                acc[part % 2] = _mm256_add_epi32(acc[part % 2], products);
            }
        }
        reduce_256(&acc)
    }
}

/// `Σ query · q(codes)` over the whole blocks of 2-bit `codes`.
///
/// # Safety
///
/// As [`score_4bit_avx2`].
#[target_feature(enable = "avx2")]
pub(super) unsafe fn score_2bit_avx2(query: &[i16], factors: &BlockedFactors, codes: &[u8]) -> i64 {
    unsafe {
        let codebook = codebook_256(factors);
        let mut acc = [_mm256_setzero_si256(); 2];
        for block in 0..factors.layout.blocks() {
            let codes = unpack_2bit_avx2(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (8 * block + part) * LANES_256;
                let folded = _mm256_mulhrs_epi16(code, load_256(&factors.factors, position));
                let products = _mm256_madd_epi16(load_256(query, position), folded);
                acc[part % 2] = _mm256_add_epi32(acc[part % 2], products);
            }
        }
        reduce_256(&acc)
    }
}

/// `Σ q(a) · q(b)` over the whole blocks of 4-bit `a` and `b`, both folded per pair.
///
/// # Safety
///
/// The CPU must support AVX2; `a`, `b` and the factors must cover the factors' blocks.
#[target_feature(enable = "avx2")]
pub(super) unsafe fn score_symmetric_4bit_avx2(
    factors: &BlockedFactors,
    a: &[u8],
    b: &[u8],
) -> i64 {
    unsafe {
        let codebook = codebook_256(factors);
        let mut acc = [_mm256_setzero_si256(); 2];
        for block in 0..factors.layout.blocks() {
            let codes_a = unpack_4bit_avx2(codebook, a, block);
            let codes_b = unpack_4bit_avx2(codebook, b, block);
            for part in 0..4 {
                let factor = load_256(&factors.factors, (4 * block + part) * LANES_256);
                let products = _mm256_madd_epi16(
                    _mm256_mulhrs_epi16(codes_a[part], factor),
                    _mm256_mulhrs_epi16(codes_b[part], factor),
                );
                acc[part % 2] = _mm256_add_epi32(acc[part % 2], products);
            }
        }
        reduce_256(&acc)
    }
}

/// `Σ q(a) · q(b)` over the whole blocks of 2-bit `a` and `b`, both folded per pair.
///
/// # Safety
///
/// As [`score_symmetric_4bit_avx2`].
#[target_feature(enable = "avx2")]
pub(super) unsafe fn score_symmetric_2bit_avx2(
    factors: &BlockedFactors,
    a: &[u8],
    b: &[u8],
) -> i64 {
    unsafe {
        let codebook = codebook_256(factors);
        let mut acc = [_mm256_setzero_si256(); 2];
        for block in 0..factors.layout.blocks() {
            let codes_a = unpack_2bit_avx2(codebook, a, block);
            let codes_b = unpack_2bit_avx2(codebook, b, block);
            for part in 0..8 {
                let factor = load_256(&factors.factors, (8 * block + part) * LANES_256);
                let products = _mm256_madd_epi16(
                    _mm256_mulhrs_epi16(codes_a[part], factor),
                    _mm256_mulhrs_epi16(codes_b[part], factor),
                );
                acc[part % 2] = _mm256_add_epi32(acc[part % 2], products);
            }
        }
        reduce_256(&acc)
    }
}
