//! aarch64 kernels of [`super`]: NEON. Each reads the whole blocks of its codes in
//! [block order](super#block-order); the callers add the dimensions after them.
//!
//! The instructions are those of the dimension-order kernels `score_{2,4}bit_internal_folded_neon`:
//! `tbl` looks the codes up in the signed codebook, `sqrdmulh` folds the values widened to i16
//! and `smlal` sums the products in i32 lanes. `sqrdmulh` computes `(2·c·f + 2¹⁵) >> 16`, which
//! equals [`super::fold_q15`]'s `(c·f + 2¹⁴) >> 15`; it saturates only for `c = f = −2¹⁵`.
//!
//! The folded values stay within ±127 (`|c| ≤ 127` in the aarch64 codebook, `D' ≤ 1` in Q15), so
//! a product is below 2¹⁴, and the sum of all `FOLDED_MAX_DIM` = 2¹⁶ of them stays below 2³¹:
//! neither the i32 lanes nor the i32 reduction overflow.

use core::arch::aarch64::*;

use super::{BLOCK_BYTES, BlockedFactors};

/// 16-bit lanes of a 128-bit vector.
const LANES: usize = 8;
/// Codes per lookup: the bytes of a 128-bit vector.
const PART: usize = 16;

/// The signed codebook, padded with zeros, as a `tbl` table.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn codebook(factors: &BlockedFactors) -> int8x16_t {
    unsafe { vld1q_s8(factors.codebook_bytes.as_ptr()) }
}

/// The 16 values of `values` from `position` on.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn load_part(values: &[i16], position: usize) -> [int16x8_t; 2] {
    debug_assert!(position + PART <= values.len());
    unsafe {
        let ptr = values.as_ptr().add(position);
        [vld1q_s16(ptr), vld1q_s16(ptr.add(LANES))]
    }
}

/// The 32 bytes of block `block` of `codes`.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn load_block(codes: &[u8], block: usize) -> [uint8x16_t; 2] {
    debug_assert!((block + 1) * BLOCK_BYTES <= codes.len());
    unsafe {
        let ptr = codes.as_ptr().add(block * BLOCK_BYTES);
        [vld1q_u8(ptr), vld1q_u8(ptr.add(PART))]
    }
}

/// Signed codebook values of the 4-bit codes of one block: the low nibbles, then the high ones,
/// 16 per vector.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn unpack_4bit(codebook: int8x16_t, codes: &[u8], block: usize) -> [int8x16_t; 4] {
    let [bytes_0, bytes_1] = unsafe { load_block(codes, block) };
    let mask = vdupq_n_u8(0x0F);
    [
        vqtbl1q_s8(codebook, vandq_u8(bytes_0, mask)),
        vqtbl1q_s8(codebook, vandq_u8(bytes_1, mask)),
        vqtbl1q_s8(codebook, vshrq_n_u8(bytes_0, 4)),
        vqtbl1q_s8(codebook, vshrq_n_u8(bytes_1, 4)),
    ]
}

/// Signed codebook values of the 2-bit codes in the low bits of each byte of `shifted`.
#[inline]
#[target_feature(enable = "neon")]
fn lookup_2bit(codebook: int8x16_t, shifted: uint8x16_t) -> int8x16_t {
    vqtbl1q_s8(codebook, vandq_u8(shifted, vdupq_n_u8(0x03)))
}

/// Signed codebook values of the 2-bit codes of one block, by bit offset, 16 per vector.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn unpack_2bit(codebook: int8x16_t, codes: &[u8], block: usize) -> [int8x16_t; 8] {
    let [bytes_0, bytes_1] = unsafe { load_block(codes, block) };
    [
        lookup_2bit(codebook, bytes_0),
        lookup_2bit(codebook, bytes_1),
        lookup_2bit(codebook, vshrq_n_u8(bytes_0, 2)),
        lookup_2bit(codebook, vshrq_n_u8(bytes_1, 2)),
        lookup_2bit(codebook, vshrq_n_u8(bytes_0, 4)),
        lookup_2bit(codebook, vshrq_n_u8(bytes_1, 4)),
        vqtbl1q_s8(codebook, vshrq_n_u8(bytes_0, 6)),
        vqtbl1q_s8(codebook, vshrq_n_u8(bytes_1, 6)),
    ]
}

/// `q` of 16 codebook values with their factors: widened to i16, then scaled by `sqrdmulh`.
#[inline]
#[target_feature(enable = "neon")]
fn fold(values: int8x16_t, factors: [int16x8_t; 2]) -> [int16x8_t; 2] {
    [
        vqrdmulhq_s16(vmovl_s8(vget_low_s8(values)), factors[0]),
        vqrdmulhq_s16(vmovl_high_s8(values), factors[1]),
    ]
}

/// Adds the products of the 16 lanes of `a` and `b` to four accumulators, so that no `smlal`
/// waits for the one before.
#[inline]
#[target_feature(enable = "neon")]
fn multiply_add(acc: &mut [int32x4_t; 4], a: [int16x8_t; 2], b: [int16x8_t; 2]) {
    acc[0] = vmlal_s16(acc[0], vget_low_s16(a[0]), vget_low_s16(b[0]));
    acc[1] = vmlal_high_s16(acc[1], a[0], b[0]);
    acc[2] = vmlal_s16(acc[2], vget_low_s16(a[1]), vget_low_s16(b[1]));
    acc[3] = vmlal_high_s16(acc[3], a[1], b[1]);
}

/// Two sets of accumulators: the even and the odd vectors of 16 codes of a block.
type Accumulators = [[int32x4_t; 4]; 2];

#[inline]
#[target_feature(enable = "neon")]
fn zero() -> Accumulators {
    [[vdupq_n_s32(0); 4]; 2]
}

#[inline]
#[target_feature(enable = "neon")]
fn reduce(accumulators: &Accumulators) -> i64 {
    let mut sum = vdupq_n_s32(0);
    for &acc in accumulators.as_flattened() {
        sum = vaddq_s32(sum, acc);
    }
    i64::from(vaddvq_s32(sum))
}

/// Stores the 16 values `folded` at `position` of `values`.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn store_part(values: &mut [i16], position: usize, folded: [int16x8_t; 2]) {
    debug_assert!(position + PART <= values.len());
    unsafe {
        let ptr = values.as_mut_ptr().add(position);
        vst1q_s16(ptr, folded[0]);
        vst1q_s16(ptr.add(LANES), folded[1]);
    }
}

/// Folds the whole blocks of 4-bit `codes` into `values`.
///
/// # Safety
///
/// The CPU must support NEON; `codes`, `values` and the factors must cover the factors' blocks.
#[target_feature(enable = "neon")]
pub(super) unsafe fn fold_4bit_neon(factors: &BlockedFactors, codes: &[u8], values: &mut [i16]) {
    unsafe {
        let codebook = codebook(factors);
        for block in 0..factors.layout.blocks() {
            let codes = unpack_4bit(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (4 * block + part) * PART;
                let folded = fold(code, load_part(&factors.factors, position));
                store_part(values, position, folded);
            }
        }
    }
}

/// Folds the whole blocks of 2-bit `codes` into `values`.
///
/// # Safety
///
/// As [`fold_4bit_neon`].
#[target_feature(enable = "neon")]
pub(super) unsafe fn fold_2bit_neon(factors: &BlockedFactors, codes: &[u8], values: &mut [i16]) {
    unsafe {
        let codebook = codebook(factors);
        for block in 0..factors.layout.blocks() {
            let codes = unpack_2bit(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (8 * block + part) * PART;
                let folded = fold(code, load_part(&factors.factors, position));
                store_part(values, position, folded);
            }
        }
    }
}

/// `Σ query · q(codes)` over the whole blocks of 4-bit `codes`.
///
/// # Safety
///
/// The CPU must support NEON; `codes`, `query` and the factors must cover the factors' blocks.
#[target_feature(enable = "neon")]
pub(super) unsafe fn score_4bit_neon(query: &[i16], factors: &BlockedFactors, codes: &[u8]) -> i64 {
    unsafe {
        let codebook = codebook(factors);
        let mut acc = zero();
        for block in 0..factors.layout.blocks() {
            let codes = unpack_4bit(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (4 * block + part) * PART;
                let folded = fold(code, load_part(&factors.factors, position));
                multiply_add(&mut acc[part % 2], load_part(query, position), folded);
            }
        }
        reduce(&acc)
    }
}

/// `Σ query · q(codes)` over the whole blocks of 2-bit `codes`.
///
/// # Safety
///
/// As [`score_4bit_neon`].
#[target_feature(enable = "neon")]
pub(super) unsafe fn score_2bit_neon(query: &[i16], factors: &BlockedFactors, codes: &[u8]) -> i64 {
    unsafe {
        let codebook = codebook(factors);
        let mut acc = zero();
        for block in 0..factors.layout.blocks() {
            let codes = unpack_2bit(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (8 * block + part) * PART;
                let folded = fold(code, load_part(&factors.factors, position));
                multiply_add(&mut acc[part % 2], load_part(query, position), folded);
            }
        }
        reduce(&acc)
    }
}

/// `Σ q(a) · q(b)` over the whole blocks of 4-bit `a` and `b`, both folded per pair.
///
/// # Safety
///
/// The CPU must support NEON; `a`, `b` and the factors must cover the factors' blocks.
#[target_feature(enable = "neon")]
pub(super) unsafe fn score_symmetric_4bit_neon(
    factors: &BlockedFactors,
    a: &[u8],
    b: &[u8],
) -> i64 {
    unsafe {
        let codebook = codebook(factors);
        let mut acc = zero();
        for block in 0..factors.layout.blocks() {
            let codes_a = unpack_4bit(codebook, a, block);
            let codes_b = unpack_4bit(codebook, b, block);
            for part in 0..4 {
                let factor = load_part(&factors.factors, (4 * block + part) * PART);
                multiply_add(
                    &mut acc[part % 2],
                    fold(codes_a[part], factor),
                    fold(codes_b[part], factor),
                );
            }
        }
        reduce(&acc)
    }
}

/// `Σ q(a) · q(b)` over the whole blocks of 2-bit `a` and `b`, both folded per pair.
///
/// # Safety
///
/// As [`score_symmetric_4bit_neon`].
#[target_feature(enable = "neon")]
pub(super) unsafe fn score_symmetric_2bit_neon(
    factors: &BlockedFactors,
    a: &[u8],
    b: &[u8],
) -> i64 {
    unsafe {
        let codebook = codebook(factors);
        let mut acc = zero();
        for block in 0..factors.layout.blocks() {
            let codes_a = unpack_2bit(codebook, a, block);
            let codes_b = unpack_2bit(codebook, b, block);
            for part in 0..8 {
                let factor = load_part(&factors.factors, (8 * block + part) * PART);
                multiply_add(
                    &mut acc[part % 2],
                    fold(codes_a[part], factor),
                    fold(codes_b[part], factor),
                );
            }
        }
        reduce(&acc)
    }
}
