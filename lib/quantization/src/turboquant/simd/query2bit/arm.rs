//! NEON kernels for the symmetric 2-bit paths (`score_2bit_internal*`) on
//! aarch64.  The codebook is the signed `CODEBOOK_I8`.
//!
//! # Unpack trick
//! Each packed data byte holds 4 × 2-bit codes; consecutive pairs (`c0c1`,
//! `c2c3`) are exactly the low / high nibble of the byte.  A nibble takes
//! 16 values → one-to-one with a 16-entry `vqtbl1_s8` table.  We precompute
//! two 16-byte tables:
//!
//!   * `PAIR_TABLE_EVEN[pair]  = CODEBOOK_I8[pair & 0b11]`        — "even code"
//!   * `PAIR_TABLE_ODD[pair]   = CODEBOOK_I8[(pair >> 2) & 0b11]` — "odd code"
//!
//! For 4 packed data bytes (= 16 codes = one query chunk):
//! 1. Split low/high nibbles of each byte (4 + 4 = 8 nibbles).
//! 2. Interleave them so lane `2i` = low nib of byte i, lane `2i+1` = high nib.
//! 3. Two `vqtbl1_s8` lookups yield 8 + 8 centroid bytes.
//! 4. `vzip1_s8` + `vzip2_s8` + `vcombine_s8` → 16 centroids in natural
//!    dim order.
//!
//! Same pipeline as 4-bit from that point on (i8 × i8 → i16 + `vpadalq_s16`).

use super::super::{BLOCK_BYTES, BlockedFactors};
use super::{CODEBOOK_I8, FOLDED_MAX_DIM, centroid_dot};

/// `PAIR_TABLE_EVEN[nibble]` = `CODEBOOK_I8[nibble & 0b11]`.
const PAIR_TABLE_EVEN: [i8; 16] = {
    let mut tbl = [0_i8; 16];
    let mut k = 0;
    while k < 16 {
        tbl[k] = CODEBOOK_I8[k & 0b11];
        k += 1;
    }
    tbl
};

/// `PAIR_TABLE_ODD[nibble]` = `CODEBOOK_I8[(nibble >> 2) & 0b11]`.
const PAIR_TABLE_ODD: [i8; 16] = {
    let mut tbl = [0_i8; 16];
    let mut k = 0;
    while k < 16 {
        tbl[k] = CODEBOOK_I8[(k >> 2) & 0b11];
        k += 1;
    }
    tbl
};

/// Unpack 4 packed data bytes into a natural-order `int8x16_t` of 16
/// centroid i8 values (one per code).
#[inline]
#[target_feature(enable = "neon")]
unsafe fn unpack_16_codes(bytes4: *const u8) -> core::arch::aarch64::int8x16_t {
    use core::arch::aarch64::*;
    unsafe {
        let data = vreinterpret_u8_u32(vld1_dup_u32(bytes4.cast::<u32>()));
        // data is uint8x8 = [b0, b1, b2, b3, b0, b1, b2, b3]; we only use low 4.

        let nibble_mask = vdup_n_u8(0x0F);
        let lo_nibs = vand_u8(data, nibble_mask); // [b0&F, b1&F, b2&F, b3&F, ...]
        let hi_nibs = vshr_n_u8(data, 4);

        // Interleave: [lo(b0), hi(b0), lo(b1), hi(b1), lo(b2), hi(b2), lo(b3), hi(b3)]
        // `vzip1_u8` on the 8-lane inputs takes lanes [0..4] of each, zipped.
        let pair_indices = vzip1_u8(lo_nibs, hi_nibs);

        let t_even = vld1q_s8(PAIR_TABLE_EVEN.as_ptr());
        let t_odd = vld1q_s8(PAIR_TABLE_ODD.as_ptr());

        let c_even = vqtbl1_s8(t_even, pair_indices); // int8x8
        let c_odd = vqtbl1_s8(t_odd, pair_indices); // int8x8

        // Natural order: [c0, c1, c2, c3, ..., c15]
        let c_lo = vzip1_s8(c_even, c_odd);
        let c_hi = vzip2_s8(c_even, c_odd);
        vcombine_s8(c_lo, c_hi)
    }
}

/// NEON implementation of [`super::score_2bit_internal`] — vector × vector
/// centroid dot, both sides unpacked via the pair-table trick.
///
/// # Safety
/// CPU must support `neon`.
#[target_feature(enable = "neon")]
pub unsafe fn score_2bit_internal_neon(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::aarch64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_2bit_internal_neon: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let mut acc = vdupq_n_s32(0);

        let n_full = a.len() / 4;
        for i in 0..n_full {
            let c_a = unpack_16_codes(a.as_ptr().add(i * 4));
            let c_b = unpack_16_codes(b.as_ptr().add(i * 4));

            let prod_lo = vmull_s8(vget_low_s8(c_a), vget_low_s8(c_b));
            let prod_hi = vmull_high_s8(c_a, c_b);
            acc = vpadalq_s16(acc, prod_lo);
            acc = vpadalq_s16(acc, prod_hi);
        }

        let simd_bytes = n_full * 4;
        let acc_i64 = i64::from(vaddvq_s32(acc))
            + super::score_2bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        centroid_dot(acc_i64 as f32)
    }
}

/// SDOT variant of [`score_2bit_internal_neon`], 2× chunk-unrolled.
///
/// # Safety
/// CPU must support `neon` and `dotprod`.
#[target_feature(enable = "neon,dotprod")]
pub unsafe fn score_2bit_internal_neon_sdot(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::aarch64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_2bit_internal_neon_sdot: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    unsafe {
        let mut acc_0 = vdupq_n_s32(0);
        let mut acc_1 = vdupq_n_s32(0);

        let n_pairs = a.len() / 8;
        for i in 0..n_pairs {
            let c_a_0 = unpack_16_codes(a.as_ptr().add(8 * i));
            let c_a_1 = unpack_16_codes(a.as_ptr().add(8 * i + 4));
            let c_b_0 = unpack_16_codes(b.as_ptr().add(8 * i));
            let c_b_1 = unpack_16_codes(b.as_ptr().add(8 * i + 4));

            core::arch::asm!(
                "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                acc = inout(vreg) acc_0,
                a = in(vreg) c_a_0,
                b = in(vreg) c_b_0,
                options(pure, nomem, nostack, preserves_flags),
            );
            core::arch::asm!(
                "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                acc = inout(vreg) acc_1,
                a = in(vreg) c_a_1,
                b = in(vreg) c_b_1,
                options(pure, nomem, nostack, preserves_flags),
            );
        }

        // Odd 4-byte chunk leftover (a.len() % 8 ∈ {4..7}): run via vmull.
        let mut acc_tail = vdupq_n_s32(0);
        let mut offset = n_pairs * 8;
        if a.len() - offset >= 4 {
            let c_a = unpack_16_codes(a.as_ptr().add(offset));
            let c_b = unpack_16_codes(b.as_ptr().add(offset));
            let prod_lo = vmull_s8(vget_low_s8(c_a), vget_low_s8(c_b));
            let prod_hi = vmull_high_s8(c_a, c_b);
            acc_tail = vpadalq_s16(acc_tail, prod_lo);
            acc_tail = vpadalq_s16(acc_tail, prod_hi);
            offset += 4;
        }

        let acc = vaddq_s32(vaddq_s32(acc_0, acc_1), acc_tail);
        let acc_i64 = i64::from(vaddvq_s32(acc))
            + super::score_2bit_internal_integer(&a[offset..], &b[offset..]);
        centroid_dot(acc_i64 as f32)
    }
}

// ------------------------------------------------------------------
// score_2bit_internal_folded — TQ+ symmetric path. Each side's signed
// codebook value is scaled by its coordinate's Q15 factor with
// `vqrdmulhq_s16`, the same rounding as `fold_q15`, which keeps it within
// i8; `vmlal_s16` then sums the products into i32 lanes:
//   per product:   |q_a · q_b| ≤ 127 · 127 = 16 129
//   total:         < 2¹⁶ · 16 384 = 2³⁰ at `FOLDED_MAX_DIM` ✓
// ------------------------------------------------------------------

/// NEON implementation of [`super::score_2bit_internal_folded`]. 16 coords
/// per iteration.
///
/// # Safety
/// CPU must support `neon`.
#[target_feature(enable = "neon")]
pub unsafe fn score_2bit_internal_folded_neon(a: &[u8], b: &[u8], factors: &[i16]) -> i64 {
    use core::arch::aarch64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_2bit_internal_folded_neon: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    assert_eq!(
        factors.len(),
        4 * a.len(),
        "score_2bit_internal_folded_neon: factors length {} != 4 · a.len() {}",
        factors.len(),
        4 * a.len(),
    );
    assert!(factors.len() <= FOLDED_MAX_DIM);

    unsafe {
        let mut acc = vdupq_n_s32(0);

        // 4 bytes per source = 16 coords per iter.
        let n_full = a.len() / 4;
        for i in 0..n_full {
            let c_a = unpack_16_codes(a.as_ptr().add(i * 4));
            let c_b = unpack_16_codes(b.as_ptr().add(i * 4));
            let f_lo = vld1q_s16(factors.as_ptr().add(16 * i));
            let f_hi = vld1q_s16(factors.as_ptr().add(16 * i + 8));

            let q_a_lo = vqrdmulhq_s16(vmovl_s8(vget_low_s8(c_a)), f_lo);
            let q_a_hi = vqrdmulhq_s16(vmovl_high_s8(c_a), f_hi);
            let q_b_lo = vqrdmulhq_s16(vmovl_s8(vget_low_s8(c_b)), f_lo);
            let q_b_hi = vqrdmulhq_s16(vmovl_high_s8(c_b), f_hi);

            acc = vmlal_s16(acc, vget_low_s16(q_a_lo), vget_low_s16(q_b_lo));
            acc = vmlal_high_s16(acc, q_a_lo, q_b_lo);
            acc = vmlal_s16(acc, vget_low_s16(q_a_hi), vget_low_s16(q_b_hi));
            acc = vmlal_high_s16(acc, q_a_hi, q_b_hi);
        }

        let simd_bytes = n_full * 4;
        i64::from(vaddvq_s32(acc))
            + super::score_2bit_internal_folded_scalar(
                &a[simd_bytes..],
                &b[simd_bytes..],
                &factors[4 * simd_bytes..],
            )
    }
}

// ------------------------------------------------------------------
// Block order — the TQ+ symmetric score of `super::super::BlockedFactors` and
// `super::super::FoldedQuery`, the same sum as `score_2bit_internal_folded`. A
// block of `BLOCK_BYTES` code bytes is read by bit offset: the codes in bits
// 0..2 of its 32 bytes (dimensions `4p`), then those in bits 2..4 (`4p + 1`),
// 4..6 and 6..8, not interleaved back into dimension order: the factors and a
// folded query are stored in that order. Same instructions as
// `score_2bit_internal_folded_neon`: `tbl` looks the codes up, `sqrdmulh` folds
// them widened to i16 (`(2·c·f + 2¹⁵) >> 16`, which equals `fold_q15`'s
// `(c·f + 2¹⁴) >> 15`), `smlal` sums the products in i32 lanes:
//   per product:   |q_a · q_b| ≤ 127 · 127 = 16 129
//   total:         < 2¹⁶ · 2¹⁴ = 2³⁰ at `FOLDED_MAX_DIM` ✓
// ------------------------------------------------------------------

/// [`CODEBOOK_I8`] padded with zeros to a `tbl` table.
const CODEBOOK_TABLE: [i8; 16] = {
    let mut table = [0; 16];
    let mut code = 0;
    while code < CODEBOOK_I8.len() {
        table[code] = CODEBOOK_I8[code];
        code += 1;
    }
    table
};

/// 16-bit lanes of a 128-bit vector.
const LANES: usize = 8;
/// Codes per lookup: the bytes of a 128-bit vector.
const PART: usize = 16;

/// The 16 values of `values` from `position` on.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn load_i16_part(values: &[i16], position: usize) -> [core::arch::aarch64::int16x8_t; 2] {
    use core::arch::aarch64::*;
    debug_assert!(position + PART <= values.len());
    unsafe {
        let ptr = values.as_ptr().add(position);
        [vld1q_s16(ptr), vld1q_s16(ptr.add(LANES))]
    }
}

/// Stores the 16 values `folded` at `position` of `values`.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn store_i16_part(
    values: &mut [i16],
    position: usize,
    folded: [core::arch::aarch64::int16x8_t; 2],
) {
    use core::arch::aarch64::*;
    debug_assert!(position + PART <= values.len());
    unsafe {
        let ptr = values.as_mut_ptr().add(position);
        vst1q_s16(ptr, folded[0]);
        vst1q_s16(ptr.add(LANES), folded[1]);
    }
}

/// The 32 bytes of block `block` of `codes`.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn load_block(codes: &[u8], block: usize) -> [core::arch::aarch64::uint8x16_t; 2] {
    use core::arch::aarch64::*;
    debug_assert!((block + 1) * BLOCK_BYTES <= codes.len());
    unsafe {
        let ptr = codes.as_ptr().add(block * BLOCK_BYTES);
        [vld1q_u8(ptr), vld1q_u8(ptr.add(PART))]
    }
}

/// `q` of 16 codebook values with their factors: widened to i16, then scaled by `sqrdmulh`.
#[inline]
#[target_feature(enable = "neon")]
fn fold_part(
    values: core::arch::aarch64::int8x16_t,
    factors: [core::arch::aarch64::int16x8_t; 2],
) -> [core::arch::aarch64::int16x8_t; 2] {
    use core::arch::aarch64::*;
    [
        vqrdmulhq_s16(vmovl_s8(vget_low_s8(values)), factors[0]),
        vqrdmulhq_s16(vmovl_high_s8(values), factors[1]),
    ]
}

/// Two sets of four accumulators: the even and the odd vectors of 16 codes of a block.
type Accumulators = [[core::arch::aarch64::int32x4_t; 4]; 2];

#[inline]
#[target_feature(enable = "neon")]
fn zero_accumulators() -> Accumulators {
    use core::arch::aarch64::*;
    [[vdupq_n_s32(0); 4]; 2]
}

/// Adds the products of the 16 lanes of `a` and `b` to four accumulators, so that no `smlal`
/// waits for the one before.
#[inline]
#[target_feature(enable = "neon")]
fn multiply_add(
    acc: &mut [core::arch::aarch64::int32x4_t; 4],
    a: [core::arch::aarch64::int16x8_t; 2],
    b: [core::arch::aarch64::int16x8_t; 2],
) {
    use core::arch::aarch64::*;
    acc[0] = vmlal_s16(acc[0], vget_low_s16(a[0]), vget_low_s16(b[0]));
    acc[1] = vmlal_high_s16(acc[1], a[0], b[0]);
    acc[2] = vmlal_s16(acc[2], vget_low_s16(a[1]), vget_low_s16(b[1]));
    acc[3] = vmlal_high_s16(acc[3], a[1], b[1]);
}

#[inline]
#[target_feature(enable = "neon")]
fn reduce_accumulators(accumulators: &Accumulators) -> i64 {
    use core::arch::aarch64::*;
    let mut sum = vdupq_n_s32(0);
    for &acc in accumulators.as_flattened() {
        sum = vaddq_s32(sum, acc);
    }
    i64::from(vaddvq_s32(sum))
}

/// Signed codebook values of the codes in the low 2 bits of each byte of `shifted`.
#[inline]
#[target_feature(enable = "neon")]
fn lookup_2bit_neon(
    codebook: core::arch::aarch64::int8x16_t,
    shifted: core::arch::aarch64::uint8x16_t,
) -> core::arch::aarch64::int8x16_t {
    use core::arch::aarch64::*;
    vqtbl1q_s8(codebook, vandq_u8(shifted, vdupq_n_u8(0x03)))
}

/// Signed codebook values of the codes of block `block` of `codes`, by bit offset, 16 per
/// vector.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn unpack_blocked_neon(
    codebook: core::arch::aarch64::int8x16_t,
    codes: &[u8],
    block: usize,
) -> [core::arch::aarch64::int8x16_t; 8] {
    use core::arch::aarch64::*;
    let [bytes_0, bytes_1] = unsafe { load_block(codes, block) };
    [
        lookup_2bit_neon(codebook, bytes_0),
        lookup_2bit_neon(codebook, bytes_1),
        lookup_2bit_neon(codebook, vshrq_n_u8(bytes_0, 2)),
        lookup_2bit_neon(codebook, vshrq_n_u8(bytes_1, 2)),
        lookup_2bit_neon(codebook, vshrq_n_u8(bytes_0, 4)),
        lookup_2bit_neon(codebook, vshrq_n_u8(bytes_1, 4)),
        vqtbl1q_s8(codebook, vshrq_n_u8(bytes_0, 6)),
        vqtbl1q_s8(codebook, vshrq_n_u8(bytes_1, 6)),
    ]
}

/// [`super::fold_2bit_blocked`] on NEON.
///
/// # Safety
///
/// The CPU must support NEON; `codes`, `values` and the factors must cover the factors' blocks.
#[target_feature(enable = "neon")]
pub(super) unsafe fn fold_2bit_blocked_neon(
    factors: &BlockedFactors,
    codes: &[u8],
    values: &mut [i16],
) {
    use core::arch::aarch64::*;
    unsafe {
        let codebook = vld1q_s8(CODEBOOK_TABLE.as_ptr());
        for block in 0..factors.layout.blocks() {
            let codes = unpack_blocked_neon(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (8 * block + part) * PART;
                let folded = fold_part(code, load_i16_part(&factors.factors, position));
                store_i16_part(values, position, folded);
            }
        }
    }
}

/// [`super::score_2bit_blocked`] on NEON.
///
/// # Safety
///
/// The CPU must support NEON; `codes`, `query` and the factors must cover the factors' blocks.
#[target_feature(enable = "neon")]
pub(super) unsafe fn score_2bit_blocked_neon(
    query: &[i16],
    factors: &BlockedFactors,
    codes: &[u8],
) -> i64 {
    use core::arch::aarch64::*;
    unsafe {
        let codebook = vld1q_s8(CODEBOOK_TABLE.as_ptr());
        let mut acc = zero_accumulators();
        for block in 0..factors.layout.blocks() {
            let codes = unpack_blocked_neon(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (8 * block + part) * PART;
                let folded = fold_part(code, load_i16_part(&factors.factors, position));
                multiply_add(&mut acc[part % 2], load_i16_part(query, position), folded);
            }
        }
        reduce_accumulators(&acc)
    }
}

/// [`super::score_2bit_internal_blocked`] on NEON.
///
/// # Safety
///
/// The CPU must support NEON; `a`, `b` and the factors must cover the factors' blocks.
#[target_feature(enable = "neon")]
pub(super) unsafe fn score_2bit_internal_blocked_neon(
    factors: &BlockedFactors,
    a: &[u8],
    b: &[u8],
) -> i64 {
    use core::arch::aarch64::*;
    unsafe {
        let codebook = vld1q_s8(CODEBOOK_TABLE.as_ptr());
        let mut acc = zero_accumulators();
        for block in 0..factors.layout.blocks() {
            let codes_a = unpack_blocked_neon(codebook, a, block);
            let codes_b = unpack_blocked_neon(codebook, b, block);
            for part in 0..8 {
                let factor = load_i16_part(&factors.factors, (8 * block + part) * PART);
                multiply_add(
                    &mut acc[part % 2],
                    fold_part(codes_a[part], factor),
                    fold_part(codes_b[part], factor),
                );
            }
        }
        reduce_accumulators(&acc)
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::super::shared::pack_codes;
    use super::super::shared::{PARITY_DIMS, random_inputs};
    use super::super::{score_2bit_internal_folded_scalar, score_2bit_internal_scalar};
    use super::{
        score_2bit_internal_folded_neon, score_2bit_internal_neon, score_2bit_internal_neon_sdot,
    };

    /// Build deterministic non-negative Q15 factors of length `4 · vec_bytes`
    /// for parity tests of the 2-bit folded kernel.
    fn random_factors(rng: &mut StdRng, vec_bytes: usize) -> Vec<i16> {
        use rand::RngExt;
        (0..4 * vec_bytes)
            .map(|_| rng.random_range(0..=i16::MAX))
            .collect()
    }

    #[test]
    fn test_score_neon_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let scalar = score_2bit_internal_scalar(&vec_a, &vec_b);

            let neon = unsafe { score_2bit_internal_neon(&vec_a, &vec_b) };
            assert_eq!(scalar, neon, "score neon parity fail at dim {dim}");

            if std::arch::is_aarch64_feature_detected!("dotprod") {
                let sdot = unsafe { score_2bit_internal_neon_sdot(&vec_a, &vec_b) };
                assert_eq!(scalar, sdot, "score sdot parity fail at dim {dim}");
            }
        }
    }

    /// Parity for the 2-bit folded kernel: NEON must match the scalar reference
    /// bit-exactly across our matryoshka corner-case dims.
    #[test]
    fn test_score_folded_neon_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(0xBEEF);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let factors = random_factors(&mut rng, vec_a.len());
            let scalar = score_2bit_internal_folded_scalar(&vec_a, &vec_b, &factors);
            let neon = unsafe { score_2bit_internal_folded_neon(&vec_a, &vec_b, &factors) };
            assert_eq!(
                scalar, neon,
                "folded: scalar {scalar} != neon {neon} at dim {dim}"
            );
        }
    }

    /// Saturation-safety for the 2-bit folded NEON kernel at 64K dims: must
    /// match the i64 scalar reference under worst-case inputs.
    #[test]
    fn test_score_folded_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![3; dim];
        let vec_a = pack_codes(&indices, 2);
        let vec_b = pack_codes(&indices, 2);
        let factors: Vec<i16> = vec![i16::MAX; dim];

        let scalar = score_2bit_internal_folded_scalar(&vec_a, &vec_b, &factors);
        unsafe {
            let neon = score_2bit_internal_folded_neon(&vec_a, &vec_b, &factors);
            assert_eq!(scalar, neon, "folded score neon overflow at dim={dim}");
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
            let neon = score_2bit_internal_neon(&vec_a, &vec_b);
            assert_eq!(scalar, neon, "score neon overflow at 64k");

            if std::arch::is_aarch64_feature_detected!("dotprod") {
                let sdot = score_2bit_internal_neon_sdot(&vec_a, &vec_b);
                assert_eq!(scalar, sdot, "score sdot overflow at 64k");
            }
        }
    }
}
