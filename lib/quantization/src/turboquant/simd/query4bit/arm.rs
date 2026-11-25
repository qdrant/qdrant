//! NEON kernels for the symmetric 4-bit paths (`score_4bit_internal*`) on
//! aarch64.  The codebook is stored as signed i8 (`CODEBOOK_I8`).

use super::super::{BLOCK_BYTES, BlockedFactors};
use super::{CODEBOOK_I8, FOLDED_MAX_DIM, centroid_dot};

/// NEON implementation of [`super::score_4bit_internal`].  Both PQ-encoded
/// vectors are unpacked into i8 codebook values via `vqtbl1q_s8`, multiplied
/// signed-signed via `vmull_s8` (16 256-max product fits i16), and pair-added
/// into an i32×4 accumulator.  Reconstruction divides by `c_scale²`.
///
/// # Safety
/// CPU must support the `neon` feature (always true on aarch64).
#[target_feature(enable = "neon")]
pub unsafe fn score_4bit_internal_neon(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::aarch64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_neon: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let codebook = vld1q_s8(CODEBOOK_I8.as_ptr());
        let mut acc = vdupq_n_s32(0);
        let nibble_mask = vdup_n_u8(0x0F);

        let n_full = a.len() / 8;
        for i in 0..n_full {
            let va = vld1_u8(a.as_ptr().add(i * 8));
            let va_lo = vand_u8(va, nibble_mask);
            let va_hi = vshr_n_u8(va, 4);
            let a_idx = vcombine_u8(vzip1_u8(va_lo, va_hi), vzip2_u8(va_lo, va_hi));

            let vb = vld1_u8(b.as_ptr().add(i * 8));
            let vb_lo = vand_u8(vb, nibble_mask);
            let vb_hi = vshr_n_u8(vb, 4);
            let b_idx = vcombine_u8(vzip1_u8(vb_lo, vb_hi), vzip2_u8(vb_lo, vb_hi));

            let c_a = vqtbl1q_s8(codebook, a_idx);
            let c_b = vqtbl1q_s8(codebook, b_idx);

            let prod_lo = vmull_s8(vget_low_s8(c_a), vget_low_s8(c_b));
            let prod_hi = vmull_high_s8(c_a, c_b);

            acc = vpadalq_s16(acc, prod_lo);
            acc = vpadalq_s16(acc, prod_hi);
        }

        let simd_bytes = n_full * 8;
        let acc_i64 = i64::from(vaddvq_s32(acc))
            + super::score_4bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        centroid_dot(acc_i64 as f32)
    }
}

/// SDOT variant of [`score_4bit_internal_neon`].  Two independent `i32×4`
/// accumulators consume two chunks (32 elements) per iteration.
///
/// # Safety
/// CPU must support `neon` and `dotprod`.
#[target_feature(enable = "neon,dotprod")]
pub unsafe fn score_4bit_internal_neon_sdot(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::aarch64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_neon_sdot: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let codebook = vld1q_s8(CODEBOOK_I8.as_ptr());
        let mut acc_0 = vdupq_n_s32(0);
        let mut acc_1 = vdupq_n_s32(0);
        let nibble_mask_q = vdupq_n_u8(0x0F);

        let n_pairs = a.len() / 16;

        for i in 0..n_pairs {
            let va = vld1q_u8(a.as_ptr().add(16 * i));
            let va_lo = vandq_u8(va, nibble_mask_q);
            let va_hi = vshrq_n_u8(va, 4);
            let a_idx_0 = vzip1q_u8(va_lo, va_hi);
            let a_idx_1 = vzip2q_u8(va_lo, va_hi);

            let vb = vld1q_u8(b.as_ptr().add(16 * i));
            let vb_lo = vandq_u8(vb, nibble_mask_q);
            let vb_hi = vshrq_n_u8(vb, 4);
            let b_idx_0 = vzip1q_u8(vb_lo, vb_hi);
            let b_idx_1 = vzip2q_u8(vb_lo, vb_hi);

            let c_a_0 = vqtbl1q_s8(codebook, a_idx_0);
            let c_a_1 = vqtbl1q_s8(codebook, a_idx_1);
            let c_b_0 = vqtbl1q_s8(codebook, b_idx_0);
            let c_b_1 = vqtbl1q_s8(codebook, b_idx_1);

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

        let acc = vaddq_s32(acc_0, acc_1);
        let simd_bytes = n_pairs * 16;
        let acc_i64 = i64::from(vaddvq_s32(acc))
            + super::score_4bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        centroid_dot(acc_i64 as f32)
    }
}

// ------------------------------------------------------------------
// score_4bit_internal_folded — TQ+ symmetric path. Each side's signed
// codebook value is scaled by its coordinate's Q15 factor with
// `vqrdmulhq_s16`, the same rounding as `fold_q15`, which keeps it within
// i8; `vmlal_s16` then sums the products into i32 lanes:
//   per product:   |q_a · q_b| ≤ 127 · 127 = 16 129
//   total:         < 2¹⁶ · 16 384 = 2³⁰ at `FOLDED_MAX_DIM` ✓
// ------------------------------------------------------------------

/// NEON implementation of [`super::score_4bit_internal_folded`]. 16 coords
/// per iteration.
///
/// # Safety
/// CPU must support `neon`.
#[target_feature(enable = "neon")]
pub unsafe fn score_4bit_internal_folded_neon(a: &[u8], b: &[u8], factors: &[i16]) -> i64 {
    use core::arch::aarch64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_folded_neon: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    assert_eq!(
        factors.len(),
        2 * a.len(),
        "score_4bit_internal_folded_neon: factors length {} != 2 · a.len() {}",
        factors.len(),
        2 * a.len(),
    );
    assert!(factors.len() <= FOLDED_MAX_DIM);

    unsafe {
        let codebook = vld1q_s8(CODEBOOK_I8.as_ptr());
        let mut acc = vdupq_n_s32(0);
        let nibble_mask = vdup_n_u8(0x0F);

        // 8 bytes per source = 16 coords per iter.
        let n_full = a.len() / 8;
        for i in 0..n_full {
            let va = vld1_u8(a.as_ptr().add(i * 8));
            let va_lo = vand_u8(va, nibble_mask);
            let va_hi = vshr_n_u8(va, 4);
            let a_idx = vcombine_u8(vzip1_u8(va_lo, va_hi), vzip2_u8(va_lo, va_hi));

            let vb = vld1_u8(b.as_ptr().add(i * 8));
            let vb_lo = vand_u8(vb, nibble_mask);
            let vb_hi = vshr_n_u8(vb, 4);
            let b_idx = vcombine_u8(vzip1_u8(vb_lo, vb_hi), vzip2_u8(vb_lo, vb_hi));

            let c_a = vqtbl1q_s8(codebook, a_idx);
            let c_b = vqtbl1q_s8(codebook, b_idx);
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

        let simd_bytes = n_full * 8;
        i64::from(vaddvq_s32(acc))
            + super::score_4bit_internal_folded_scalar(
                &a[simd_bytes..],
                &b[simd_bytes..],
                &factors[2 * simd_bytes..],
            )
    }
}

// ------------------------------------------------------------------
// Block order — the TQ+ symmetric score of `super::super::BlockedFactors` and
// `super::super::FoldedQuery`, the same sum as `score_4bit_internal_folded`. A
// block of `BLOCK_BYTES` code bytes is read as its 32 low nibbles, then its 32
// high nibbles (its even, then its odd dimensions), not interleaved back into
// dimension order: the factors and a folded query are stored in that order.
// Same instructions as `score_4bit_internal_folded_neon`: `tbl` looks the codes
// up, `sqrdmulh` folds them widened to i16 (`(2·c·f + 2¹⁵) >> 16`, which equals
// `fold_q15`'s `(c·f + 2¹⁴) >> 15`), `smlal` sums the products in i32 lanes:
//   per product:   |q_a · q_b| ≤ 127 · 127 = 16 129
//   total:         < 2¹⁶ · 2¹⁴ = 2³⁰ at `FOLDED_MAX_DIM` ✓
// ------------------------------------------------------------------

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

/// Signed codebook values of the codes of block `block` of `codes`: the low nibbles, then the
/// high ones, 16 per vector.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn unpack_blocked_neon(
    codebook: core::arch::aarch64::int8x16_t,
    codes: &[u8],
    block: usize,
) -> [core::arch::aarch64::int8x16_t; 4] {
    use core::arch::aarch64::*;
    let [bytes_0, bytes_1] = unsafe { load_block(codes, block) };
    let mask = vdupq_n_u8(0x0F);
    [
        vqtbl1q_s8(codebook, vandq_u8(bytes_0, mask)),
        vqtbl1q_s8(codebook, vandq_u8(bytes_1, mask)),
        vqtbl1q_s8(codebook, vshrq_n_u8(bytes_0, 4)),
        vqtbl1q_s8(codebook, vshrq_n_u8(bytes_1, 4)),
    ]
}

/// [`super::fold_4bit_blocked`] on NEON.
///
/// # Safety
///
/// The CPU must support NEON; `codes`, `values` and the factors must cover the factors' blocks.
#[target_feature(enable = "neon")]
pub(super) unsafe fn fold_4bit_blocked_neon(
    factors: &BlockedFactors,
    codes: &[u8],
    values: &mut [i16],
) {
    use core::arch::aarch64::*;
    unsafe {
        let codebook = vld1q_s8(CODEBOOK_I8.as_ptr());
        for block in 0..factors.layout.blocks() {
            let codes = unpack_blocked_neon(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (4 * block + part) * PART;
                let folded = fold_part(code, load_i16_part(&factors.factors, position));
                store_i16_part(values, position, folded);
            }
        }
    }
}

/// [`super::score_4bit_blocked`] on NEON.
///
/// # Safety
///
/// The CPU must support NEON; `codes`, `query` and the factors must cover the factors' blocks.
#[target_feature(enable = "neon")]
pub(super) unsafe fn score_4bit_blocked_neon(
    query: &[i16],
    factors: &BlockedFactors,
    codes: &[u8],
) -> i64 {
    use core::arch::aarch64::*;
    unsafe {
        let codebook = vld1q_s8(CODEBOOK_I8.as_ptr());
        let mut acc = zero_accumulators();
        for block in 0..factors.layout.blocks() {
            let codes = unpack_blocked_neon(codebook, codes, block);
            for (part, code) in codes.into_iter().enumerate() {
                let position = (4 * block + part) * PART;
                let folded = fold_part(code, load_i16_part(&factors.factors, position));
                multiply_add(&mut acc[part % 2], load_i16_part(query, position), folded);
            }
        }
        reduce_accumulators(&acc)
    }
}

/// [`super::score_4bit_internal_blocked`] on NEON.
///
/// # Safety
///
/// The CPU must support NEON; `a`, `b` and the factors must cover the factors' blocks.
#[target_feature(enable = "neon")]
pub(super) unsafe fn score_4bit_internal_blocked_neon(
    factors: &BlockedFactors,
    a: &[u8],
    b: &[u8],
) -> i64 {
    use core::arch::aarch64::*;
    unsafe {
        let codebook = vld1q_s8(CODEBOOK_I8.as_ptr());
        let mut acc = zero_accumulators();
        for block in 0..factors.layout.blocks() {
            let codes_a = unpack_blocked_neon(codebook, a, block);
            let codes_b = unpack_blocked_neon(codebook, b, block);
            for part in 0..4 {
                let factor = load_i16_part(&factors.factors, (4 * block + part) * PART);
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
    use super::super::{score_4bit_internal_folded_scalar, score_4bit_internal_scalar};
    use super::{
        score_4bit_internal_folded_neon, score_4bit_internal_neon, score_4bit_internal_neon_sdot,
    };

    /// Build deterministic non-negative Q15 factors of length `2 · vec_bytes`
    /// for parity tests of the folded kernel.
    fn random_factors(rng: &mut StdRng, vec_bytes: usize) -> Vec<i16> {
        use rand::RngExt;
        (0..2 * vec_bytes)
            .map(|_| rng.random_range(0..=i16::MAX))
            .collect()
    }

    /// Parity: NEON `score_4bit_internal` variants must reproduce the scalar
    /// reference bit-exactly.  Integer accumulators are identical across
    /// paths (no saturating intermediates at parity-test dims), so the f32
    /// outputs should match `assert_eq!` down to the last ulp.
    #[test]
    fn test_score_neon_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let scalar = score_4bit_internal_scalar(&vec_a, &vec_b);

            let neon = unsafe { score_4bit_internal_neon(&vec_a, &vec_b) };
            assert_eq!(
                scalar, neon,
                "score: scalar {scalar} != neon {neon} at dim {dim}"
            );

            if std::arch::is_aarch64_feature_detected!("dotprod") {
                let sdot = unsafe { score_4bit_internal_neon_sdot(&vec_a, &vec_b) };
                assert_eq!(
                    scalar, sdot,
                    "score: scalar {scalar} != sdot {sdot} at dim {dim}"
                );
            }
        }
    }

    /// Parity for the folded kernel: NEON must match the scalar reference
    /// bit-exactly across our matryoshka corner-case dims.
    #[test]
    fn test_score_folded_neon_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(0xBEEF);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let factors = random_factors(&mut rng, vec_a.len());
            let scalar = score_4bit_internal_folded_scalar(&vec_a, &vec_b, &factors);
            let neon = unsafe { score_4bit_internal_folded_neon(&vec_a, &vec_b, &factors) };
            assert_eq!(
                scalar, neon,
                "folded: scalar {scalar} != neon {neon} at dim {dim}"
            );
        }
    }

    /// Saturation-safety for the folded NEON kernel at 64K dims: must
    /// match the i64 scalar reference under worst-case inputs.
    #[test]
    fn test_score_folded_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![15; dim];
        let vec_a = pack_codes(&indices, 4);
        let vec_b = pack_codes(&indices, 4);
        let factors: Vec<i16> = vec![i16::MAX; dim];

        let scalar = score_4bit_internal_folded_scalar(&vec_a, &vec_b, &factors);
        unsafe {
            let neon = score_4bit_internal_folded_neon(&vec_a, &vec_b, &factors);
            assert_eq!(scalar, neon, "folded score neon overflow at dim={dim}");
        }
    }

    /// Saturation-safety at 64K: both vectors every index 15 → every product
    /// hits `CODEBOOK_I8[15]² = 127² = 16 129`.  Total = 16 384 × 16 129 ≈
    /// 264 M which comfortably fits i32 (~8× headroom), but any intermediate
    /// i16 accumulator (pre-vpadalq) maxes out at 16 129 < 32 767 so no
    /// saturation.  SDOT accumulates into i32 directly.
    #[test]
    fn test_score_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![15; dim]; // CODEBOOK_I8[15] = +127
        let vec_a = pack_codes(&indices, 4);
        let vec_b = pack_codes(&indices, 4);

        let scalar = score_4bit_internal_scalar(&vec_a, &vec_b);

        unsafe {
            let neon = score_4bit_internal_neon(&vec_a, &vec_b);
            assert_eq!(scalar, neon, "score neon disagrees at dim={dim}");

            if std::arch::is_aarch64_feature_detected!("dotprod") {
                let sdot = score_4bit_internal_neon_sdot(&vec_a, &vec_b);
                assert_eq!(scalar, sdot, "score sdot disagrees at dim={dim}");
            }
        }
    }
}
