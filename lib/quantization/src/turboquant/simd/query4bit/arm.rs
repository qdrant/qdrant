//! NEON kernels for the symmetric 4-bit paths (`score_4bit_internal*`) on
//! aarch64.  The codebook is stored as signed i8 (`CODEBOOK_I8`).

use super::{CODEBOOK_I8, CODEBOOK_SCALE};

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
        acc_i64 as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
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
        acc_i64 as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
    }
}

// ------------------------------------------------------------------
// score_4bit_internal_weighted — TQ+ symmetric path with per-coord
// `D'²` weighting. `weights[i]` is `i16` (non-negative, capped at
// `i16::MAX − 1` — see `ErrorCorrection::d_prime_sq_i16` doc), which
// matches the SIMD load directly. Same overflow story as the x64
// path: per-coord product up to ~5.28e8, sum at dim=64 K reaches
// ~1.7e10 — must accumulate into i64 lanes. We pair-sum i32 → i64
// every iteration via `vpaddlq_s32`.
// ------------------------------------------------------------------

/// NEON weighted variant of [`super::score_4bit_internal`]. 16 coords per
/// iteration; products go through `vmull_s8` (i8 → i16) then per-pair
/// multiply by i16 weights via `vmull_s16` (i16 → i32), pair-summed into
/// an i64 accumulator.
///
/// # Safety
/// CPU must support `neon`.
#[target_feature(enable = "neon")]
pub unsafe fn score_4bit_internal_weighted_neon(a: &[u8], b: &[u8], weights: &[i16]) -> i64 {
    use core::arch::aarch64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_weighted_neon: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    assert_eq!(
        weights.len(),
        2 * a.len(),
        "score_4bit_internal_weighted_neon: weights length {} != 2 · a.len() {}",
        weights.len(),
        2 * a.len(),
    );

    unsafe {
        let codebook = vld1q_s8(CODEBOOK_I8.as_ptr());
        let mut acc = vdupq_n_s64(0); // 2 i64 lanes
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

            // c_a × c_b in i16 (max 16129 fits — same bound as the unweighted kernel).
            let prod_lo = vmull_s8(vget_low_s8(c_a), vget_low_s8(c_b)); // i16×8
            let prod_hi = vmull_high_s8(c_a, c_b); // i16×8

            // Load 16 i16 weights = 2 q regs.
            let w_lo = vld1q_s16(weights.as_ptr().add(16 * i));
            let w_hi = vld1q_s16(weights.as_ptr().add(16 * i + 8));

            // Multiply each i16 pair → i32. Each `vmull_s16` produces 4 i32 from
            // the low 4 i16 lanes; `vmull_high_s16` does the upper 4.
            let pw_a = vmull_s16(vget_low_s16(prod_lo), vget_low_s16(w_lo)); // i32×4
            let pw_b = vmull_high_s16(prod_lo, w_lo); // i32×4
            let pw_c = vmull_s16(vget_low_s16(prod_hi), vget_low_s16(w_hi)); // i32×4
            let pw_d = vmull_high_s16(prod_hi, w_hi); // i32×4

            // Pair-sum each i32×4 → i64×2 and accumulate.
            acc = vaddq_s64(acc, vpaddlq_s32(pw_a));
            acc = vaddq_s64(acc, vpaddlq_s32(pw_b));
            acc = vaddq_s64(acc, vpaddlq_s32(pw_c));
            acc = vaddq_s64(acc, vpaddlq_s32(pw_d));
        }

        let simd_sum = vaddvq_s64(acc);
        let simd_bytes = n_full * 8;
        let tail = super::score_4bit_internal_weighted_scalar(
            &a[simd_bytes..],
            &b[simd_bytes..],
            &weights[2 * simd_bytes..],
        );
        simd_sum + tail
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::super::shared::pack_codes;
    use super::super::shared::{PARITY_DIMS, random_inputs};
    use super::super::{score_4bit_internal_scalar, score_4bit_internal_weighted_scalar};
    use super::{
        score_4bit_internal_neon, score_4bit_internal_neon_sdot, score_4bit_internal_weighted_neon,
    };

    /// Build deterministic non-negative i16 weights of length `2 · vec_bytes`
    /// for parity tests of the weighted kernel.
    fn random_weights(rng: &mut StdRng, vec_bytes: usize) -> Vec<i16> {
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

    /// Parity for the weighted kernel: NEON must match the scalar reference
    /// bit-exactly across our matryoshka corner-case dims.
    #[test]
    fn test_score_weighted_neon_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(0xBEEF);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let weights = random_weights(&mut rng, vec_a.len());
            let scalar = score_4bit_internal_weighted_scalar(&vec_a, &vec_b, &weights);
            let neon = unsafe { score_4bit_internal_weighted_neon(&vec_a, &vec_b, &weights) };
            assert_eq!(
                scalar, neon,
                "weighted: scalar {scalar} != neon {neon} at dim {dim}"
            );
        }
    }

    /// Saturation-safety for the weighted NEON kernel at 64K dims: must
    /// match i64 scalar reference under worst-case inputs.
    #[test]
    fn test_score_weighted_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![15; dim];
        let vec_a = pack_codes(&indices, 4);
        let vec_b = pack_codes(&indices, 4);
        let max_weight: i16 = i16::MAX;
        let weights: Vec<i16> = vec![max_weight; dim];

        let scalar = score_4bit_internal_weighted_scalar(&vec_a, &vec_b, &weights);
        unsafe {
            let neon = score_4bit_internal_weighted_neon(&vec_a, &vec_b, &weights);
            assert_eq!(scalar, neon, "weighted score neon overflow at dim={dim}");
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
