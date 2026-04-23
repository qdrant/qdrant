//! NEON SIMD paths for [`Query4bitSimd`] on aarch64.
//!
//! The codebook is stored as signed i8 here (`CODEBOOK_I8`), so `vmull_s8` and
//! `sdot` operate on true i8×i8 products — no bias correction.  The
//! combining coefficient `QUERY_HIGH_COEF = 256` pairs with full-range i8
//! query halves, yielding ~15.9-bit query precision.

use super::{CODEBOOK_I8, CODEBOOK_SCALE, QUERY_HIGH_COEF, Query4bitSimd};

impl Query4bitSimd {
    /// ARM NEON implementation of [`Query4bitSimd::dotprod_raw`].
    ///
    /// # Safety
    /// CPU must support the `neon` feature (always true on aarch64).
    #[target_feature(enable = "neon")]
    pub unsafe fn dotprod_raw_neon(&self, vector: &[u8]) -> i64 {
        use core::arch::aarch64::*;

        unsafe {
            let codebook = vld1q_s8(CODEBOOK_I8.as_ptr());
            let mut acc_low = vdupq_n_s32(0);
            let mut acc_high = vdupq_n_s32(0);

            let nibble_mask = vdup_n_u8(0x0F);
            for (chunk_idx, [low, high]) in self.query_data.iter().enumerate() {
                let v_packed = vld1_u8(vector.as_ptr().add(chunk_idx * 8));
                let v_lo = vand_u8(v_packed, nibble_mask);
                let v_hi = vshr_n_u8(v_packed, 4);
                let v = vcombine_u8(vzip1_u8(v_lo, v_hi), vzip2_u8(v_lo, v_hi));

                let c = vqtbl1q_s8(codebook, v);

                let q_low = vld1q_s8(low.as_ptr());
                let q_high = vld1q_s8(high.as_ptr());

                let prod_low_lo = vmull_s8(vget_low_s8(q_low), vget_low_s8(c));
                let prod_low_hi = vmull_high_s8(q_low, c);
                let prod_high_lo = vmull_s8(vget_low_s8(q_high), vget_low_s8(c));
                let prod_high_hi = vmull_high_s8(q_high, c);

                acc_low = vpadalq_s16(acc_low, prod_low_lo);
                acc_low = vpadalq_s16(acc_low, prod_low_hi);
                acc_high = vpadalq_s16(acc_high, prod_high_lo);
                acc_high = vpadalq_s16(acc_high, prod_high_hi);
            }

            // Tail: one extra chunk via NEON on a zero-padded 8-byte scratch.
            // Data bytes beyond `tail_dims / 2` are zero, and `tail_low /
            // tail_high` slots beyond `tail_dims` are zero — their products
            // contribute `0` to the final sum.
            if let Some(buf) = self.tail_chunk_scratch(vector) {
                let v_packed = vld1_u8(buf.as_ptr());
                let v_lo = vand_u8(v_packed, nibble_mask);
                let v_hi = vshr_n_u8(v_packed, 4);
                let v = vcombine_u8(vzip1_u8(v_lo, v_hi), vzip2_u8(v_lo, v_hi));
                let c = vqtbl1q_s8(codebook, v);

                let q_low = vld1q_s8(self.tail_low.as_ptr());
                let q_high = vld1q_s8(self.tail_high.as_ptr());

                let prod_low_lo = vmull_s8(vget_low_s8(q_low), vget_low_s8(c));
                let prod_low_hi = vmull_high_s8(q_low, c);
                let prod_high_lo = vmull_s8(vget_low_s8(q_high), vget_low_s8(c));
                let prod_high_hi = vmull_high_s8(q_high, c);

                acc_low = vpadalq_s16(acc_low, prod_low_lo);
                acc_low = vpadalq_s16(acc_low, prod_low_hi);
                acc_high = vpadalq_s16(acc_high, prod_high_lo);
                acc_high = vpadalq_s16(acc_high, prod_high_hi);
            }

            i64::from(vaddvq_s32(acc_low)) + QUERY_HIGH_COEF * i64::from(vaddvq_s32(acc_high))
        }
    }
}

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

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::shared::{PARITY_DIMS, pack_nibbles, random_inputs};
    use super::super::{Query4bitSimd, score_4bit_internal_scalar};
    use super::score_4bit_internal_neon;

    #[test]
    fn test_neon_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (simd_query, vector) = random_inputs(&mut rng, dim);
            let scalar = simd_query.dotprod_raw(&vector);
            let neon = unsafe { simd_query.dotprod_raw_neon(&vector) };
            assert_eq!(scalar, neon, "scalar {scalar} != neon {neon} at dim {dim}");
        }
    }

    /// Single saturation-safety check at an extreme dim (64K) with the
    /// worst-case combination: query maxed out and every lane of the vector
    /// pointing at the extreme-magnitude codebook slot.  Scalar is the
    /// reference (i64 throughout, saturation-free by construction); each
    /// SIMD path must match it exactly.  A mismatch proves that some
    /// intermediate integer saturated or overflowed.
    #[test]
    fn test_saturation_safety_64k() {
        let dim = 65_536;
        let query = vec![1.0_f32; dim];
        let indices: Vec<u8> = vec![15; dim]; // CODEBOOK_I8[15] = +127
        let vector = pack_nibbles(&indices);

        let q = Query4bitSimd::new(&query);
        let scalar = q.dotprod_raw(&vector);

        unsafe {
            let neon = q.dotprod_raw_neon(&vector);
            assert_eq!(scalar, neon, "neon disagrees at dim={dim}");
        }
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
        let vec_a = pack_nibbles(&indices);
        let vec_b = pack_nibbles(&indices);

        let scalar = score_4bit_internal_scalar(&vec_a, &vec_b);

        unsafe {
            let neon = score_4bit_internal_neon(&vec_a, &vec_b);
            assert_eq!(scalar, neon, "score neon disagrees at dim={dim}");
        }
    }
}
