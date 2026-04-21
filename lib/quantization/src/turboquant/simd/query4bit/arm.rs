//! NEON SIMD paths for [`Query4bitSimd`] on aarch64.
//!
//! The codebook is stored as signed i8 here (`CODEBOOK_I8`), so `vmull_s8` and
//! `sdot` operate on true i8×i8 products — no bias correction.  The
//! combining coefficient `QUERY_HIGH_COEF = 256` pairs with full-range i8
//! query halves, yielding ~15.9-bit query precision.

use super::{CODEBOOK_I8, QUERY_HIGH_COEF, Query4bitSimd};

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
            for ([low, high], v_chunk) in self.query_data.iter().zip(vector.chunks_exact(8)) {
                // Unpack 8 packed bytes into 16 nibble indices:
                //   [byte0.lo, byte0.hi, byte1.lo, byte1.hi, ..., byte7.lo, byte7.hi].
                let v_packed = vld1_u8(v_chunk.as_ptr());
                let v_lo = vand_u8(v_packed, nibble_mask);
                let v_hi = vshr_n_u8(v_packed, 4);
                let v = vcombine_u8(vzip1_u8(v_lo, v_hi), vzip2_u8(v_lo, v_hi));

                let c = vqtbl1q_s8(codebook, v);

                // Signed i8 query halves — one 16-byte load per half.
                let q_low = vld1q_s8(low.as_ptr());
                let q_high = vld1q_s8(high.as_ptr());

                // i8 × i8 → i16; per product magnitude ≤ 127 × 128 = 16 256 fits in i16.
                let prod_low_lo = vmull_s8(vget_low_s8(q_low), vget_low_s8(c));
                let prod_low_hi = vmull_high_s8(q_low, c);
                let prod_high_lo = vmull_s8(vget_low_s8(q_high), vget_low_s8(c));
                let prod_high_hi = vmull_high_s8(q_high, c);

                // Pairwise-add the i16×8 products into the i32×4 accumulators.
                acc_low = vpadalq_s16(acc_low, prod_low_lo);
                acc_low = vpadalq_s16(acc_low, prod_low_hi);
                acc_high = vpadalq_s16(acc_high, prod_high_lo);
                acc_high = vpadalq_s16(acc_high, prod_high_hi);
            }

            // dot_raw = Σ (low + 256·high) · c_signed = acc_low + 256 · acc_high.
            i64::from(vaddvq_s32(acc_low)) + QUERY_HIGH_COEF * i64::from(vaddvq_s32(acc_high))
        }
    }

    /// ARMv8.2-A Dot Product variant.  Uses `SDOT` to sum four i8×i8 products
    /// per i32 lane per instruction, emitted via inline asm because
    /// `vdotq_s32` is still unstable (rust-lang/rust#117224).
    ///
    /// 2× unrolled: two chunks per iteration with four independent `i32×4`
    /// accumulators (two for low, two for high) break the single dependency
    /// chain of a naive implementation.  On Apple M-series SDOT has ~3-cycle
    /// latency at 4/cycle throughput, and four parallel chains lift
    /// throughput ~7–20% over a 1× version (larger dims benefit more —
    /// latency dominates there).
    ///
    /// # Safety
    /// CPU must support `neon` and `dotprod`.
    #[target_feature(enable = "neon,dotprod")]
    pub unsafe fn dotprod_raw_neon_sdot(&self, vector: &[u8]) -> i64 {
        use core::arch::aarch64::*;

        unsafe {
            let codebook = vld1q_s8(CODEBOOK_I8.as_ptr());
            let mut acc_low_0 = vdupq_n_s32(0);
            let mut acc_low_1 = vdupq_n_s32(0);
            let mut acc_high_0 = vdupq_n_s32(0);
            let mut acc_high_1 = vdupq_n_s32(0);
            let nibble_mask_q = vdupq_n_u8(0x0F);

            // `query_data.len()` is guaranteed even by the dim-%32 precondition,
            // so every pair is consumed and there is no tail to handle.
            let chunks = self.query_data.as_slice();
            let n_pairs = chunks.len() / 2;

            for i in 0..n_pairs {
                let [low_0, high_0] = chunks[2 * i];
                let [low_1, high_1] = chunks[2 * i + 1];

                // One 16-byte load covers both chunks' 8 packed bytes each.
                let v_packed = vld1q_u8(vector.as_ptr().add(16 * i));
                let v_lo = vandq_u8(v_packed, nibble_mask_q);
                let v_hi = vshrq_n_u8(v_packed, 4);
                let v_0 = vzip1q_u8(v_lo, v_hi);
                let v_1 = vzip2q_u8(v_lo, v_hi);
                let c_0 = vqtbl1q_s8(codebook, v_0);
                let c_1 = vqtbl1q_s8(codebook, v_1);

                let q_low_0 = vld1q_s8(low_0.as_ptr());
                let q_high_0 = vld1q_s8(high_0.as_ptr());
                let q_low_1 = vld1q_s8(low_1.as_ptr());
                let q_high_1 = vld1q_s8(high_1.as_ptr());

                // Four independent SDOT dependency chains — 2× unroll for ILP.
                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_low_0,
                    a = in(vreg) q_low_0,
                    b = in(vreg) c_0,
                    options(pure, nomem, nostack, preserves_flags),
                );
                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_low_1,
                    a = in(vreg) q_low_1,
                    b = in(vreg) c_1,
                    options(pure, nomem, nostack, preserves_flags),
                );
                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_high_0,
                    a = in(vreg) q_high_0,
                    b = in(vreg) c_0,
                    options(pure, nomem, nostack, preserves_flags),
                );
                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_high_1,
                    a = in(vreg) q_high_1,
                    b = in(vreg) c_1,
                    options(pure, nomem, nostack, preserves_flags),
                );
            }

            let acc_low = vaddq_s32(acc_low_0, acc_low_1);
            let acc_high = vaddq_s32(acc_high_0, acc_high_1);
            i64::from(vaddvq_s32(acc_low)) + QUERY_HIGH_COEF * i64::from(vaddvq_s32(acc_high))
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::Query4bitSimd;
    use super::super::shared::{PARITY_DIMS, pack_nibbles, random_inputs};

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

    #[test]
    fn test_neon_sdot_matches_scalar() {
        if !std::arch::is_aarch64_feature_detected!("dotprod") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (simd_query, vector) = random_inputs(&mut rng, dim);
            let scalar = simd_query.dotprod_raw(&vector);
            let sdot = unsafe { simd_query.dotprod_raw_neon_sdot(&vector) };
            assert_eq!(scalar, sdot, "scalar {scalar} != sdot {sdot} at dim {dim}");
        }
    }

    /// Single saturation-safety check at an extreme dim (16K) with the
    /// worst-case combination: query maxed out and every lane of the vector
    /// pointing at the extreme-magnitude codebook slot.  Scalar is the
    /// reference (i64 throughout, saturation-free by construction); each
    /// SIMD path must match it exactly.  A mismatch proves that some
    /// intermediate integer saturated or overflowed.
    #[test]
    fn test_saturation_safety_16k() {
        let dim = 16_384;
        let query = vec![1.0_f32; dim];
        let indices: Vec<u8> = vec![15; dim]; // CODEBOOK_I8[15] = +127
        let vector = pack_nibbles(&indices);

        let q = Query4bitSimd::new(&query);
        let scalar = q.dotprod_raw(&vector);

        unsafe {
            let neon = q.dotprod_raw_neon(&vector);
            assert_eq!(scalar, neon, "neon disagrees at dim={dim}");

            if std::arch::is_aarch64_feature_detected!("dotprod") {
                let sdot = q.dotprod_raw_neon_sdot(&vector);
                assert_eq!(scalar, sdot, "sdot disagrees at dim={dim}");
            }
        }
    }
}
