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
    assert!(
        a.len().is_multiple_of(16),
        "score_4bit_internal_neon requires vector length to be a multiple of 16 bytes, got {}",
        a.len(),
    );

    unsafe {
        let codebook = vld1q_s8(CODEBOOK_I8.as_ptr());
        let mut acc = vdupq_n_s32(0);
        let nibble_mask = vdup_n_u8(0x0F);

        for (a_chunk, b_chunk) in a.chunks_exact(8).zip(b.chunks_exact(8)) {
            let va = vld1_u8(a_chunk.as_ptr());
            let va_lo = vand_u8(va, nibble_mask);
            let va_hi = vshr_n_u8(va, 4);
            let a_idx = vcombine_u8(vzip1_u8(va_lo, va_hi), vzip2_u8(va_lo, va_hi));

            let vb = vld1_u8(b_chunk.as_ptr());
            let vb_lo = vand_u8(vb, nibble_mask);
            let vb_hi = vshr_n_u8(vb, 4);
            let b_idx = vcombine_u8(vzip1_u8(vb_lo, vb_hi), vzip2_u8(vb_lo, vb_hi));

            let c_a = vqtbl1q_s8(codebook, a_idx);
            let c_b = vqtbl1q_s8(codebook, b_idx);

            // i8 × i8 → i16; per product magnitude ≤ 127 × 127 = 16 129 fits in i16.
            let prod_lo = vmull_s8(vget_low_s8(c_a), vget_low_s8(c_b));
            let prod_hi = vmull_high_s8(c_a, c_b);

            acc = vpadalq_s16(acc, prod_lo);
            acc = vpadalq_s16(acc, prod_hi);
        }

        let acc_i64 = i64::from(vaddvq_s32(acc));
        acc_i64 as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
    }
}

/// SDOT variant of [`score_4bit_internal_neon`].  Two independent `i32×4`
/// accumulators consume two chunks (32 elements) per iteration, following
/// the same 2× unroll pattern as [`Query4bitSimd::dotprod_raw_neon_sdot`].
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
    assert!(
        a.len().is_multiple_of(16),
        "score_4bit_internal_neon_sdot requires vector length to be a multiple of 16 bytes, got {}",
        a.len(),
    );

    unsafe {
        let codebook = vld1q_s8(CODEBOOK_I8.as_ptr());
        let mut acc_0 = vdupq_n_s32(0);
        let mut acc_1 = vdupq_n_s32(0);
        let nibble_mask_q = vdupq_n_u8(0x0F);

        // `a.len() / 16` == number of chunk-pairs (2 chunks = 32 elements each).
        let n_pairs = a.len() / 16;

        for i in 0..n_pairs {
            // One 16-byte load covers both chunks' 8 packed bytes each.
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

            // SDOT: acc[lane] += sum_{k=0..3} c_a[4·lane+k] · c_b[4·lane+k].
            // i8 × i8 is what we want; per lane per iter ≤ 4 · 127² = 64 516 in i32.
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
        let acc_i64 = i64::from(vaddvq_s32(acc));
        acc_i64 as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::shared::{PARITY_DIMS, pack_nibbles, random_inputs};
    use super::super::{Query4bitSimd, score_4bit_internal_scalar};
    use super::{score_4bit_internal_neon, score_4bit_internal_neon_sdot};

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

    /// Saturation-safety at 16K: both vectors every index 15 → every product
    /// hits `CODEBOOK_I8[15]² = 127² = 16 129`.  Total = 16 384 × 16 129 ≈
    /// 264 M which comfortably fits i32 (~8× headroom), but any intermediate
    /// i16 accumulator (pre-vpadalq) maxes out at 16 129 < 32 767 so no
    /// saturation.  SDOT accumulates into i32 directly.
    #[test]
    fn test_score_saturation_safety_16k() {
        let dim = 16_384;
        let indices: Vec<u8> = vec![15; dim]; // CODEBOOK_I8[15] = +127
        let vec_a = pack_nibbles(&indices);
        let vec_b = pack_nibbles(&indices);

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
