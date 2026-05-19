//! NEON SIMD paths for [`Query2bitSimd`] on aarch64.
//!
//! Codebook storage mirrors [`super::query4bit::arm`] — full signed `i8` with
//! no offset — so `vmull_s8` / `sdot` run as true signed-signed multiplies.
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

use super::{CODEBOOK_I8, CODEBOOK_SCALE, QUERY_HIGH_COEF, Query2bitSimd};

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

impl Query2bitSimd {
    /// NEON implementation of [`Query2bitSimd::dotprod_raw`].
    ///
    /// # Safety
    /// CPU must support the `neon` feature (always true on aarch64).
    #[target_feature(enable = "neon")]
    pub unsafe fn dotprod_raw_neon(&self, vector: &[u8]) -> i64 {
        use core::arch::aarch64::*;

        assert_eq!(
            vector.len(),
            self.expected_vector_bytes(),
            "Query2bitSimd::dotprod_raw_neon: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.expected_vector_bytes(),
        );

        unsafe {
            let mut acc_low = vdupq_n_s32(0);
            let mut acc_high = vdupq_n_s32(0);

            for (chunk_idx, [low, high]) in self.query_data.iter().enumerate() {
                let c = unpack_16_codes(vector.as_ptr().add(chunk_idx * 4));

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

            let full =
                i64::from(vaddvq_s32(acc_low)) + QUERY_HIGH_COEF * i64::from(vaddvq_s32(acc_high));
            full + self.dotprod_raw_tail(vector)
        }
    }

    /// ARMv8.2-A Dot Product variant.  Uses SDOT to sum 4 × i8 × i8 products
    /// per i32 lane per instruction, with a 2× unroll (2 chunks per iter) +
    /// a 1-chunk tail for odd chunk counts (allows `dim % 16 == 0`, matching
    /// the scalar / plain-NEON contract).
    ///
    /// # Safety
    /// CPU must support `neon` and `dotprod`.
    #[target_feature(enable = "neon,dotprod")]
    pub unsafe fn dotprod_raw_neon_sdot(&self, vector: &[u8]) -> i64 {
        use core::arch::aarch64::*;

        assert_eq!(
            vector.len(),
            self.expected_vector_bytes(),
            "Query2bitSimd::dotprod_raw_neon_sdot: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.expected_vector_bytes(),
        );

        unsafe {
            let mut acc_low_0 = vdupq_n_s32(0);
            let mut acc_low_1 = vdupq_n_s32(0);
            let mut acc_high_0 = vdupq_n_s32(0);
            let mut acc_high_1 = vdupq_n_s32(0);

            let n_pairs = self.query_data.len() / 2;
            let data_ptr = vector.as_ptr();
            for i in 0..n_pairs {
                let [low_0, high_0] = &self.query_data[2 * i];
                let [low_1, high_1] = &self.query_data[2 * i + 1];

                let c_0 = unpack_16_codes(data_ptr.add(8 * i));
                let c_1 = unpack_16_codes(data_ptr.add(8 * i + 4));

                let q_low_0 = vld1q_s8(low_0.as_ptr());
                let q_high_0 = vld1q_s8(high_0.as_ptr());
                let q_low_1 = vld1q_s8(low_1.as_ptr());
                let q_high_1 = vld1q_s8(high_1.as_ptr());

                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_low_0,
                    a = in(vreg) q_low_0,
                    b = in(vreg) c_0,
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
                    acc = inout(vreg) acc_low_1,
                    a = in(vreg) q_low_1,
                    b = in(vreg) c_1,
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

            // Tail: odd chunk count → one extra chunk via single SDOT per half.
            if self.query_data.len() % 2 == 1 {
                let tail = 2 * n_pairs;
                let [low_t, high_t] = &self.query_data[tail];
                let c_t = unpack_16_codes(data_ptr.add(4 * tail));
                let q_low_t = vld1q_s8(low_t.as_ptr());
                let q_high_t = vld1q_s8(high_t.as_ptr());

                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_low_0,
                    a = in(vreg) q_low_t,
                    b = in(vreg) c_t,
                    options(pure, nomem, nostack, preserves_flags),
                );
                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_high_0,
                    a = in(vreg) q_high_t,
                    b = in(vreg) c_t,
                    options(pure, nomem, nostack, preserves_flags),
                );
            }

            let acc_low = vaddq_s32(acc_low_0, acc_low_1);
            let acc_high = vaddq_s32(acc_high_0, acc_high_1);
            let full =
                i64::from(vaddvq_s32(acc_low)) + QUERY_HIGH_COEF * i64::from(vaddvq_s32(acc_high));
            full + self.dotprod_raw_tail(vector)
        }
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
        acc_i64 as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
    }
}

/// SDOT variant of [`score_2bit_internal_neon`].  2× chunk unroll mirrors
/// [`Query2bitSimd::dotprod_raw_neon_sdot`].
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
        acc_i64 as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
    }
}

// ------------------------------------------------------------------
// score_2bit_internal_weighted — TQ+ symmetric path. Same overflow
// pattern as the 4-bit NEON weighted kernel (i64 acc via `vpaddlq_s32`
// to avoid i32 saturation at large dim).
// ------------------------------------------------------------------

/// NEON weighted variant of [`super::score_2bit_internal`]. 16 coords per
/// iteration; products go through `vmull_s8` then per-pair multiply by
/// i16 weights via `vmull_s16`, pair-summed into an i64 accumulator.
///
/// # Safety
/// CPU must support `neon`.
#[target_feature(enable = "neon")]
pub unsafe fn score_2bit_internal_weighted_neon(a: &[u8], b: &[u8], weights: &[i16]) -> i64 {
    use core::arch::aarch64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_2bit_internal_weighted_neon: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    assert_eq!(
        weights.len(),
        4 * a.len(),
        "score_2bit_internal_weighted_neon: weights length {} != 4 · a.len() {}",
        weights.len(),
        4 * a.len(),
    );

    unsafe {
        let mut acc = vdupq_n_s64(0);

        // 4 bytes per source = 16 coords per iter.
        let n_full = a.len() / 4;
        for i in 0..n_full {
            let c_a = unpack_16_codes(a.as_ptr().add(i * 4));
            let c_b = unpack_16_codes(b.as_ptr().add(i * 4));

            let prod_lo = vmull_s8(vget_low_s8(c_a), vget_low_s8(c_b));
            let prod_hi = vmull_high_s8(c_a, c_b);

            let w_lo = vld1q_s16(weights.as_ptr().add(16 * i));
            let w_hi = vld1q_s16(weights.as_ptr().add(16 * i + 8));

            let pw_a = vmull_s16(vget_low_s16(prod_lo), vget_low_s16(w_lo));
            let pw_b = vmull_high_s16(prod_lo, w_lo);
            let pw_c = vmull_s16(vget_low_s16(prod_hi), vget_low_s16(w_hi));
            let pw_d = vmull_high_s16(prod_hi, w_hi);

            acc = vaddq_s64(acc, vpaddlq_s32(pw_a));
            acc = vaddq_s64(acc, vpaddlq_s32(pw_b));
            acc = vaddq_s64(acc, vpaddlq_s32(pw_c));
            acc = vaddq_s64(acc, vpaddlq_s32(pw_d));
        }

        let simd_sum = vaddvq_s64(acc);
        let simd_bytes = n_full * 4;
        let tail = super::score_2bit_internal_weighted_scalar(
            &a[simd_bytes..],
            &b[simd_bytes..],
            &weights[4 * simd_bytes..],
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
    use super::super::{
        Query2bitSimd, score_2bit_internal_scalar, score_2bit_internal_weighted_scalar,
    };
    use super::{
        score_2bit_internal_neon, score_2bit_internal_neon_sdot, score_2bit_internal_weighted_neon,
    };

    /// Build deterministic non-negative i16 weights of length `4 · vec_bytes`
    /// for parity tests of the 2-bit weighted kernel.
    fn random_weights(rng: &mut StdRng, vec_bytes: usize) -> Vec<i16> {
        use rand::RngExt;
        (0..4 * vec_bytes)
            .map(|_| rng.random_range(0..=i16::MAX))
            .collect()
    }

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

    /// Saturation safety for query-vs-vector at dim=64K: query maxed out,
    /// every lane pointing at CODEBOOK_I8[3] = +127.
    #[test]
    fn test_saturation_safety_64k() {
        let dim = 65_536;
        let query = vec![1.0_f32; dim];
        let indices: Vec<u8> = vec![3; dim]; // +127 centroid
        let vector = pack_codes(&indices, 2);

        let q = Query2bitSimd::new(&query);
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

    /// Parity for the 2-bit weighted kernel: NEON must match the scalar
    /// reference bit-exactly across our matryoshka corner-case dims.
    #[test]
    fn test_score_weighted_neon_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(0xBEEF);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let weights = random_weights(&mut rng, vec_a.len());
            let scalar = score_2bit_internal_weighted_scalar(&vec_a, &vec_b, &weights);
            let neon = unsafe { score_2bit_internal_weighted_neon(&vec_a, &vec_b, &weights) };
            assert_eq!(
                scalar, neon,
                "weighted: scalar {scalar} != neon {neon} at dim {dim}"
            );
        }
    }

    /// Saturation-safety for the 2-bit weighted NEON kernel at 64K dims:
    /// must match i64 scalar reference under worst-case inputs.
    #[test]
    fn test_score_weighted_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![3; dim];
        let vec_a = pack_codes(&indices, 2);
        let vec_b = pack_codes(&indices, 2);
        let max_weight: i16 = i16::MAX;
        let weights: Vec<i16> = vec![max_weight; dim];

        let scalar = score_2bit_internal_weighted_scalar(&vec_a, &vec_b, &weights);
        unsafe {
            let neon = score_2bit_internal_weighted_neon(&vec_a, &vec_b, &weights);
            assert_eq!(scalar, neon, "weighted score neon overflow at dim={dim}");
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
