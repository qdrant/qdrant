//! NEON popcount path for [`super::score_1bit_internal`].
//!
//! `vcntq_u8` returns per-byte popcounts (each ≤ 8), which we widen through
//! `vpaddlq_u8` (u8 → u16) and `vpadalq_u16` (accumulate 8 × u16 into 4 × u32
//! pairwise sums).  Per 16-byte chunk each u32 lane gains at most
//! `2 · 16 = 32`, so `u32::MAX / 32 ≈ 134 M` chunks (~2 GB) before the
//! accumulator lanes could overflow — far beyond any realistic vector.

/// NEON implementation of [`super::score_1bit_internal`].
///
/// # Safety
/// CPU must support the `neon` feature (always true on aarch64).
#[target_feature(enable = "neon")]
pub unsafe fn score_1bit_internal_neon(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::aarch64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_1bit_internal_neon: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let mut acc = vdupq_n_u32(0);
        let chunks = a.len() / 16;
        for i in 0..chunks {
            let va = vld1q_u8(a.as_ptr().add(i * 16));
            let vb = vld1q_u8(b.as_ptr().add(i * 16));
            let x = veorq_u8(va, vb);
            let cnt8 = vcntq_u8(x);
            let cnt16 = vpaddlq_u8(cnt8);
            acc = vpadalq_u16(acc, cnt16);
        }

        let mut popcnt = u64::from(vaddvq_u32(acc));

        let tail_start = chunks * 16;
        for i in tail_start..a.len() {
            popcnt += u64::from((a[i] ^ b[i]).count_ones());
        }

        super::popcount_to_score(a.len(), popcnt)
    }
}

impl<const BITS: usize> super::Query1bitSimd<BITS> {
    /// NEON implementation of [`super::Query1bitSimd::dotprod_raw`].
    ///
    /// Per block: one 16-byte data load + `BITS` plane loads; each plane's
    /// `vandq_u8 · vcntq_u8` pair is pair-added through `vpaddlq_u8` /
    /// `vpadalq_u16` into a dedicated `uint32x4_t` accumulator.  `BITS`
    /// accumulators live in registers (≤ 16 of 32 vregs available), so the
    /// inner loop is purely ALU.
    ///
    /// # Safety
    /// CPU must support the `neon` feature (always true on aarch64).
    #[target_feature(enable = "neon")]
    pub unsafe fn dotprod_raw_neon(&self, vector: &[u8]) -> i64 {
        use core::arch::aarch64::*;

        unsafe {
            let mut acc: [uint32x4_t; BITS] = core::array::from_fn(|_| vdupq_n_u32(0));

            // Main loop: full 128-dim blocks read directly from `vector`.
            for block_idx in 0..self.num_full_blocks() {
                let data = vld1q_u8(vector.as_ptr().add(block_idx * super::BLOCK_BYTES));
                let block_base = block_idx * BITS * super::BLOCK_BYTES;
                for (b, acc_b) in acc.iter_mut().enumerate() {
                    let plane = vld1q_u8(
                        self.planes
                            .as_ptr()
                            .add(block_base + b * super::BLOCK_BYTES),
                    );
                    let cnt = vcntq_u8(vandq_u8(data, plane));
                    let cnt16 = vpaddlq_u8(cnt);
                    *acc_b = vpadalq_u16(*acc_b, cnt16);
                }
            }

            // Partial tail block via zero-padded stack buffer — same SIMD
            // kernel, data bytes beyond `tail_bytes` are zero so they can't
            // contribute to any plane's AND-popcount.
            if let Some((buf, block_idx)) = self.tail_block_scratch(vector) {
                let data = vld1q_u8(buf.as_ptr());
                let block_base = block_idx * BITS * super::BLOCK_BYTES;
                for (b, acc_b) in acc.iter_mut().enumerate() {
                    let plane = vld1q_u8(
                        self.planes
                            .as_ptr()
                            .add(block_base + b * super::BLOCK_BYTES),
                    );
                    let cnt = vcntq_u8(vandq_u8(data, plane));
                    let cnt16 = vpaddlq_u8(cnt);
                    *acc_b = vpadalq_u16(*acc_b, cnt16);
                }
            }

            let mut v_dot_q: i64 = 0;
            for (b, acc_b) in acc.iter().enumerate() {
                let popcnt = u64::from(vaddvq_u32(*acc_b));
                let w_b: i64 = if b == BITS - 1 {
                    -(1i64 << (BITS - 1))
                } else {
                    1i64 << b
                };
                v_dot_q += w_b * popcnt as i64;
            }
            v_dot_q
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::super::shared::random_bytes;
    use super::super::shared::PARITY_BYTE_LENS;
    use super::super::{score_1bit_internal_neon, score_1bit_internal_scalar};

    #[test]
    fn test_score_neon_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(7);
        for &byte_len in PARITY_BYTE_LENS {
            let a = random_bytes(&mut rng, byte_len);
            let b = random_bytes(&mut rng, byte_len);
            let scalar = score_1bit_internal_scalar(&a, &b);
            let neon = unsafe { score_1bit_internal_neon(&a, &b) };
            assert_eq!(
                scalar.to_bits(),
                neon.to_bits(),
                "scalar {scalar} != neon {neon} at byte_len {byte_len}",
            );
        }
    }

    /// Overflow safety: at 64 KiB (= 524 288 bits) with `a = all 0xFF`,
    /// `b = all 0x00`, every bit disagrees → `popcnt = n_bits`.  Scalar is
    /// the reference (u64 accumulator throughout, can't overflow).  NEON
    /// must match exactly — a mismatch would indicate an intermediate
    /// `u32`/`u16` lane saturated.
    #[test]
    fn test_score_neon_overflow_safety_64k() {
        let byte_len = 65_536 / 8;
        let a = vec![0xFF_u8; byte_len];
        let b = vec![0x00_u8; byte_len];

        let scalar = score_1bit_internal_scalar(&a, &b);
        let neon = unsafe { score_1bit_internal_neon(&a, &b) };
        assert_eq!(
            scalar.to_bits(),
            neon.to_bits(),
            "neon disagrees at byte_len={byte_len}: scalar={scalar} neon={neon}",
        );

        // Also sanity-check the known closed-form: all bits disagree →
        // sign_sum = −n_bits → score = −c² · n_bits.
        let expected = -super::super::CENTROID_SQ * (byte_len * 8) as f32;
        assert!((scalar - expected).abs() / expected.abs() < 1e-6);
    }

    /// Parity for `Query1bitSimd::dotprod_raw_neon` vs the scalar kernel,
    /// at a few `BITS` values and dims.  Integer result must match bit-exactly.
    #[test]
    fn test_query_dotprod_neon_matches_scalar() {
        use rand_distr::{Distribution, StandardNormal};

        use super::super::Query1bitSimd;

        fn check<const BITS: usize>(dim: usize, seed: u64) {
            let mut rng = StdRng::seed_from_u64(seed);
            let query: Vec<f32> = (0..dim).map(|_| StandardNormal.sample(&mut rng)).collect();
            let data = random_bytes(&mut rng, dim / 8);
            let q = Query1bitSimd::<BITS>::new(&query);
            let scalar = q.dotprod_raw(&data);
            let neon = unsafe { q.dotprod_raw_neon(&data) };
            assert_eq!(
                scalar, neon,
                "BITS={BITS} dim={dim}: scalar={scalar} neon={neon}"
            );
        }

        // Corner-case dims exercising every tail size the 1-bit pipeline
        // can produce (tail ∈ {0, 8, 16, …, 120}, always a multiple of 8):
        //   • 128, 256, 512, 1024, 2048 — full blocks, no tail.
        //   • 8, 64, 120 — zero blocks + tail-only scoring paths.
        //   • 136, 1032, 2040 — realistic matryoshka slices with tails.
        //   • 640, 768, 896 — Gemma/BGE-style matryoshka dims.
        for &dim in &[
            8usize, 64, 120, 128, 136, 256, 512, 640, 768, 896, 1024, 1032, 2040, 2048,
        ] {
            check::<8>(dim, 0xCAFE);
            check::<10>(dim, 0xBEEF);
            check::<12>(dim, 0xDEAD);
        }
    }

    /// Overflow safety for `dotprod_raw_neon` at dim=64K and max BITS
    /// (quantization constants stressed to the extreme): all-1 data vs a
    /// query scaled to saturate the signed range.  Scalar is u64-accumulator
    /// reference; NEON u32 per-plane accumulators must match exactly.
    #[test]
    fn test_query_dotprod_neon_overflow_safety_64k() {
        use super::super::Query1bitSimd;

        let dim = 65_536;
        // Query = all +1.0 float → maps to +max signed int in every lane.
        let query = vec![1.0_f32; dim];
        let data = vec![0xFFu8; dim / 8];

        let q8 = Query1bitSimd::<8>::new(&query);
        assert_eq!(
            q8.dotprod_raw(&data),
            unsafe { q8.dotprod_raw_neon(&data) },
            "BITS=8 dim={dim}",
        );

        let q16 = Query1bitSimd::<16>::new(&query);
        assert_eq!(
            q16.dotprod_raw(&data),
            unsafe { q16.dotprod_raw_neon(&data) },
            "BITS=16 dim={dim}",
        );
    }
}
