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

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::shared::{PARITY_BYTE_LENS, random_bytes};
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

    /// Overflow safety: at 16 KiB (= 131 072 bits) with `a = all 0xFF`,
    /// `b = all 0x00`, every bit disagrees → `popcnt = n_bits`.  Scalar is
    /// the reference (u64 accumulator throughout, can't overflow).  NEON
    /// must match exactly — a mismatch would indicate an intermediate
    /// `u32`/`u16` lane saturated.
    #[test]
    fn test_score_neon_overflow_safety_16k() {
        let byte_len = 16 * 1024;
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
}
