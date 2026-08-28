//! 1-bit product-quantization scoring.
//!
//! The 1-bit codebook is `[-c, +c]` (see `CENTROIDS_1BIT`), so each code is a
//! single sign bit.  Input vectors pack 8 lanes per byte.
//!
//! Two scoring paths live here:
//!
//! * [`score_1bit_internal`] — dot between two packed vectors.  Collapses to
//!   `c² · (n_match − n_mismatch)`, a plain `XOR + popcount`, so the per-arch
//!   work is just picking the fastest popcount primitive (AVX-512 VPOPCNTDQ,
//!   AVX2 / SSE `pshufb`-nibble lookup, NEON `vcntq_u8`).
//!
//! * [`Query1bitSimd`] — asymmetric scoring of an original query against
//!   packed data, through the shared [`QuerySimd`] kernels with the 1-bit
//!   [`Encoding`] defined here (a sign bit indexes a two-entry codebook).

use super::query::{Code, Encoding, QuerySimd, pad_codebook};

/// `|c|` for the 1-bit codebook — Lloyd-Max on N(0, 1) gives `sqrt(2/π)`.
/// Kept in sync with `CENTROIDS_1BIT` in `lloyd_max.rs` by a test below.
const CENTROID_ABS: f32 = 0.797_884_6;

/// `c²` — converts the signed-agreement count into a centroid dot product:
/// each lane contributes `(±c)·(±c) = ±c²`, summing to `c² · sign_sum`.
const CENTROID_SQ: f32 = CENTROID_ABS * CENTROID_ABS;

/// Integer encoding of the 1-bit width for the shared asymmetric kernels
/// ([`super::query::QuerySimd`]).  Both codebook entries are `∓c`, so any
/// symmetric integer pair represents them exactly; the pair is chosen per
/// arch for the widest query the multiply keeps inside its lane.
pub(super) const ENCODING: Encoding = Encoding {
    codebook: pad_codebook(CODEBOOK),
    offset: CODEBOOK_OFFSET,
    scale: CODEBOOK_SCALE,
    query_high_coef: 256,
    query_abs_max: 32639.0,
};

/// aarch64: signed `∓127`, no offset.
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const CODEBOOK: [Code; 2] = [-127, 127];
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const CODEBOOK_OFFSET: i64 = 0;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const CODEBOOK_SCALE: f32 = 127.0 / CENTROID_ABS;

/// x86_64: `{0, 128}` with offset 64 — a codebook magnitude of 128 lets
/// the query halves span the full i8 range without the `maddubs` pair sum
/// leaving i16 (`|pair| ≤ 2 · 128 · 128 = 32 768`, reached only at the
/// negative end, which i16 holds).
#[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
const CODEBOOK: [Code; 2] = [0, 128];
#[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
const CODEBOOK_OFFSET: i64 = 64;
#[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
const CODEBOOK_SCALE: f32 = 64.0 / CENTROID_ABS;

/// Encoded query for asymmetric 1-bit scoring: [`QuerySimd`] over eight
/// codes per byte (the sign bit of dim `8j + k` is bit `k` of byte `j`).
pub type Query1bitSimd = QuerySimd<8>;

/// Dot product between two already-encoded 1-bit PQ vectors.
///
/// Both `a` and `b` are packed 8 lanes per byte.  Returns `c² · sign_sum`
/// where `sign_sum = n_bits − 2 · popcount(a ⊕ b)`: bits that agree
/// contribute `+c²`, bits that disagree contribute `−c²`.
///
/// Dispatches at runtime to the fastest popcount backend available on the
/// host CPU; falls back to [`score_1bit_internal_scalar`] otherwise.
///
/// # Panics
/// Panics if `a` and `b` have different lengths.
pub fn score_1bit_internal(a: &[u8], b: &[u8]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "score_1bit_internal: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512vpopcntdq")
        {
            return unsafe { x64::score_1bit_internal_avx512_vpopcntdq(a, b) };
        }
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { x64::score_1bit_internal_avx2(a, b) };
        }
        if std::is_x86_feature_detected!("sse4.1") && std::is_x86_feature_detected!("ssse3") {
            return unsafe { x64::score_1bit_internal_sse(a, b) };
        }
    }
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        return unsafe { arm::score_1bit_internal_neon(a, b) };
    }
    #[allow(unreachable_code)]
    score_1bit_internal_scalar(a, b)
}

/// Scalar reference implementation of [`score_1bit_internal`] — byte-wise
/// `XOR + count_ones`.  Same result as the public function; used as the
/// parity baseline for SIMD tests and the ultimate fallback on
/// architectures without a SIMD variant.
pub fn score_1bit_internal_scalar(a: &[u8], b: &[u8]) -> f32 {
    let mut popcnt: u64 = 0;
    for (&ba, &bb) in a.iter().zip(b.iter()) {
        popcnt += u64::from((ba ^ bb).count_ones());
    }
    popcount_to_score(a.len(), popcnt)
}

/// Convert `popcount(a ⊕ b)` over `byte_len` bytes into the centroid dot
/// product.  Shared across scalar and SIMD paths so every backend agrees on
/// the final rounding.
#[inline]
pub(crate) fn popcount_to_score(byte_len: usize, popcnt: u64) -> f32 {
    let total_bits = (byte_len as i64) * 8;
    let sign_sum = total_bits - 2 * (popcnt as i64);
    CENTROID_SQ * sign_sum as f32
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod arm;
#[cfg(target_arch = "x86_64")]
mod x64;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use arm::score_1bit_internal_neon;
#[cfg(target_arch = "x86_64")]
pub use x64::{
    score_1bit_internal_avx2, score_1bit_internal_avx512_vpopcntdq, score_1bit_internal_sse,
};

#[cfg(test)]
pub mod shared {
    /// Byte lengths used by parity tests.  Cover: below SSE width, a single
    /// SSE chunk, an AVX2 chunk, an AVX-512 chunk, two AVX-512 chunks + tail.
    pub const PARITY_BYTE_LENS: &[usize] = &[1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 257, 513];
}

/// Codebook and symmetric-scoring accuracy tests.  Per-arch parity tests of
/// the popcount kernels live in the `arm` / `x64` submodules; the
/// asymmetric path is covered by `super::query`.
#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::shared::random_bytes;
    use super::*;
    use crate::turboquant::TQBits;

    /// `CENTROID_ABS` must match the magnitude in `CENTROIDS_1BIT`.
    #[test]
    fn test_codebook_matches_lloyd_max() {
        let centroids = TQBits::Bits1.get_centroids();
        assert_eq!(centroids.len(), 2);
        assert!((centroids[0] + CENTROID_ABS).abs() < 1e-6);
        assert!((centroids[1] - CENTROID_ABS).abs() < 1e-6);
    }

    /// Scalar result equals the naive sum-of-centroid-products.
    #[test]
    fn test_scalar_matches_centroid_product() {
        let mut rng = StdRng::seed_from_u64(0xC0FFEE);
        for &byte_len in &[1usize, 4, 8, 9, 16, 32, 128, 257] {
            let a = random_bytes(&mut rng, byte_len);
            let b = random_bytes(&mut rng, byte_len);

            let mut expected = 0.0_f32;
            for byte_idx in 0..byte_len {
                for bit in 0..8 {
                    let sa = if (a[byte_idx] >> bit) & 1 == 1 {
                        CENTROID_ABS
                    } else {
                        -CENTROID_ABS
                    };
                    let sb = if (b[byte_idx] >> bit) & 1 == 1 {
                        CENTROID_ABS
                    } else {
                        -CENTROID_ABS
                    };
                    expected += sa * sb;
                }
            }
            let got = score_1bit_internal_scalar(&a, &b);
            assert!(
                (expected - got).abs() < 1e-3,
                "byte_len={byte_len} expected {expected} got {got}",
            );
        }
    }

    /// Dispatched result matches scalar bit-for-bit.
    #[test]
    fn test_dispatch_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(42);
        for &byte_len in super::shared::PARITY_BYTE_LENS {
            let a = random_bytes(&mut rng, byte_len);
            let b = random_bytes(&mut rng, byte_len);
            let s = score_1bit_internal_scalar(&a, &b);
            let d = score_1bit_internal(&a, &b);
            assert_eq!(s.to_bits(), d.to_bits(), "byte_len={byte_len}");
        }
    }

    /// score(a, a) == +c² · n_bits, score(a, ¬a) == −c² · n_bits.
    #[test]
    fn test_score_extremes() {
        let mut rng = StdRng::seed_from_u64(1);
        let byte_len = 64;
        let a = random_bytes(&mut rng, byte_len);
        let not_a: Vec<u8> = a.iter().map(|&x| !x).collect();

        let n_bits = (byte_len * 8) as f32;
        let max = CENTROID_SQ * n_bits;

        assert!((score_1bit_internal(&a, &a) - max).abs() < 1e-3);
        assert!((score_1bit_internal(&a, &not_a) + max).abs() < 1e-3);
    }
}
