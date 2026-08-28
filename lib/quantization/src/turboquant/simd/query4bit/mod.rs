//! 4-bit product-quantization scoring.
//!
//! Asymmetric scoring (query against packed codes) goes through the shared
//! [`QuerySimd`] kernels with the 4-bit [`Encoding`] defined here; the
//! symmetric paths (`score_4bit_internal*`, two packed vectors) have their
//! own kernels in the `arm` / `x64` submodules.
//!
//! Both codebooks derive from `CENTROIDS_4BIT` (Lloyd-Max on N(0,1)); see
//! `test_codebook_matches_lloyd_max` for the consistency check.

use super::SimdBackend;
use super::query::{Code, Encoding, QuerySimd};

/// `max|c|` over `CENTROIDS_4BIT` — the extreme centroid.  Shared by both archs.
const CODEBOOK_ABS_MAX: f32 = 2.733;

/// Integer encoding of the 4-bit width for the shared asymmetric kernels
/// ([`super::query::QuerySimd`]).
pub(super) const ENCODING: Encoding = Encoding {
    codebook: CODEBOOK,
    offset: CODEBOOK_OFFSET,
    scale: CODEBOOK_SCALE,
    query_high_coef: QUERY_HIGH_COEF,
    query_abs_max: QUERY_ABS_MAX,
};

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const CODEBOOK: [Code; 16] = CODEBOOK_I8;
#[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
const CODEBOOK: [Code; 16] = CODEBOOK_U8;

/// Full `i8` signed codebook for aarch64.  `c_scale = 127 / max|c|` so the
/// extremes hit ±127.  `c_signed[k] = CODEBOOK_I8[k]` directly — no offset.
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const CODEBOOK_I8: [i8; 16] = [
    -127, -96, -75, -58, -44, -31, -18, -6, 6, 18, 31, 44, 58, 75, 96, 127,
];

/// Codebook scale on aarch64: `c_scale = 127 / max|c|`.
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const CODEBOOK_SCALE: f32 = 127.0 / CODEBOOK_ABS_MAX;

/// Aarch64 stores the codebook already signed, so no shift-recovery is needed.
/// Kept as a uniform symbol so `new()` / `dotprod` don't need cfg branches.
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const CODEBOOK_OFFSET: i64 = 0;

/// Maximum signed-integer magnitude the query encoder targets.  Derived so the
/// balanced `q_signed = K · high + low` split keeps both halves inside i8 (on
/// aarch64: |low|, |high| ≤ 128 with K=256; on x86_64: |low|, |high| ≤ 64 with
/// K=128 to satisfy maddubs saturation given the full u8 codebook).
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const QUERY_ABS_MAX: f32 = 32639.0;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const QUERY_HIGH_COEF: i64 = 256;

/// Full `u8` unsigned codebook for x86_64.  `c_scale = 128 / max|c|` and
/// `c_u[k] = c_signed[k] + 128` puts the centroids into `[0, 255]`.
/// `maddubs` / `VPDPBUSD` consume this directly as their u8 operand.
#[cfg(target_arch = "x86_64")]
const CODEBOOK_U8: [u8; 16] = [
    0, 31, 52, 69, 84, 97, 110, 122, 134, 146, 159, 172, 187, 204, 225, 255,
];

/// Codebook shift: `c_signed[k] = CODEBOOK_U8[k] − CODEBOOK_OFFSET`.  Pair this
/// with `bias_correction = OFFSET · Σ q_signed` in `dotprod` to recover
/// `Σ q · c_signed` from the raw `Σ q · c_u`.
#[cfg(target_arch = "x86_64")]
const CODEBOOK_OFFSET: i64 = 128;

/// Codebook scale on x86_64: `c_scale = 128 / max|c|`.
#[cfg(target_arch = "x86_64")]
const CODEBOOK_SCALE: f32 = 128.0 / CODEBOOK_ABS_MAX;

/// Max q_signed on x86_64 — 7-bit signed half × 128 → roughly ±8127.  Actual
/// symmetric cap of 8127 = 128·63 + 63 keeps both halves in `[−64, 63]`.
#[cfg(target_arch = "x86_64")]
const QUERY_ABS_MAX: f32 = 8127.0;
#[cfg(target_arch = "x86_64")]
const QUERY_HIGH_COEF: i64 = 128;

// Fallback constants for architectures with neither NEON SIMD nor x86_64.
// They match the x86_64 scheme so the scalar reference path can run and
// produce numerically identical results to whichever arch is active.
#[cfg(not(any(
    all(target_arch = "aarch64", target_feature = "neon"),
    target_arch = "x86_64",
)))]
const CODEBOOK_U8: [u8; 16] = [
    0, 31, 52, 69, 84, 97, 110, 122, 134, 146, 159, 172, 187, 204, 225, 255,
];
#[cfg(not(any(
    all(target_arch = "aarch64", target_feature = "neon"),
    target_arch = "x86_64",
)))]
const CODEBOOK_OFFSET: i64 = 128;
#[cfg(not(any(
    all(target_arch = "aarch64", target_feature = "neon"),
    target_arch = "x86_64",
)))]
const CODEBOOK_SCALE: f32 = 128.0 / CODEBOOK_ABS_MAX;
#[cfg(not(any(
    all(target_arch = "aarch64", target_feature = "neon"),
    target_arch = "x86_64",
)))]
const QUERY_ABS_MAX: f32 = 8127.0;
#[cfg(not(any(
    all(target_arch = "aarch64", target_feature = "neon"),
    target_arch = "x86_64",
)))]
const QUERY_HIGH_COEF: i64 = 128;

/// Read the codebook value at `idx` in its arch-native storage form as `i64`.
/// On aarch64 that's the signed `CODEBOOK_I8`; on x86_64 (and fallback) it's
/// the unsigned `CODEBOOK_U8` — the `+OFFSET` shift is unwound later by the
/// query-side `bias_correction`.  Use this inside SIMD-adjacent code that
/// mirrors what the intrinsics actually see.
#[inline]
fn codebook_value_i64(idx: u8) -> i64 {
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        i64::from(CODEBOOK_I8[idx as usize])
    }
    #[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
    {
        i64::from(CODEBOOK_U8[idx as usize])
    }
}

/// Read the codebook value at `idx` as a **true signed** integer, regardless
/// of which storage the current arch uses.  Used by vector-vs-vector scoring
/// where both operands come from the codebook and there's no query-side
/// `bias_correction` to absorb the `+OFFSET` shift.
#[inline]
fn codebook_signed_i64(idx: u8) -> i64 {
    codebook_value_i64(idx) - CODEBOOK_OFFSET
}

/// Encoded query for asymmetric 4-bit scoring: [`QuerySimd`] over two codes
/// per byte (low nibble = even dim, high nibble = odd dim).
pub type Query4bitSimd = QuerySimd<2>;

/// Dot product between two already-encoded 4-bit PQ vectors.  Both `a` and
/// `b` are the packed-nibble format that [`Query4bitSimd::dotprod`] takes as
/// its `vector` argument — every byte holds two codebook indices (low nibble
/// = even lane, high nibble = odd lane).
///
/// Computes `Σ c[a[j]] · c[b[j]]` in centroid-float space.  Dispatches to the
/// fastest available SIMD implementation at runtime and falls back to
/// [`score_4bit_internal_scalar`] otherwise.  Any byte length is accepted —
/// bytes that don't fill a full SIMD chunk are folded in scalar-wise so that
/// Matryoshka-style dim ∈ {2k: k ∈ ℕ} all work.
///
/// # Panics
/// Panics if the two vectors have different lengths.
pub fn score_4bit_internal(a: &[u8], b: &[u8]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    match SimdBackend::detect() {
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Avx512Vnni => unsafe { x64::score_4bit_internal_avx512_vnni(a, b) },
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Avx2 => unsafe { x64::score_4bit_internal_avx2(a, b) },
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Sse => unsafe { x64::score_4bit_internal_sse(a, b) },
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        SimdBackend::NeonSdot => unsafe { arm::score_4bit_internal_neon_sdot(a, b) },
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        SimdBackend::Neon => unsafe { arm::score_4bit_internal_neon(a, b) },
        SimdBackend::Scalar => score_4bit_internal_scalar(a, b),
    }
}

/// Scalar reference implementation of [`score_4bit_internal`].  Exposed as
/// the fallback on architectures without a SIMD variant, the `assert_eq!`
/// baseline for per-arch parity tests, and as a standalone bench target.
///
/// Caller is responsible for checking the length preconditions — the public
/// [`score_4bit_internal`] enforces them before dispatching.
pub fn score_4bit_internal_scalar(a: &[u8], b: &[u8]) -> f32 {
    // c_signed ≈ c_float · c_scale → c_signed_a · c_signed_b ≈ c_float_a · c_float_b · c_scale².
    score_4bit_internal_integer(a, b) as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
}

/// Integer-only scalar kernel shared by all backends — used by SIMD paths to
/// fold in any bytes that didn't fit a full SIMD chunk (and by
/// [`score_4bit_internal_scalar`] as its inner loop).
#[inline]
pub(crate) fn score_4bit_internal_integer(a: &[u8], b: &[u8]) -> i64 {
    let mut acc: i64 = 0;
    for (&byte_a, &byte_b) in a.iter().zip(b.iter()) {
        let a_lo = byte_a & 0x0F;
        let a_hi = byte_a >> 4;
        let b_lo = byte_b & 0x0F;
        let b_hi = byte_b >> 4;
        acc += codebook_signed_i64(a_lo) * codebook_signed_i64(b_lo);
        acc += codebook_signed_i64(a_hi) * codebook_signed_i64(b_hi);
    }
    acc
}

/// Weighted variant of [`score_4bit_internal`]: returns `Σ_j c[a[j]] · c[b[j]]
/// · weights[j]` in centroid-float space. `weights` is i16-quantized `D'_j²`
/// from TQ+ error correction (non-negative, capped at `i16::MAX − 1`); the
/// caller divides the integer sum by `weight_scale · CODEBOOK_SCALE²` to
/// recover the true f32 dot. The `i16` element type encodes the SIMD
/// invariant directly — every backend can multiply via `vmull_s16` /
/// `madd_epi16` without re-checking the high bit.
///
/// Each input byte holds two 4-bit indices (low nibble = even lane, high
/// nibble = odd lane). `weights.len()` must equal `2 · a.len()`.
///
/// # Panics
/// Panics if `a` and `b` have different lengths or if `weights` has the
/// wrong length.
pub fn score_4bit_internal_weighted(a: &[u8], b: &[u8], weights: &[i16]) -> i64 {
    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_weighted: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    assert_eq!(
        weights.len(),
        2 * a.len(),
        "score_4bit_internal_weighted: weights length {} != 2 · a.len() {}",
        weights.len(),
        2 * a.len(),
    );

    match SimdBackend::detect() {
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Avx512Vnni | SimdBackend::Avx2 => unsafe {
            x64::score_4bit_internal_weighted_avx2(a, b, weights)
        },
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Sse => unsafe { x64::score_4bit_internal_weighted_sse(a, b, weights) },
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        SimdBackend::NeonSdot | SimdBackend::Neon => unsafe {
            arm::score_4bit_internal_weighted_neon(a, b, weights)
        },
        SimdBackend::Scalar => score_4bit_internal_weighted_scalar(a, b, weights),
    }
}

/// Scalar reference for [`score_4bit_internal_weighted`].
#[inline]
pub fn score_4bit_internal_weighted_scalar(a: &[u8], b: &[u8], weights: &[i16]) -> i64 {
    let mut acc: i64 = 0;
    for (i, (&byte_a, &byte_b)) in a.iter().zip(b.iter()).enumerate() {
        let a_lo = byte_a & 0x0F;
        let a_hi = byte_a >> 4;
        let b_lo = byte_b & 0x0F;
        let b_hi = byte_b >> 4;
        let p_lo = codebook_signed_i64(a_lo) * codebook_signed_i64(b_lo);
        let p_hi = codebook_signed_i64(a_hi) * codebook_signed_i64(b_hi);
        acc += p_lo * i64::from(weights[2 * i]);
        acc += p_hi * i64::from(weights[2 * i + 1]);
    }
    acc
}

/// Square of the codebook scale — the integer sum from
/// [`score_4bit_internal_weighted`] is divided by `weight_scale ·
/// CODEBOOK_SCALE_SQ` to get the f32 weighted dot.
pub const CODEBOOK_SCALE_SQ: f32 = CODEBOOK_SCALE * CODEBOOK_SCALE;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod arm;

#[cfg(target_arch = "x86_64")]
mod x64;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use arm::{
    score_4bit_internal_neon, score_4bit_internal_neon_sdot, score_4bit_internal_weighted_neon,
};
#[cfg(target_arch = "x86_64")]
pub use x64::{
    score_4bit_internal_avx2, score_4bit_internal_avx512_vnni, score_4bit_internal_sse,
    score_4bit_internal_weighted_avx2, score_4bit_internal_weighted_sse,
};

/// 4-bit-specific test helpers.  Bit-width-agnostic helpers (`pack_codes`,
/// `sample_normal_vec`, `encode_to_nearest_centroid`) live in
/// [`super::super::shared`].
#[cfg(test)]
pub mod shared {
    use rand::prelude::StdRng;
    use rand::seq::SliceRandom;

    use super::super::shared::{pack_codes, sample_normal_vec};
    use super::Query4bitSimd;

    /// Corner-case dims covering every tail size the 4-bit pipeline can
    /// produce, for each SIMD backend:
    ///   • `16, 32, 128, 256, 1024, 2048` — full chunks, no tail (baseline).
    ///   • `48` — 3 chunks (odd SDOT/VNNI leftover), no tail.
    ///   • `18, 30, 46, 62, 1026, 2046` — full chunks + non-zero tail
    ///     (tail sizes 2, 14, 14, 14, 2, 14 dims respectively).
    ///   • `270` — 16 chunks + 14-dim tail (exercises largest tail at a
    ///     realistic matryoshka dim).
    pub const PARITY_DIMS: &[usize] = &[
        16, 18, 30, 32, 46, 48, 62, 128, 256, 270, 1024, 1026, 2046, 2048,
    ];

    /// Parity-test helper: query ~ N(0,1), balanced index distribution (each
    /// centroid appears `dim/16` times, shuffled).  Index distribution doesn't
    /// affect scalar-vs-SIMD parity; we just need non-trivial data.
    pub fn random_inputs(rng: &mut StdRng, dim: usize) -> (Query4bitSimd, Vec<u8>) {
        let query = sample_normal_vec(rng, dim);
        let mut indices: Vec<u8> = (0..dim).map(|i| (i % 16) as u8).collect();
        indices.shuffle(rng);
        (Query4bitSimd::new(&query), pack_codes(&indices, 4))
    }
}

/// Codebook and symmetric-scoring accuracy tests.  Per-arch parity tests of
/// the symmetric kernels live in the `arm` / `x64` submodules; the
/// asymmetric path is covered by `super::query`.
#[cfg(test)]
mod tests {
    // Anonymous `use _` brings the trait into scope for `StdRng::seed_from_u64`
    // without introducing a name rustc then flags as unused.
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::shared::{encode_to_nearest_centroid, pack_codes, sample_normal_vec};
    use super::CODEBOOK_ABS_MAX;
    use crate::turboquant::TQBits;

    /// Whichever codebook representation the current arch uses (signed i8 on
    /// aarch64, shifted u8 on x86_64), it must match what the runtime recipe
    /// would produce from `CENTROIDS_4BIT`.
    #[test]
    fn test_codebook_matches_lloyd_max() {
        let centroids = TQBits::Bits4.get_centroids();
        assert_eq!(centroids.len(), 16);

        let c_abs_max = centroids
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max);
        assert!(
            (CODEBOOK_ABS_MAX - c_abs_max).abs() < 1e-6,
            "CODEBOOK_ABS_MAX ({CODEBOOK_ABS_MAX}) != max|CENTROIDS_4BIT| ({c_abs_max})"
        );

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            let c_scale = 127.0 / c_abs_max;
            let quantized: [i8; 16] = std::array::from_fn(|k| {
                (centroids[k] * c_scale).round().clamp(-127.0, 127.0) as i8
            });
            assert_eq!(
                quantized,
                super::CODEBOOK_I8,
                "const CODEBOOK_I8 drifted from CENTROIDS_4BIT Lloyd-Max quantization",
            );
        }
        #[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
        {
            let c_scale = 128.0 / c_abs_max;
            let offset = super::CODEBOOK_OFFSET as i32;
            let quantized: [u8; 16] = std::array::from_fn(|k| {
                let signed = (centroids[k] * c_scale).round().clamp(-128.0, 127.0) as i32;
                (signed + offset) as u8
            });
            assert_eq!(
                quantized,
                super::CODEBOOK_U8,
                "const CODEBOOK_U8 drifted from CENTROIDS_4BIT Lloyd-Max quantization",
            );
        }
    }

    /// `score_4bit_internal` should recover the pure centroid-space dot
    /// product `Σ c[a[j]] · c[b[j]]` up to the i8 quantization step of the
    /// codebook (≤ 1/c_scale ≈ 0.022 per centroid).  For dim=256 the
    /// cumulative RMS error stays well under 1.0.
    #[test]
    fn test_score_4bit_internal_matches_centroid_product() {
        let mut rng = StdRng::seed_from_u64(7);
        let centroids = TQBits::Bits4.get_centroids();
        let dim = 256;
        let n_trials = 32;

        for _ in 0..n_trials {
            let raw_a = sample_normal_vec(&mut rng, dim);
            let raw_b = sample_normal_vec(&mut rng, dim);
            let idx_a = encode_to_nearest_centroid(centroids, &raw_a);
            let idx_b = encode_to_nearest_centroid(centroids, &raw_b);

            let truth: f64 = idx_a
                .iter()
                .zip(idx_b.iter())
                .map(|(&ia, &ib)| {
                    f64::from(centroids[ia as usize]) * f64::from(centroids[ib as usize])
                })
                .sum();
            let score = super::score_4bit_internal(&pack_codes(&idx_a, 4), &pack_codes(&idx_b, 4));

            // Codebook quantization budget: Δc ≈ max|c|/127 ≈ 0.022, so each
            // term has error ≲ 2·c·Δc ≲ 0.12; over d=256 independent terms
            // the RMS error is ≲ √d · 0.12 ≈ 1.9.  2.0 is a loose 1σ-ish bound.
            assert!(
                (truth as f32 - score).abs() < 2.0,
                "score {score} too far from centroid-product truth {truth}",
            );
        }
    }

    /// Saturation-safety at 64K dims: every centroid index at max (`15`),
    /// every weight at `i16::MAX`. `weights[i]` is `i16` (the storage type
    /// matches the SIMD load — `madd_epi16` / `vmull_s16`). This is the
    /// worst-case integer load the weighted kernel can see, and the i64
    /// accumulator must absorb it.
    ///
    /// Per-coord product:
    ///   `c_signed² × weight = 127² × 32 767 = 16 129 × 32 767 ≈ 5.28e8`
    /// (fits i32 with ~4× headroom). Total over 65 536 coords:
    /// `≈ 3.46e13` — fits i64 (~2.7e5× headroom; signed i64 max ≈ 9.2e18).
    #[test]
    fn test_score_4bit_internal_weighted_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![15; dim]; // max-magnitude centroid
        let vec_a = pack_codes(&indices, 4);
        let vec_b = pack_codes(&indices, 4);
        let max_weight: i16 = i16::MAX;
        let weights: Vec<i16> = vec![max_weight; dim];

        let raw_int = super::score_4bit_internal_weighted(&vec_a, &vec_b, &weights);

        // Worst-case integer ground truth: every coord contributes
        // `c_signed² × max_weight`, summed over `dim`. Any SIMD i32-lane
        // accumulator overflow would manifest here as a wrap-around.
        let c_max_signed = super::codebook_signed_i64(15);
        let per_coord = c_max_signed * c_max_signed * i64::from(max_weight);
        let expected = per_coord * dim as i64;
        assert_eq!(
            raw_int, expected,
            "i64 sum overflow / mismatch at dim={dim} (per-coord={per_coord}, expected={expected}, got={raw_int})",
        );
    }

    /// `score_4bit_internal_weighted` should recover `Σ c[a[j]] · c[b[j]] ·
    /// D'_j²` up to:
    ///   1. the i8 codebook quantization step (≈ 0.022 per centroid), and
    ///   2. the i16 weight quantization step (relative ≤ 1/(i16::MAX-1)).
    ///
    /// Reconstruction is `int_sum / (weight_scale · CODEBOOK_SCALE²)`.
    #[test]
    fn test_score_4bit_internal_weighted_matches_reference() {
        use rand::RngExt;

        let mut rng = StdRng::seed_from_u64(0xCAFE);
        let centroids = TQBits::Bits4.get_centroids();
        let dim = 256;
        let n_trials = 32;

        for _ in 0..n_trials {
            let raw_a = sample_normal_vec(&mut rng, dim);
            let raw_b = sample_normal_vec(&mut rng, dim);
            let idx_a = encode_to_nearest_centroid(centroids, &raw_a);
            let idx_b = encode_to_nearest_centroid(centroids, &raw_b);

            // Random weights in [0, 4) f32, quantized into the `i16` storage
            // form the SIMD kernels consume directly — matches
            // `ErrorCorrection::new` (values capped to `[0, i16::MAX − 1]`).
            let weights_f32: Vec<f32> = (0..dim).map(|_| rng.random_range(0.0..4.0)).collect();
            let max_w = weights_f32.iter().copied().fold(0.0f32, f32::max);
            const QUANT_CAP: i16 = i16::MAX - 1;
            let weight_scale = f32::from(QUANT_CAP) / max_w;
            let weights_i16: Vec<i16> = weights_f32
                .iter()
                .map(|&x| (x * weight_scale).round().clamp(0.0, f32::from(QUANT_CAP)) as i16)
                .collect();

            let truth: f64 = idx_a
                .iter()
                .zip(idx_b.iter())
                .zip(weights_f32.iter())
                .map(|((&ia, &ib), &w)| {
                    f64::from(centroids[ia as usize])
                        * f64::from(centroids[ib as usize])
                        * f64::from(w)
                })
                .sum();
            let raw_int = super::score_4bit_internal_weighted(
                &pack_codes(&idx_a, 4),
                &pack_codes(&idx_b, 4),
                &weights_i16,
            );
            let score = raw_int as f32 / (weight_scale * super::CODEBOOK_SCALE_SQ);

            // Per-coord error budget ≈ |c|² · max_w / 65534 + 2·|c|·Δc·w.
            // For dim=256 with weights up to 4 and centroids up to ~2.7 the
            // RMS bound is ≲ √d · 0.5 ≈ 8 — generous slack at 16.
            assert!(
                (truth as f32 - score).abs() < 16.0,
                "weighted score {score} too far from reference {truth}",
            );
        }
    }
}
