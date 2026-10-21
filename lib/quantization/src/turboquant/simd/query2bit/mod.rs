//! 2-bit product-quantization scoring.
//!
//! Asymmetric scoring (query against packed codes) goes through the shared
//! [`QuerySimd`] kernels with the 2-bit [`Encoding`] defined here.  The
//! symmetric paths (`score_2bit_internal*`, two packed vectors) have their
//! own kernels in the `arm` / `x64` submodules, built on a **pair-table**
//! unpack: a nibble of a packed byte holds a pair of 2-bit codes (16
//! combinations), which maps one-to-one to a 16-entry `vqtbl1q_s8` /
//! `pshufb` table.  Two such lookups (even / odd code of each pair),
//! zipped, give 16 centroid bytes in natural dim order per 4 packed bytes.
//!
//! Both codebooks derive from `CENTROIDS_2BIT` (Lloyd-Max on N(0,1)); see
//! `test_codebook_matches_lloyd_max` for the consistency check.

use super::query::{Code, Encoding, QuerySimd, pad_codebook};
use super::{FOLDED_MAX_DIM, SimdBackend, fold_q15};

/// `max|c|` over `CENTROIDS_2BIT` — the extreme centroid magnitude.
const CODEBOOK_ABS_MAX: f32 = 1.510;

/// Integer encoding of the 2-bit width for the shared asymmetric kernels
/// ([`super::query::QuerySimd`]).
pub(super) const ENCODING: Encoding = Encoding {
    codebook: pad_codebook(CODEBOOK),
    offset: CODEBOOK_OFFSET,
    scale: CODEBOOK_SCALE,
    query_high_coef: QUERY_HIGH_COEF,
    query_abs_max: QUERY_ABS_MAX,
};

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const CODEBOOK: [Code; 4] = CODEBOOK_I8;
#[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
const CODEBOOK: [Code; 4] = CODEBOOK_U8;

/// Signed `i8` codebook for aarch64: `c_scale = 127 / max|c|`, no offset.
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const CODEBOOK_I8: [i8; 4] = [-127, -38, 38, 127];

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const CODEBOOK_SCALE: f32 = 127.0 / CODEBOOK_ABS_MAX;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const CODEBOOK_OFFSET: i64 = 0;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const QUERY_ABS_MAX: f32 = 32639.0;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const QUERY_HIGH_COEF: i64 = 256;

/// Unsigned `u8` codebook for x86_64: `c_u = c_signed + 128`.
#[cfg(target_arch = "x86_64")]
const CODEBOOK_U8: [u8; 4] = [0, 90, 166, 255];

#[cfg(target_arch = "x86_64")]
const CODEBOOK_OFFSET: i64 = 128;

#[cfg(target_arch = "x86_64")]
const CODEBOOK_SCALE: f32 = 128.0 / CODEBOOK_ABS_MAX;

#[cfg(target_arch = "x86_64")]
const QUERY_ABS_MAX: f32 = 8127.0;

#[cfg(target_arch = "x86_64")]
const QUERY_HIGH_COEF: i64 = 128;

// Fallback for architectures with neither NEON nor x86_64.  Matches the
// x86_64 scheme so the scalar reference produces the same numeric result.
#[cfg(not(any(
    all(target_arch = "aarch64", target_feature = "neon"),
    target_arch = "x86_64",
)))]
const CODEBOOK_U8: [u8; 4] = [0, 90, 166, 255];
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

/// Read the codebook value at `idx` (a 2-bit code in `0..=3`) in arch-native
/// storage form.  On aarch64 that is the signed `CODEBOOK_I8`; on x86_64
/// (and the fallback) the unsigned `CODEBOOK_U8` — the `+OFFSET` shift is
/// unwound later by the query-side `bias_correction`.
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
/// of which arch's storage is active.  Used by vector-vs-vector scoring.
#[inline]
fn codebook_signed_i64(idx: u8) -> i64 {
    codebook_value_i64(idx) - CODEBOOK_OFFSET
}

/// The codes' true signed values, as [`codebook_signed_i64`] gives them.
pub(crate) fn signed_codebook() -> [i8; 4] {
    std::array::from_fn(|code| codebook_signed_i64(code as u8) as i8)
}

/// Encoded query for asymmetric 2-bit scoring: [`QuerySimd`] over four
/// codes per byte (code `k` of dim `4j + k` in bits `[2k, 2k + 2)` of byte
/// `j`).
pub type Query2bitSimd = QuerySimd<4, 2>;

/// Dot product between two already-encoded 2-bit PQ vectors.  Any byte length
/// is accepted — bytes beyond the last SIMD chunk are folded in scalar-wise.
///
/// # Panics
/// Panics if the two vectors have different lengths.
pub fn score_2bit_internal(a: &[u8], b: &[u8]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "score_2bit_internal: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    match SimdBackend::detect() {
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Avx512Vnni => unsafe { x64::score_2bit_internal_avx512_vnni(a, b) },
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Avx2 => unsafe { x64::score_2bit_internal_avx2(a, b) },
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Sse => unsafe { x64::score_2bit_internal_sse(a, b) },
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        SimdBackend::NeonSdot => unsafe { arm::score_2bit_internal_neon_sdot(a, b) },
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        SimdBackend::Neon => unsafe { arm::score_2bit_internal_neon(a, b) },
        SimdBackend::Scalar => score_2bit_internal_scalar(a, b),
    }
}

/// Scalar reference for [`score_2bit_internal`] — see the 4-bit counterpart
/// for the design rationale.
pub fn score_2bit_internal_scalar(a: &[u8], b: &[u8]) -> f32 {
    centroid_dot(score_2bit_internal_integer(a, b) as f32)
}

/// Integer-only scalar kernel — used by SIMD paths to fold in bytes that
/// didn't fit a full SIMD chunk, and by [`score_2bit_internal_scalar`] as its
/// inner loop.
#[inline]
pub(crate) fn score_2bit_internal_integer(a: &[u8], b: &[u8]) -> i64 {
    let mut acc: i64 = 0;
    for (&byte_a, &byte_b) in a.iter().zip(b.iter()) {
        for k in 0..4 {
            let shift = 2 * k;
            let a_k = (byte_a >> shift) & 0x03;
            let b_k = (byte_b >> shift) & 0x03;
            acc += codebook_signed_i64(a_k) * codebook_signed_i64(b_k);
        }
    }
    acc
}

/// The TQ+ variant of [`score_2bit_internal`]: `Σ_j q(a, j) · q(b, j)`, where `q(v, j) =
/// fold_q15(c[v[j]], factors[j])` is the code's signed codebook value scaled by the
/// coordinate's Q15 factor (`ErrorCorrection::d_prime_q15`, `D'_j / max D'`). The folded
/// values stay in i8, so the matrix operands decode the same values with [`fold_codes`] and
/// score them exactly as this does.
///
/// Each input byte holds four 2-bit indices (lanes 0..=3 from LSB). `factors.len()` must
/// equal `4 · a.len()`.
///
/// # Panics
/// Panics if `a` and `b` have different lengths, if `factors` has the wrong length, or if it
/// exceeds [`FOLDED_MAX_DIM`].
pub fn score_2bit_internal_folded(a: &[u8], b: &[u8], factors: &[i16]) -> i64 {
    assert_eq!(
        a.len(),
        b.len(),
        "score_2bit_internal_folded: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    assert_eq!(
        factors.len(),
        4 * a.len(),
        "score_2bit_internal_folded: factors length {} != 4 · a.len() {}",
        factors.len(),
        4 * a.len(),
    );
    assert!(factors.len() <= FOLDED_MAX_DIM);

    match SimdBackend::detect() {
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Avx512Vnni | SimdBackend::Avx2 => unsafe {
            x64::score_2bit_internal_folded_avx2(a, b, factors)
        },
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Sse => unsafe { x64::score_2bit_internal_folded_sse(a, b, factors) },
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        SimdBackend::NeonSdot | SimdBackend::Neon => unsafe {
            arm::score_2bit_internal_folded_neon(a, b, factors)
        },
        SimdBackend::Scalar => score_2bit_internal_folded_scalar(a, b, factors),
    }
}

/// Scalar reference for [`score_2bit_internal_folded`].
#[inline]
pub fn score_2bit_internal_folded_scalar(a: &[u8], b: &[u8], factors: &[i16]) -> i64 {
    let mut acc: i64 = 0;
    for (i, (&byte_a, &byte_b)) in a.iter().zip(b.iter()).enumerate() {
        for k in 0..4 {
            let shift = 2 * k;
            let factor = factors[4 * i + k];
            let q_a = fold_q15(codebook_signed_i64((byte_a >> shift) & 0x03), factor);
            let q_b = fold_q15(codebook_signed_i64((byte_b >> shift) & 0x03), factor);
            acc += q_a * q_b;
        }
    }
    acc
}

/// The folded values of [`score_2bit_internal_folded`] of one vector, one per position of
/// `out`: `codes` must hold at least `out.len()` codes and `factors` at least as many factors.
pub(crate) fn fold_codes(codes: &[u8], factors: &[i16], out: &mut [i8]) {
    assert!(out.len() <= 4 * codes.len() && out.len() <= factors.len());
    #[cfg(target_arch = "x86_64")]
    let done = if std::is_x86_feature_detected!("avx2") {
        // SAFETY: AVX2 checked above, the lengths asserted above
        unsafe { x64::fold_codes_avx2(codes, factors, out) }
    } else {
        0
    };
    #[cfg(not(target_arch = "x86_64"))]
    let done = 0;
    for (i, o) in out.iter_mut().enumerate().skip(done) {
        let code = (codes[i / 4] >> (2 * (i % 4))) & 0x03;
        *o = fold_q15(codebook_signed_i64(code), factors[i]) as i8;
    }
}

/// Square of the codebook scale — the integer sum from
/// [`score_2bit_internal_folded`] is divided by `CODEBOOK_SCALE_SQ` and the
/// square of the factors' scale to get the f32 weighted dot.
pub const CODEBOOK_SCALE_SQ: f32 = CODEBOOK_SCALE * CODEBOOK_SCALE;

/// The centroid dot of two vectors from the sum of their codes' products, the codes as their
/// signed codebook values: shared by every backend and the matrix operands, so all agree on
/// the rounding.
#[inline(always)]
pub(crate) fn centroid_dot(sum: f32) -> f32 {
    sum / CODEBOOK_SCALE_SQ
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod arm;

#[cfg(target_arch = "x86_64")]
mod x64;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use arm::{
    score_2bit_internal_folded_neon, score_2bit_internal_neon, score_2bit_internal_neon_sdot,
};
#[cfg(target_arch = "x86_64")]
pub use x64::{
    score_2bit_internal_avx2, score_2bit_internal_avx512_vnni, score_2bit_internal_folded_avx2,
    score_2bit_internal_folded_sse, score_2bit_internal_sse,
};

/// 2-bit-specific test helpers.  Bit-width-agnostic helpers (`pack_codes`,
/// `sample_normal_vec`, `encode_to_nearest_centroid`) live in
/// [`super::super::shared`].
#[cfg(test)]
pub mod shared {
    use rand::prelude::StdRng;
    use rand::seq::SliceRandom;

    use super::super::shared::{pack_codes, sample_normal_vec};
    use super::Query2bitSimd;

    /// Corner-case dims covering every tail size the 2-bit pipeline can
    /// produce (tail is 0, 4, 8, or 12 dims since `dim % 4 == 0`):
    ///   • `16, 64, 128, 256, 1024, 2048` — full chunks, no tail.
    ///   • `48` — 3 chunks (odd for SDOT/AVX2/AVX-512 unrolls), no tail.
    ///   • `20, 28, 44, 60, 1028, 2044` — full chunks + 4/12/12/12/4/12-dim tail.
    ///   • `268` — 16 chunks + 12-dim tail (realistic matryoshka).
    pub const PARITY_DIMS: &[usize] = &[
        16, 20, 28, 32, 44, 48, 60, 64, 128, 256, 268, 1024, 1028, 2044, 2048,
    ];

    /// Parity-test helper: query ~ N(0, 1), balanced index distribution.
    pub fn random_inputs(rng: &mut StdRng, dim: usize) -> (Query2bitSimd, Vec<u8>) {
        let query = sample_normal_vec(rng, dim);
        let mut indices: Vec<u8> = (0..dim).map(|i| (i % 4) as u8).collect();
        indices.shuffle(rng);
        (Query2bitSimd::new(&query), pack_codes(&indices, 2))
    }
}

/// Codebook and symmetric-scoring accuracy tests.  Per-arch parity tests of
/// the symmetric kernels live in the `arm` / `x64` submodules; the
/// asymmetric path is covered by `super::query`.
#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::shared::{encode_to_nearest_centroid, pack_codes, sample_normal_vec};
    use super::{CODEBOOK_ABS_MAX, score_2bit_internal_scalar};
    use crate::turboquant::TQBits;

    #[test]
    fn test_codebook_matches_lloyd_max() {
        let centroids = TQBits::Bits2.get_centroids();
        assert_eq!(centroids.len(), 4);

        let c_abs_max = centroids
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max);
        assert!(
            (CODEBOOK_ABS_MAX - c_abs_max).abs() < 1e-6,
            "CODEBOOK_ABS_MAX ({CODEBOOK_ABS_MAX}) != max|CENTROIDS_2BIT| ({c_abs_max})"
        );

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            let c_scale = 127.0 / c_abs_max;
            let quantized: [i8; 4] = std::array::from_fn(|k| {
                (centroids[k] * c_scale).round().clamp(-127.0, 127.0) as i8
            });
            assert_eq!(quantized, super::CODEBOOK_I8);
        }
        #[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
        {
            let c_scale = 128.0 / c_abs_max;
            let offset = super::CODEBOOK_OFFSET as i32;
            let quantized: [u8; 4] = std::array::from_fn(|k| {
                let signed = (centroids[k] * c_scale).round().clamp(-128.0, 127.0) as i32;
                (signed + offset) as u8
            });
            assert_eq!(quantized, super::CODEBOOK_U8);
        }
    }

    /// `score_2bit_internal_scalar(a, b)` ≈ `Σ centroid(a_k) · centroid(b_k)`.
    #[test]
    fn test_score_2bit_internal_matches_centroid_product() {
        let mut rng = StdRng::seed_from_u64(0xBAD);
        let dim = 256;
        let centroids = TQBits::Bits2.get_centroids();
        let n_trials = 16;

        for _ in 0..n_trials {
            let raw_a = sample_normal_vec(&mut rng, dim);
            let raw_b = sample_normal_vec(&mut rng, dim);
            let idx_a = encode_to_nearest_centroid(centroids, &raw_a);
            let idx_b = encode_to_nearest_centroid(centroids, &raw_b);

            let expected: f32 = idx_a
                .iter()
                .zip(idx_b.iter())
                .map(|(&a, &b)| centroids[a as usize] * centroids[b as usize])
                .sum();
            let got = score_2bit_internal_scalar(&pack_codes(&idx_a, 2), &pack_codes(&idx_b, 2));

            assert!(
                (expected - got).abs() < 0.5,
                "scalar score {got} too far from centroid product {expected}",
            );
        }
    }

    /// Saturation-safety at 64K dims: every centroid index at the largest
    /// magnitude (`0`, `−128` on x86_64), every factor at `i16::MAX`. The
    /// worst-case integer load the folded kernel can see.
    #[test]
    fn test_score_2bit_internal_folded_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![0; dim];
        let vec_a = pack_codes(&indices, 2);
        let vec_b = pack_codes(&indices, 2);
        let factors: Vec<i16> = vec![i16::MAX; dim];

        let raw_int = super::score_2bit_internal_folded(&vec_a, &vec_b, &factors);

        let q_max = super::fold_q15(super::codebook_signed_i64(0), i16::MAX);
        let expected = q_max * q_max * dim as i64;
        assert_eq!(
            raw_int, expected,
            "sum overflow / mismatch at dim={dim} (q={q_max}, expected={expected}, got={raw_int})",
        );
    }

    /// `score_2bit_internal_folded` should recover `Σ c[a[j]] · c[b[j]] ·
    /// D'_j²` after dividing by `CODEBOOK_SCALE²` and the square of the
    /// factors' scale. Factors are quantized into the Q15 form the SIMD
    /// kernels consume directly, as `ErrorCorrection::new` does.
    #[test]
    fn test_score_2bit_internal_folded_matches_reference() {
        use rand::RngExt;

        let mut rng = StdRng::seed_from_u64(0xBADD00D);
        let dim = 256;
        let centroids = TQBits::Bits2.get_centroids();
        let n_trials = 16;

        for _ in 0..n_trials {
            let raw_a = sample_normal_vec(&mut rng, dim);
            let raw_b = sample_normal_vec(&mut rng, dim);
            let idx_a = encode_to_nearest_centroid(centroids, &raw_a);
            let idx_b = encode_to_nearest_centroid(centroids, &raw_b);

            let d_prime: Vec<f32> = (0..dim).map(|_| rng.random_range(0.0..2.0)).collect();
            let max_d_prime = d_prime.iter().copied().fold(0.0f32, f32::max);
            let q15_scale = f32::from(i16::MAX) / max_d_prime;
            let factors: Vec<i16> = d_prime
                .iter()
                .map(|&x| (x * q15_scale).round().clamp(0.0, f32::from(i16::MAX)) as i16)
                .collect();

            let truth: f64 = idx_a
                .iter()
                .zip(idx_b.iter())
                .zip(d_prime.iter())
                .map(|((&a, &b), &d)| {
                    f64::from(centroids[a as usize])
                        * f64::from(centroids[b as usize])
                        * f64::from(d)
                        * f64::from(d)
                })
                .sum();
            let raw_int = super::score_2bit_internal_folded(
                &pack_codes(&idx_a, 2),
                &pack_codes(&idx_b, 2),
                &factors,
            );
            let factor_scale = q15_scale / 32768.0;
            let score = raw_int as f32 / (super::CODEBOOK_SCALE_SQ * factor_scale * factor_scale);

            assert!(
                (truth as f32 - score).abs() < 16.0,
                "folded score {score} too far from reference {truth}",
            );
        }
    }
}
