//! 8-bit scoring.
//!
//! Unlike the narrower widths, 8-bit codes carry no codebook: a vector is
//! quantized onto the uniform integer grid `s ∈ [−127, 127]` with its own
//! scale (see `TurboQuantizer::quantize`), and the per-vector scale cancels
//! in the renormalized score.  A code byte stores `u = s + 128`, so the
//! "lookup" is the identity on x86_64 (`maddubs` / `VPDPBUSD` take `u`
//! directly, the `+128` is undone by the query-side bias) and a sign flip
//! (`u ^ 0x80 = s`) on aarch64, where `SDOT` multiplies signed bytes.
//!
//! Asymmetric scoring goes through the shared [`QuerySimd`] kernels with
//! `PLANES = 1`; the symmetric paths (`score_8bit_internal*`) have their own
//! kernels in the `arm` / `x64` submodules.

use super::SimdBackend;
use super::query::{Code, Encoding, QuerySimd};

/// Offset of the stored code: `s = u − CODE_OFFSET`.
pub(crate) const CODE_OFFSET: u8 = 128;

/// Integer encoding of the 8-bit width for the shared asymmetric kernels.
/// The codebook is unused — [`code_value`] maps codes to values — and the
/// query ranges are the 4-bit ones: the same `maddubs` pair bound
/// (`2 · 255 · 64`) holds on x86_64, and `SDOT` has no i16 intermediate.
pub(super) const ENCODING: Encoding = Encoding {
    codebook: [0; 16],
    offset: CODEBOOK_OFFSET,
    scale: 1.0,
    query_high_coef: QUERY_HIGH_COEF,
    query_abs_max: QUERY_ABS_MAX,
};

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const CODEBOOK_OFFSET: i64 = 0;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const QUERY_HIGH_COEF: i64 = 256;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
const QUERY_ABS_MAX: f32 = 32639.0;

#[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
const CODEBOOK_OFFSET: i64 = CODE_OFFSET as i64;
#[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
const QUERY_HIGH_COEF: i64 = 128;
#[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
const QUERY_ABS_MAX: f32 = 8127.0;

/// The value the asymmetric kernels multiply for `code`, in the arch-native
/// [`Code`] form: `u` itself where the codebook is stored unsigned, `s`
/// where it is signed.
#[inline(always)]
pub(crate) const fn code_value(code: u8) -> Code {
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        (code ^ CODE_OFFSET) as i8
    }
    #[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
    {
        code
    }
}

/// The signed grid value of a stored code.
#[inline(always)]
pub(crate) const fn code_signed(code: u8) -> i32 {
    code as i32 - CODE_OFFSET as i32
}

/// Encode a signed grid value `s ∈ [−127, 127]` into its stored code.
#[inline(always)]
pub(crate) const fn signed_code(value: i32) -> u8 {
    (value + CODE_OFFSET as i32) as u8
}

/// Encoded query for asymmetric 8-bit scoring: [`QuerySimd`] over one code
/// per byte with a two-byte query.
pub type Query8bitSimd = QuerySimd<1, 2>;

/// Dot product `Σ s_a · s_b` of two 8-bit code vectors on the signed grid.
/// Dispatches to the fastest available SIMD kernel; exact on every
/// backend.
///
/// # Panics
/// Panics if the two vectors have different lengths.
pub fn score_8bit_internal(a: &[u8], b: &[u8]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "score_8bit_internal: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    match SimdBackend::detect() {
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Avx512Vnni => unsafe { x64::score_8bit_internal_avx512_vnni(a, b) },
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Avx2 => unsafe { x64::score_8bit_internal_avx2(a, b) },
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Sse => unsafe { x64::score_8bit_internal_sse(a, b) },
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        SimdBackend::NeonSdot => unsafe { arm::score_8bit_internal_neon_sdot(a, b) },
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        SimdBackend::Neon => unsafe { arm::score_8bit_internal_neon(a, b) },
        SimdBackend::Scalar => score_8bit_internal_scalar(a, b),
    }
}

/// Scalar reference of [`score_8bit_internal`].
pub fn score_8bit_internal_scalar(a: &[u8], b: &[u8]) -> f32 {
    score_8bit_internal_integer(a, b) as f32
}

/// Integer kernel shared by all backends for the bytes past the last full
/// SIMD block.
#[inline]
pub(crate) fn score_8bit_internal_integer(a: &[u8], b: &[u8]) -> i64 {
    a.iter()
        .zip(b)
        .map(|(&x, &y)| i64::from(code_signed(x) * code_signed(y)))
        .sum()
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod arm;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use arm::{score_8bit_internal_neon, score_8bit_internal_neon_sdot};

#[cfg(target_arch = "x86_64")]
mod x64;
#[cfg(target_arch = "x86_64")]
pub use x64::{score_8bit_internal_avx2, score_8bit_internal_avx512_vnni, score_8bit_internal_sse};

#[cfg(test)]
mod tests {
    use rand::prelude::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::turboquant::simd::shared::{random_bytes, sample_normal_vec};

    /// Vector lengths hitting every block size (16/32/64) with and without
    /// tails.
    const DIMS: &[usize] = &[
        1, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 128, 384, 1023, 1536,
    ];

    #[test]
    fn test_code_roundtrip() {
        for s in -127..=127 {
            let code = signed_code(s);
            assert_eq!(code_signed(code), s);
            assert_eq!(i64::from(code_value(code)) - ENCODING.offset, i64::from(s));
        }
    }

    /// The asymmetric kernel must reproduce the float dot against the grid
    /// values up to the query's own quantization.
    #[test]
    fn test_dotprod_matches_float() {
        let mut rng = StdRng::seed_from_u64(42);
        for &dim in DIMS {
            for _ in 0..16 {
                let query = sample_normal_vec(&mut rng, dim);
                let values: Vec<i32> = (0..dim).map(|_| rng.random_range(-127..=127)).collect();
                let codes: Vec<u8> = values.iter().map(|&s| signed_code(s)).collect();
                let expected: f64 = query
                    .iter()
                    .zip(&values)
                    .map(|(&q, &s)| f64::from(q) * f64::from(s))
                    .sum();
                let actual = f64::from(Query8bitSimd::new(&query).dotprod(&codes));
                // Query quantization error: ~127 · |q|_max / 8127 per dim, random sign.
                let tol = 0.5 + 0.05 * (dim as f64).sqrt() * 127.0 * 4.0 / 8127.0 * 10.0;
                assert!(
                    (expected - actual).abs() < tol,
                    "dim={dim}: expected {expected}, got {actual} (tol {tol})"
                );
            }
        }
    }

    #[test]
    fn test_symmetric_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(3);
        for &dim in DIMS {
            let a = random_bytes(&mut rng, dim);
            let b = random_bytes(&mut rng, dim);
            let expected = score_8bit_internal_integer(&a, &b) as f32;
            assert_eq!(score_8bit_internal(&a, &b), expected, "dim={dim}");
        }
        // All-extreme codes stay exact (no saturation in any intermediate).
        for (x, y) in [(0x00, 0x00), (0x00, 0xFF), (0xFF, 0xFF), (0x01, 0x01)] {
            let a = vec![x; 65_536];
            let b = vec![y; 65_536];
            assert_eq!(
                score_8bit_internal(&a, &b),
                score_8bit_internal_integer(&a, &b) as f32,
            );
        }
    }
}
