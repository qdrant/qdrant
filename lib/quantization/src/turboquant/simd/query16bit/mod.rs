//! 16-bit scoring.
//!
//! Like the 8-bit width, 16-bit codes carry no codebook: a vector is
//! quantized onto the uniform integer grid `s ∈ [−32767, 32767]` with its own
//! scale (see `TurboQuantizer::quantize`), and the per-vector scale cancels
//! in the renormalized score.  A code is the grid value itself, stored as a
//! little-endian `i16`, so both scoring paths multiply `i16 × i16` pairs:
//! `madd_epi16` on x86_64, `vmull_s16` / `vmlal_s16` on aarch64.
//!
//! The asymmetric path quantizes the query onto the same grid with its own
//! scale ([`Query16bitSimd`]) and reuses the symmetric kernels, so there is
//! a single set of kernels.
//!
//! # Overflow
//!
//! Codes never take `−32768`, so a pair of products is at most
//! `2 · 32767² = 2 147 352 578 < 2³¹`: one `madd_epi16` lane (or one
//! `vmull_s16` + `vmlal_s16` lane) holds exactly one pair and no more.  The
//! x86_64 kernels therefore convert every pair sum to f32 and accumulate
//! there (exact products, f32 accumulation with relative error ~1e-7, far
//! below the grid's own error); the aarch64 kernel widens into i64 with
//! `vpadalq_s32` and is exact.

use super::SimdBackend;

/// Largest grid magnitude; codes lie in `[−GRID_MAX, GRID_MAX]`.
pub(crate) const GRID_MAX: f32 = 32767.0;

/// Bytes per code.
pub(crate) const CODE_BYTES: usize = size_of::<i16>();

/// Dot product `Σ s_a · s_b` of two 16-bit code vectors (little-endian
/// `i16`, `2 · dim` bytes each).  Dispatches to the fastest available SIMD
/// kernel.
///
/// # Panics
/// Panics if the two vectors have different lengths.
pub fn score_16bit_internal(a: &[u8], b: &[u8]) -> f32 {
    score_16bit_with(SimdBackend::detect(), a, b)
}

/// [`score_16bit_internal`] on an already detected backend.
#[inline]
fn score_16bit_with(backend: SimdBackend, a: &[u8], b: &[u8]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "score_16bit_internal: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    debug_assert!(a.len().is_multiple_of(CODE_BYTES));

    match backend {
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Avx512Vnni => unsafe { x64::score_16bit_internal_avx512(a, b) },
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Avx2 => unsafe { x64::score_16bit_internal_avx2(a, b) },
        #[cfg(target_arch = "x86_64")]
        SimdBackend::Sse => unsafe { x64::score_16bit_internal_sse(a, b) },
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        SimdBackend::NeonSdot | SimdBackend::Neon => unsafe {
            arm::score_16bit_internal_neon(a, b)
        },
        SimdBackend::Scalar => score_16bit_internal_scalar(a, b),
    }
}

/// Scalar reference of [`score_16bit_internal`].
pub fn score_16bit_internal_scalar(a: &[u8], b: &[u8]) -> f32 {
    score_16bit_internal_integer(a, b) as f32
}

/// Exact integer dot product, also used by every backend for the codes past
/// the last full SIMD block.
#[inline]
pub(crate) fn score_16bit_internal_integer(a: &[u8], b: &[u8]) -> i64 {
    let (a, _) = a.as_chunks::<CODE_BYTES>();
    let (b, _) = b.as_chunks::<CODE_BYTES>();
    a.iter()
        .zip(b)
        .map(|(&x, &y)| i64::from(i16::from_le_bytes(x)) * i64::from(i16::from_le_bytes(y)))
        .sum()
}

/// Encode a grid value into its stored little-endian bytes.
#[inline]
pub(crate) fn code_bytes(value: f64) -> [u8; CODE_BYTES] {
    (value
        .round()
        .clamp(-f64::from(GRID_MAX), f64::from(GRID_MAX)) as i16)
        .to_le_bytes()
}

/// Encoded query for asymmetric 16-bit scoring: the rotated query on the
/// 16-bit grid with its own absmax scale, scored with the symmetric kernels.
pub struct Query16bitSimd {
    /// Query codes, little-endian `i16`.
    codes: Vec<u8>,
    /// `1 / grid scale`: turns the integer dot back into query units.
    postprocess_scale: f32,
    backend: SimdBackend,
}

impl Query16bitSimd {
    pub fn new(data: &[f32]) -> Self {
        let abs_max = data
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max)
            .max(f32::EPSILON);
        let scale = f64::from(GRID_MAX) / f64::from(abs_max);
        let codes = data
            .iter()
            .flat_map(|&x| code_bytes(f64::from(x) * scale))
            .collect();
        Self {
            codes,
            postprocess_scale: (1.0 / scale) as f32,
            backend: SimdBackend::detect(),
        }
    }

    /// `Σ q_i · s_i` against one encoded vector's codes.
    pub fn dotprod(&self, vector: &[u8]) -> f32 {
        let vector = &vector[..self.codes.len()];
        self.postprocess_scale * score_16bit_with(self.backend, &self.codes, vector)
    }

    /// Batch counterpart of [`Self::dotprod`] for vectors stored `stride`
    /// bytes apart: `out[v]` ← score of the vector at `data[v * stride..]`.
    ///
    /// # Panics
    /// Panics if `stride` is shorter than an encoded vector or `data` is too
    /// short for `out.len()` vectors.
    pub fn dotprod_batch(&self, data: &[u8], stride: usize, out: &mut [f32]) {
        let Some(last) = out.len().checked_sub(1) else {
            return;
        };
        let vector_bytes = self.codes.len();
        assert!(
            stride >= vector_bytes && data.len() >= last * stride + vector_bytes,
            "Query16bitSimd::dotprod_batch: {} vectors of {vector_bytes} bytes at stride \
             {stride} don't fit into {} data bytes",
            out.len(),
            data.len(),
        );
        for (v, out) in out.iter_mut().enumerate() {
            let vector = &data[v * stride..][..vector_bytes];
            *out = self.postprocess_scale * score_16bit_with(self.backend, &self.codes, vector);
        }
    }
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod arm;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use arm::score_16bit_internal_neon;

#[cfg(target_arch = "x86_64")]
mod x64;
#[cfg(target_arch = "x86_64")]
pub use x64::{score_16bit_internal_avx2, score_16bit_internal_avx512, score_16bit_internal_sse};

#[cfg(test)]
pub(crate) mod tests {
    use rand::prelude::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::turboquant::simd::shared::sample_normal_vec;

    /// Vector lengths (in codes) hitting every block size (8/16/32 codes) and
    /// the multi-block loops, with and without tails.
    const DIMS: &[usize] = &[
        1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 128, 129, 384, 960, 1023, 1536,
    ];

    /// Random codes over the whole grid.
    pub(crate) fn random_codes(rng: &mut StdRng, dim: usize) -> Vec<u8> {
        (0..dim)
            .flat_map(|_| (rng.random_range(-32767..=32767) as i16).to_le_bytes())
            .collect()
    }

    /// The x86_64 kernels accumulate in f32, so their error scales with the
    /// magnitude of the terms, `Σ |a_i · b_i|`, not with the (possibly
    /// cancelled) result.
    fn assert_close(actual: f32, expected: i64, magnitude: i64, what: &str) {
        let expected = expected as f64;
        let tol = 1e-6 * (magnitude as f64).max(1.0);
        assert!(
            (f64::from(actual) - expected).abs() <= tol,
            "{what}: expected {expected}, got {actual}"
        );
    }

    /// Every kernel the host supports against the exact integer dot.
    #[test]
    fn test_kernels_match_integer() {
        let mut rng = StdRng::seed_from_u64(16);
        for &dim in DIMS {
            for _ in 0..8 {
                let a = random_codes(&mut rng, dim);
                let b = random_codes(&mut rng, dim);
                let expected = score_16bit_internal_integer(&a, &b);
                let magnitude = abs_magnitude(&a, &b);
                assert_eq!(score_16bit_internal_scalar(&a, &b), expected as f32);
                let got = score_16bit_internal(&a, &b);
                assert_close(got, expected, magnitude, &format!("dispatch dim={dim}"));
                #[cfg(target_arch = "x86_64")]
                unsafe {
                    if std::is_x86_feature_detected!("sse4.1") {
                        let got = score_16bit_internal_sse(&a, &b);
                        assert_close(got, expected, magnitude, &format!("sse dim={dim}"));
                    }
                    if std::is_x86_feature_detected!("avx2") {
                        let got = score_16bit_internal_avx2(&a, &b);
                        assert_close(got, expected, magnitude, &format!("avx2 dim={dim}"));
                    }
                    if std::is_x86_feature_detected!("avx512f")
                        && std::is_x86_feature_detected!("avx512bw")
                    {
                        let got = score_16bit_internal_avx512(&a, &b);
                        assert_close(got, expected, magnitude, &format!("avx512 dim={dim}"));
                    }
                }
            }
        }
    }

    fn abs_magnitude(a: &[u8], b: &[u8]) -> i64 {
        let abs = |v: &[u8]| -> Vec<u8> {
            v.as_chunks::<CODE_BYTES>()
                .0
                .iter()
                .flat_map(|&c| i16::from_le_bytes(c).abs().to_le_bytes())
                .collect()
        };
        score_16bit_internal_integer(&abs(a), &abs(b))
    }

    /// The largest pair sums (`±32767` everywhere) must not wrap in any
    /// intermediate.
    #[test]
    fn test_extreme_codes_do_not_overflow() {
        for (x, y) in [(32767i16, 32767i16), (-32767, -32767), (32767, -32767)] {
            for dim in [1, 16, 33, 4096, 65_536] {
                let a: Vec<u8> = (0..dim).flat_map(|_| x.to_le_bytes()).collect();
                let b: Vec<u8> = (0..dim).flat_map(|_| y.to_le_bytes()).collect();
                let expected = score_16bit_internal_integer(&a, &b);
                let magnitude = abs_magnitude(&a, &b);
                let got = score_16bit_internal(&a, &b);
                assert_close(got, expected, magnitude, &format!("{x}·{y} dim={dim}"));
            }
        }
    }

    /// The asymmetric path reproduces the float dot against the grid values
    /// up to the query's own 16-bit quantization.
    #[test]
    fn test_dotprod_matches_float() {
        let mut rng = StdRng::seed_from_u64(42);
        for &dim in DIMS {
            let query = sample_normal_vec(&mut rng, dim);
            let codes = random_codes(&mut rng, dim);
            let values: Vec<f64> = codes
                .as_chunks::<CODE_BYTES>()
                .0
                .iter()
                .map(|&c| f64::from(i16::from_le_bytes(c)))
                .collect();
            let expected: f64 = query
                .iter()
                .zip(&values)
                .map(|(&q, &s)| f64::from(q) * s)
                .sum();
            let q = Query16bitSimd::new(&query);
            let actual = f64::from(q.dotprod(&codes));
            // Query rounding: ≤ ½ grid step of |q|max per dim, random sign.
            let q_max = query.iter().fold(0.0f32, |m, &x| m.max(x.abs()));
            let tol = 1.0 + 4.0 * (dim as f64).sqrt() * f64::from(q_max);
            assert!(
                (expected - actual).abs() < tol,
                "dim={dim}: expected {expected}, got {actual} (tol {tol})"
            );

            // The batch path is the per-vector path, vector by vector.
            let stride = codes.len() + 6;
            let mut data = Vec::new();
            let mut others = Vec::new();
            for _ in 0..5 {
                let c = random_codes(&mut rng, dim);
                data.extend_from_slice(&c);
                data.extend_from_slice(&[0xAB; 6]);
                others.push(c);
            }
            let mut out = vec![0.0; 5];
            q.dotprod_batch(&data, stride, &mut out);
            for (o, c) in out.iter().zip(&others) {
                assert_eq!(o.to_bits(), q.dotprod(c).to_bits(), "dim={dim}");
            }
        }
    }
}
