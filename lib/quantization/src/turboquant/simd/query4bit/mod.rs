// # Encoding strategy (arch-specific)
//
// Both architectures share the same storage layout for `query_data`
// (`Vec<[[i8; 16]; 2]>`) and the same `dot_raw` → float reconstruction formula
// (`postprocess_scale · (dot_raw − bias_correction)`), but the numerical
// encoding of the codebook and query differs to squeeze the most precision out
// of each SIMD instruction set:
//
// * **aarch64** — `vmull_s8` and `sdot` are true `i8 × i8 → i16/i32` signed
//   multiplies, so we can store the full `i8 ∈ [−127, 127]` codebook directly
//   with no offset.  Query halves are full `i8 ∈ [−128, 127]` combined as
//   `q_signed = 256 · high + low`, giving ~15.9-bit query precision.  The
//   reconstruction needs no bias correction.
//
// * **x86_64** — `_mm_maddubs_epi16` and `VPDPBUSD` consume one `u8` and one
//   `i8` operand.  To carry full 8-bit codebook magnitude we feed it unsigned
//   `c_u ∈ [0, 255]` (shifted from signed by `+128`) and keep the query halves
//   narrower to stay under i16 pair-sum saturation:
//     c_u ≤ 255, q ∈ [−64, 63] → |pair| ≤ 2·255·64 = 32 640 < 32 767 ✓
//   Query halves are 7-bit signed combined as `q_signed = 128 · high + low`,
//   giving ~13.9-bit query precision.  The shift contributes a per-query
//   bias `128 · Σ q_signed` that we subtract once in `dotprod`.
//
// Both codebooks derive from `CENTROIDS_4BIT` (Lloyd-Max on N(0,1)); see
// `test_codebook_matches_lloyd_max` for the consistency check.

/// `max|c|` over `CENTROIDS_4BIT` — the extreme centroid.  Shared by both archs.
const CODEBOOK_ABS_MAX: f32 = 2.733;

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

/// Read the signed codebook value at `idx` as an `i64`.  On aarch64 the
/// constant is already i8 signed; on x86_64 (and fallback) it's stored
/// unsigned (+OFFSET) and the caller's `bias_correction` later undoes the shift.
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

pub struct Query4bitSimd {
    /// Query encoded as a balanced signed-i8 split
    /// `q_signed = QUERY_HIGH_COEF · high + low`, each chunk stored as
    /// `[low, high]`.  Both halves are signed — `low, high ∈ [−128, 127]` on
    /// aarch64 (K=256) or `[−64, 63]` on x86_64 (K=128).
    query_data: Vec<[[i8; 16]; 2]>,
    /// `1 / (q_scale · c_scale)` — prefactor from integer to float dot product.
    postprocess_scale: f32,
    /// `CODEBOOK_OFFSET · Σ q_signed[j]` — subtract from `dot_raw` to recover
    /// the true signed integer dot product.  Computed once in `new()`; doesn't
    /// depend on the PQ vector, so `dotprod` does zero per-vector scalar work.
    /// `0` on aarch64, where the codebook is already stored signed.
    bias_correction: i64,
}

impl Query4bitSimd {
    /// Query dim must be a multiple of 32.  The NEON SDOT and AVX-512 VNNI paths
    /// both consume 2 query_data chunks (= 32 query elements) per iteration;
    /// with `dim % 32 == 0` we get `query_data.len()` even and can drop all tail
    /// handling.  Every PQ dimension we ship (128, 256, 384, 512, 768, 1024,
    /// 1536, 2048, 4096) already satisfies this.
    pub fn new(data: &[f32]) -> Self {
        assert!(
            data.len().is_multiple_of(32),
            "Query4bitSimd requires query dim to be a multiple of 32 (got {})",
            data.len(),
        );

        // Scale query to signed integer `q_signed ∈ [−QUERY_ABS_MAX, +QUERY_ABS_MAX]`.
        // Balanced split `q_signed = QUERY_HIGH_COEF · high + low` keeps both
        // halves inside i8.  QUERY_HIGH_COEF is 256 on aarch64 (full-range i8
        // halves → 16-bit query) or 128 on x86_64 (7-bit halves → 14-bit
        // query, keeps maddubs within i16 given full-range u8 codebook).
        let q_abs_max = data
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max)
            .max(f32::EPSILON);
        let q_scale = QUERY_ABS_MAX / q_abs_max;

        let k = QUERY_HIGH_COEF as i32;
        let half_k = k / 2;
        let clamp_hi = QUERY_ABS_MAX;
        let clamp_lo = -QUERY_ABS_MAX;

        let num_chunks = data.len() / 16;
        let mut query_data: Vec<[[i8; 16]; 2]> = Vec::with_capacity(num_chunks);
        let mut sum_q_signed: i64 = 0;
        for chunk_idx in 0..num_chunks {
            let mut low = [0_i8; 16];
            let mut high = [0_i8; 16];
            for i in 0..16 {
                let q_signed = (data[chunk_idx * 16 + i] * q_scale)
                    .round()
                    .clamp(clamp_lo, clamp_hi) as i32;
                // Balanced split: force low into `[−k/2, k/2 − 1]` so both
                // halves land inside i8 (not the default Rust `%` behaviour,
                // which mirrors the sign of the dividend).
                let l_mod = q_signed.rem_euclid(k);
                let l = if l_mod >= half_k { l_mod - k } else { l_mod } as i8;
                let h = ((q_signed - i32::from(l)) / k) as i8;
                low[i] = l;
                high[i] = h;
                sum_q_signed += i64::from(q_signed);
            }
            query_data.push([low, high]);
        }

        // dot_raw = Σ q_signed · c_raw, where `c_raw` is whatever the SIMD
        // path loads: signed `i8` on aarch64 (already = c_signed, OFFSET = 0)
        // or unsigned `u8` on x86_64 (= c_signed + 128).  Either way,
        //   Σ q_signed · c_signed = dot_raw − OFFSET · sum_q_signed,
        // and the float reconstruction divides by `q_scale · c_scale`.
        let postprocess_scale = 1.0 / (q_scale * CODEBOOK_SCALE);
        let bias_correction = CODEBOOK_OFFSET * sum_q_signed;

        Self {
            query_data,
            postprocess_scale,
            bias_correction,
        }
    }

    pub fn dotprod(&self, vector: &[u8]) -> f32 {
        // No per-vector correction loop: `bias_correction` was baked in at `new()`.
        let dot_raw = self.dotprod_raw_best(vector);
        self.postprocess_scale * (dot_raw - self.bias_correction) as f32
    }

    #[inline]
    fn dotprod_raw_best(&self, vector: &[u8]) -> i64 {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512vnni")
            {
                return unsafe { self.dotprod_raw_avx512_vnni(vector) };
            }
            if std::is_x86_feature_detected!("avx2") {
                return unsafe { self.dotprod_raw_avx2(vector) };
            }
            if std::is_x86_feature_detected!("sse4.1") && std::is_x86_feature_detected!("ssse3") {
                return unsafe { self.dotprod_raw_sse(vector) };
            }
        }
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("dotprod") {
                return unsafe { self.dotprod_raw_neon_sdot(vector) };
            }
            return unsafe { self.dotprod_raw_neon(vector) };
        }
        #[allow(unreachable_code)]
        self.dotprod_raw(vector)
    }

    /// Compute `Σ q_signed[j] · c_raw[v[j]]` over 16 lanes per chunk, returned
    /// as `acc_low + QUERY_HIGH_COEF · acc_high`.  Whatever integer codebook
    /// the current arch stores (`CODEBOOK_I8` on aarch64, `CODEBOOK_U8` on
    /// x86_64), the reconstruction in `Query4bitSimd::dotprod` subtracts
    /// `CODEBOOK_OFFSET · sum_q_signed` (0 on aarch64, `128·sum_q_signed` on
    /// x86_64) to recover `Σ q · c_signed`.
    ///
    /// `vector` is PQ-encoded with two 4-bit codebook indices packed per byte:
    /// the low nibble is the index for the even lane (j = 2k), the high nibble
    /// for the odd lane (j = 2k + 1).
    pub fn dotprod_raw(&self, vector: &[u8]) -> i64 {
        let mut acc_low: i64 = 0;
        let mut acc_high: i64 = 0;
        for ([low, high], v) in self.query_data.iter().zip(vector.chunks(8)) {
            for i in 0..16 {
                let byte = v[i / 2];
                let idx = if i & 1 == 0 { byte & 0x0F } else { byte >> 4 };
                let c = codebook_value_i64(idx);
                acc_low += i64::from(low[i]) * c;
                acc_high += i64::from(high[i]) * c;
            }
        }
        acc_low + QUERY_HIGH_COEF * acc_high
    }
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod arm;

#[cfg(target_arch = "x86_64")]
mod x64;

/// Shared test helpers used by the accuracy tests below and by the per-arch
/// SIMD parity / saturation tests in [`arm`] and [`x64`].
#[cfg(test)]
mod shared {
    use rand::prelude::StdRng;
    use rand::seq::SliceRandom;
    use rand_distr::{Distribution, StandardNormal};

    use super::Query4bitSimd;

    /// All parity/overflow tests use dims from this list (all multiples of 32).
    pub const PARITY_DIMS: &[usize] = &[32, 128, 256, 1024, 2048];

    /// Packs a sequence of 4-bit indices (each in [0, 15]) two per byte:
    /// low nibble → even lane, high nibble → odd lane.
    pub fn pack_nibbles(indices: &[u8]) -> Vec<u8> {
        assert_eq!(indices.len() % 2, 0);
        indices
            .chunks_exact(2)
            .map(|p| p[0] | (p[1] << 4))
            .collect()
    }

    pub fn sample_normal_vec(rng: &mut StdRng, len: usize) -> Vec<f32> {
        (0..len).map(|_| StandardNormal.sample(rng)).collect()
    }

    /// Map each raw float to the index of its nearest centroid.
    pub fn encode_to_nearest_centroid(centroids: &[f32], raw: &[f32]) -> Vec<u8> {
        raw.iter()
            .map(|&v| {
                centroids
                    .iter()
                    .enumerate()
                    .min_by(|a, b| (a.1 - v).abs().partial_cmp(&(b.1 - v).abs()).unwrap())
                    .map(|(k, _)| k as u8)
                    .unwrap()
            })
            .collect()
    }

    /// Parity-test helper: query ~ N(0,1), balanced index distribution (each
    /// centroid appears `dim/16` times, shuffled).  Index distribution doesn't
    /// affect scalar-vs-SIMD parity; we just need non-trivial data.
    pub fn random_inputs(rng: &mut StdRng, dim: usize) -> (Query4bitSimd, Vec<u8>) {
        let query = sample_normal_vec(rng, dim);
        let mut indices: Vec<u8> = (0..dim).map(|i| (i % 16) as u8).collect();
        indices.shuffle(rng);
        (Query4bitSimd::new(&query), pack_nibbles(&indices))
    }
}

/// Accuracy / precision tests for the public `Query4bitSimd` API.
///
/// Per-arch SIMD parity tests and the saturation-safety test live in the
/// `arm` / `x64` submodules — they verify that each SIMD implementation
/// matches the scalar reference `dotprod_raw`, not the float ground truth.
#[cfg(test)]
mod tests {
    // Anonymous `use _` brings the trait into scope for `StdRng::seed_from_u64`
    // without introducing a name rustc then flags as unused.
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::shared::{encode_to_nearest_centroid, pack_nibbles, sample_normal_vec};
    use super::{CODEBOOK_ABS_MAX, Query4bitSimd};
    use crate::turboquant::lloyd_max;

    /// Whichever codebook representation the current arch uses (signed i8 on
    /// aarch64, shifted u8 on x86_64), it must match what the runtime recipe
    /// would produce from `CENTROIDS_4BIT`.
    #[test]
    fn test_codebook_matches_lloyd_max() {
        let centroids = lloyd_max::get_centroids(4);
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

    /// Reconstruction accuracy on realistic PQ inputs: query ∼ N(0,1), vector
    /// drawn from N(0,1) then mapped to its nearest centroid.  We compare
    /// `simd.dotprod()` against the "ideal" PQ dot (sum of `q[j] · c[v[j]]`
    /// with float-precision centroid lookup) — the error our SIMD path adds
    /// over a hypothetical perfect-precision PQ should be tiny.
    #[test]
    fn test_dotprod_matches_float() {
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 256;
        let n_trials = 64;

        let centroids = lloyd_max::get_centroids(4);

        for _ in 0..n_trials {
            let query = sample_normal_vec(&mut rng, dim);
            let v_raw = sample_normal_vec(&mut rng, dim);
            let indices = encode_to_nearest_centroid(centroids, &v_raw);
            let v_pq: Vec<f32> = indices.iter().map(|&k| centroids[k as usize]).collect();

            let pq_dot: f32 = query.iter().zip(v_pq.iter()).map(|(a, b)| a * b).sum();
            let simd_dot = Query4bitSimd::new(&query).dotprod(&pack_nibbles(&indices));

            // SIMD quantization error scales like √d × σ_q × ε_c.  For d=256,
            // unit-variance query and i8 codebook precision, typical error is
            // ≲0.1 and 3σ tail excursions stay under 0.5.
            assert!(
                (pq_dot - simd_dot).abs() < 0.5,
                "simd_dot {simd_dot} too far from ideal PQ dot {pq_dot}",
            );
        }
    }

    /// Quantitative proof that our SIMD quantization is negligible next to PQ
    /// centroid snapping: RMS error added by our encoding is at least 5×
    /// smaller than the RMS error PQ itself introduces.  If this invariant
    /// ever flips, something in the quantization pipeline lost precision.
    #[test]
    fn test_simd_noise_below_pq_noise() {
        let mut rng = StdRng::seed_from_u64(123);
        let dim = 256;
        let n_trials = 256;

        let centroids = lloyd_max::get_centroids(4);

        let mut sq_pq_noise = 0.0_f64;
        let mut sq_simd_noise = 0.0_f64;

        for _ in 0..n_trials {
            let query = sample_normal_vec(&mut rng, dim);
            let v_raw = sample_normal_vec(&mut rng, dim);
            let indices = encode_to_nearest_centroid(centroids, &v_raw);
            let v_pq: Vec<f32> = indices.iter().map(|&k| centroids[k as usize]).collect();

            let true_dot: f64 = query
                .iter()
                .zip(v_raw.iter())
                .map(|(a, b)| f64::from(*a) * f64::from(*b))
                .sum();
            let pq_dot: f64 = query
                .iter()
                .zip(v_pq.iter())
                .map(|(a, b)| f64::from(*a) * f64::from(*b))
                .sum();
            let simd_dot = f64::from(Query4bitSimd::new(&query).dotprod(&pack_nibbles(&indices)));

            sq_pq_noise += (pq_dot - true_dot).powi(2);
            sq_simd_noise += (simd_dot - pq_dot).powi(2);
        }

        let rms_pq_noise = (sq_pq_noise / f64::from(n_trials)).sqrt();
        let rms_simd_noise = (sq_simd_noise / f64::from(n_trials)).sqrt();

        // Print for easy comparison across encoding variants.
        eprintln!(
            "NOISE at dim={dim}: pq_rms={rms_pq_noise:.4} simd_rms={rms_simd_noise:.4} \
             ratio={:.2}×",
            rms_pq_noise / rms_simd_noise,
        );

        assert!(
            rms_simd_noise * 5.0 < rms_pq_noise,
            "SIMD noise RMS {rms_simd_noise:.4} should be << PQ noise RMS \
             {rms_pq_noise:.4} (ratio {:.2}×)",
            rms_pq_noise / rms_simd_noise,
        );
    }
}
