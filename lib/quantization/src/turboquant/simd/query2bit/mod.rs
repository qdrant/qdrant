//! 2-bit product-quantization scoring.
//!
//! Mirrors [`super::query4bit`]: query is quantized to two signed 7-bit
//! halves (combined via `q_signed = QUERY_HIGH_COEF · high + low`), the
//! codebook is the arch-native storage form (`CODEBOOK_I8` on aarch64,
//! `CODEBOOK_U8` on x86_64), and scoring uses bias-corrected integer
//! accumulation.  The only real differences from 4-bit are:
//!
//! 1. 4 centroids instead of 16 — `CENTROIDS_2BIT` from `lloyd_max`.
//! 2. 4 codes packed per byte (2 bits each) instead of 2 nibbles per byte.
//! 3. The SIMD unpack uses a **pair-table** trick: a nibble of the packed
//!    data byte encodes a pair of 2-bit codes (16 possible combinations),
//!    which maps one-to-one to a 16-entry `vqtbl1q_s8` / `pshufb` table.
//!    Two such lookups (even / odd centroid of each pair), zipped, give a
//!    natural-order `int8x16` of 16 centroid bytes per 4 packed data bytes.

/// `max|c|` over `CENTROIDS_2BIT` — the extreme centroid magnitude.
const CODEBOOK_ABS_MAX: f32 = 1.510;

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

pub struct Query2bitSimd {
    /// Query encoded as a balanced signed-i8 split
    /// `q_signed = QUERY_HIGH_COEF · high + low`, each chunk stored as
    /// `[low, high]`.  Both halves are signed — `low, high ∈ [−128, 127]`
    /// on aarch64 (K=256) or `[−64, 63]` on x86_64 (K=128).
    ///
    /// Each chunk covers 16 dims → 4 packed data bytes.
    query_data: Vec<[[i8; 16]; 2]>,
    /// `1 / (q_scale · c_scale)` — integer-to-float reconstruction factor.
    postprocess_scale: f32,
    /// `CODEBOOK_OFFSET · Σ q_signed[j]` — subtracted from `dot_raw` to
    /// recover the true signed integer dot product.  `0` on aarch64.
    bias_correction: i64,
}

impl Query2bitSimd {
    /// Query dim must be a multiple of 16 (one chunk); every PQ dim we ship
    /// already satisfies this.
    pub fn new(data: &[f32]) -> Self {
        assert!(
            data.len().is_multiple_of(16),
            "Query2bitSimd requires query dim to be a multiple of 16 (got {})",
            data.len(),
        );

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
                let l_mod = q_signed.rem_euclid(k);
                let l = if l_mod >= half_k { l_mod - k } else { l_mod } as i8;
                let h = ((q_signed - i32::from(l)) / k) as i8;
                low[i] = l;
                high[i] = h;
                sum_q_signed += i64::from(q_signed);
            }
            query_data.push([low, high]);
        }

        let postprocess_scale = 1.0 / (q_scale * CODEBOOK_SCALE);
        let bias_correction = CODEBOOK_OFFSET * sum_q_signed;

        Self {
            query_data,
            postprocess_scale,
            bias_correction,
        }
    }

    pub fn dotprod(&self, vector: &[u8]) -> f32 {
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

    /// Compute `Σ q_signed[j] · c_raw[v[j]]` over 16 lanes per chunk,
    /// returned as `acc_low + QUERY_HIGH_COEF · acc_high`.
    ///
    /// `vector` is PQ-encoded with four 2-bit codes packed per byte:
    /// bits `[2k..2k+2]` for `k ∈ 0..=3` hold codes `0..=3` of the byte,
    /// in low-to-high bit order.
    pub fn dotprod_raw(&self, vector: &[u8]) -> i64 {
        let mut acc_low: i64 = 0;
        let mut acc_high: i64 = 0;
        for ([low, high], v) in self.query_data.iter().zip(vector.chunks_exact(4)) {
            for i in 0..16 {
                let byte = v[i / 4];
                let shift = 2 * (i % 4);
                let idx = (byte >> shift) & 0x03;
                let c = codebook_value_i64(idx);
                acc_low += i64::from(low[i]) * c;
                acc_high += i64::from(high[i]) * c;
            }
        }
        acc_low + QUERY_HIGH_COEF * acc_high
    }
}

/// Dot product between two already-encoded 2-bit PQ vectors.  Dispatches to
/// the fastest available SIMD implementation and falls back to
/// [`score_2bit_internal_scalar`] otherwise.
///
/// # Panics
/// Panics if the two vectors have different lengths, or if the shared length
/// is not a multiple of 4 bytes (which corresponds to `dim % 16 == 0`).
pub fn score_2bit_internal(a: &[u8], b: &[u8]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "score_2bit_internal: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    assert!(
        a.len().is_multiple_of(4),
        "score_2bit_internal requires vector length to be a multiple of 4 bytes (dim % 16 == 0), got {}",
        a.len(),
    );

    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512bw")
            && std::is_x86_feature_detected!("avx512vnni")
        {
            return unsafe { x64::score_2bit_internal_avx512_vnni(a, b) };
        }
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { x64::score_2bit_internal_avx2(a, b) };
        }
        if std::is_x86_feature_detected!("sse4.1") && std::is_x86_feature_detected!("ssse3") {
            return unsafe { x64::score_2bit_internal_sse(a, b) };
        }
    }
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        if std::arch::is_aarch64_feature_detected!("dotprod") {
            return unsafe { arm::score_2bit_internal_neon_sdot(a, b) };
        }
        return unsafe { arm::score_2bit_internal_neon(a, b) };
    }
    #[allow(unreachable_code)]
    score_2bit_internal_scalar(a, b)
}

/// Scalar reference for [`score_2bit_internal`] — see the 4-bit counterpart
/// for the design rationale.
pub fn score_2bit_internal_scalar(a: &[u8], b: &[u8]) -> f32 {
    let mut acc: i64 = 0;
    for (&byte_a, &byte_b) in a.iter().zip(b.iter()) {
        for k in 0..4 {
            let shift = 2 * k;
            let a_k = (byte_a >> shift) & 0x03;
            let b_k = (byte_b >> shift) & 0x03;
            acc += codebook_signed_i64(a_k) * codebook_signed_i64(b_k);
        }
    }
    acc as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod arm;

#[cfg(target_arch = "x86_64")]
mod x64;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use arm::{score_2bit_internal_neon, score_2bit_internal_neon_sdot};
#[cfg(target_arch = "x86_64")]
pub use x64::{score_2bit_internal_avx2, score_2bit_internal_avx512_vnni, score_2bit_internal_sse};

/// Shared test helpers used by the accuracy tests below and by the per-arch
/// SIMD parity / saturation tests in [`arm`] and [`x64`].
#[cfg(test)]
mod shared {
    use rand::prelude::StdRng;
    use rand::seq::SliceRandom;
    use rand_distr::{Distribution, StandardNormal};

    use super::Query2bitSimd;

    /// All parity/overflow tests use dims from this list (all multiples of 16).
    pub const PARITY_DIMS: &[usize] = &[16, 64, 128, 256, 1024, 2048];

    /// Packs a sequence of 2-bit indices (each in [0, 3]) four per byte:
    /// `byte = c0 | (c1 << 2) | (c2 << 4) | (c3 << 6)`.
    pub fn pack_codes_2bit(indices: &[u8]) -> Vec<u8> {
        assert_eq!(indices.len() % 4, 0);
        indices
            .chunks_exact(4)
            .map(|p| p[0] | (p[1] << 2) | (p[2] << 4) | (p[3] << 6))
            .collect()
    }

    pub fn sample_normal_vec(rng: &mut StdRng, len: usize) -> Vec<f32> {
        (0..len).map(|_| StandardNormal.sample(rng)).collect()
    }

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

    /// Parity-test helper: query ~ N(0, 1), balanced index distribution.
    pub fn random_inputs(rng: &mut StdRng, dim: usize) -> (Query2bitSimd, Vec<u8>) {
        let query = sample_normal_vec(rng, dim);
        let mut indices: Vec<u8> = (0..dim).map(|i| (i % 4) as u8).collect();
        indices.shuffle(rng);
        (Query2bitSimd::new(&query), pack_codes_2bit(&indices))
    }
}

/// Accuracy / precision tests for `Query2bitSimd` and `score_2bit_internal`.
/// Per-arch SIMD parity tests live in the `arm` / `x64` submodules.
#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::shared::{encode_to_nearest_centroid, pack_codes_2bit, sample_normal_vec};
    use super::{CODEBOOK_ABS_MAX, Query2bitSimd, score_2bit_internal_scalar};
    use crate::turboquant::lloyd_max;

    #[test]
    fn test_codebook_matches_lloyd_max() {
        let centroids = lloyd_max::get_centroids(2);
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

    #[test]
    fn test_dotprod_matches_float() {
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 256;
        let n_trials = 64;

        let centroids = lloyd_max::get_centroids(2);

        for _ in 0..n_trials {
            let query = sample_normal_vec(&mut rng, dim);
            let v_raw = sample_normal_vec(&mut rng, dim);
            let indices = encode_to_nearest_centroid(centroids, &v_raw);
            let v_pq: Vec<f32> = indices.iter().map(|&k| centroids[k as usize]).collect();

            let pq_dot: f32 = query.iter().zip(v_pq.iter()).map(|(a, b)| a * b).sum();
            let simd_dot = Query2bitSimd::new(&query).dotprod(&pack_codes_2bit(&indices));

            // i8-codebook precision is the same as 4-bit; PQ noise for 2 bits
            // is larger, but our SIMD-induced noise should still be ≲0.5.
            assert!(
                (pq_dot - simd_dot).abs() < 0.5,
                "simd_dot {simd_dot} too far from ideal PQ dot {pq_dot}",
            );
        }
    }

    /// `score_2bit_internal_scalar(a, b)` ≈ `Σ centroid(a_k) · centroid(b_k)`.
    #[test]
    fn test_score_2bit_internal_matches_centroid_product() {
        let mut rng = StdRng::seed_from_u64(0xBAD);
        let dim = 256;
        let centroids = lloyd_max::get_centroids(2);
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
            let got =
                score_2bit_internal_scalar(&pack_codes_2bit(&idx_a), &pack_codes_2bit(&idx_b));

            assert!(
                (expected - got).abs() < 0.5,
                "scalar score {got} too far from centroid product {expected}",
            );
        }
    }
}
