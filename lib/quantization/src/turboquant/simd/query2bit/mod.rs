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

/// Encoded query for asymmetric 2-bit PQ scoring.
///
/// # Encoding
/// The f32 query is quantized to signed integers
/// `q_signed ∈ [−QUERY_ABS_MAX, QUERY_ABS_MAX]` and split into two i8 halves
/// combined as `q_signed = QUERY_HIGH_COEF · high + low` (`K = 256` on
/// aarch64 for full-range i8 halves, `K = 128` on x86_64 to keep the x86
/// `maddubs` pair sum inside i16).  Storage is 16-dim chunks of `[low, high]`
/// plus a scalar-handled tail of up to 12 dims (`dim % 4 == 0`).
///
/// # Scoring
/// `dotprod_raw = Σ_j q_signed[j] · c_raw[v[j]]` accumulated from the SIMD
/// kernel's chunk pass plus the scalar tail.  The float result is
/// `postprocess_scale · (dot_raw − bias_correction)`, where `bias_correction`
/// absorbs the `+OFFSET` shift in the x86 unsigned-codebook layout and is 0
/// on aarch64 (signed codebook, no shift).
pub struct Query2bitSimd {
    /// Full 16-dim chunks of the query — each covers 16 dims → 4 packed data bytes.
    query_data: Vec<[[i8; 16]; 2]>,
    /// Trailing dims that didn't fill a 16-dim chunk — up to 12 (since
    /// `dim % 4 == 0`: tail is one of 0, 4, 8, 12 dims).
    tail_low: [i8; 12],
    tail_high: [i8; 12],
    /// Number of meaningful entries in the tail arrays (`0..=12`, multiple of 4).
    tail_dims: u8,
    /// `1 / (q_scale · c_scale)` — prefactor from integer to float dot product.
    postprocess_scale: f32,
    /// `CODEBOOK_OFFSET · Σ q_signed[j]` — subtracted from `dot_raw` to
    /// recover the true signed dot.  `0` on aarch64 (signed codebook).
    bias_correction: i64,
}

impl Query2bitSimd {
    /// Query dim must be a multiple of 4 (the 2-bit packing width: four codes
    /// per byte).  Dims that don't fill a 16-dim chunk produce up to a 12-dim
    /// tail handled scalar-wise in every SIMD path — Matryoshka-friendly.
    pub fn new(data: &[f32]) -> Self {
        assert!(
            data.len().is_multiple_of(4),
            "Query2bitSimd requires query dim to be a multiple of 4 (got {})",
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

        let encode = |value: f32| -> (i8, i8, i64) {
            let q_signed = (value * q_scale).round().clamp(clamp_lo, clamp_hi) as i32;
            let l_mod = q_signed.rem_euclid(k);
            let l = if l_mod >= half_k { l_mod - k } else { l_mod } as i8;
            let h = ((q_signed - i32::from(l)) / k) as i8;
            (l, h, i64::from(q_signed))
        };

        let num_chunks = data.len() / 16;
        let full_dims = num_chunks * 16;
        let tail_dims = data.len() - full_dims;
        debug_assert!(tail_dims < 16 && tail_dims.is_multiple_of(4));

        let mut query_data: Vec<[[i8; 16]; 2]> = Vec::with_capacity(num_chunks);
        let mut sum_q_signed: i64 = 0;
        for chunk_idx in 0..num_chunks {
            let mut low = [0_i8; 16];
            let mut high = [0_i8; 16];
            for i in 0..16 {
                let (l, h, q) = encode(data[chunk_idx * 16 + i]);
                low[i] = l;
                high[i] = h;
                sum_q_signed += q;
            }
            query_data.push([low, high]);
        }

        let mut tail_low = [0_i8; 12];
        let mut tail_high = [0_i8; 12];
        for i in 0..tail_dims {
            let (l, h, q) = encode(data[full_dims + i]);
            tail_low[i] = l;
            tail_high[i] = h;
            sum_q_signed += q;
        }

        Self {
            query_data,
            tail_low,
            tail_high,
            tail_dims: tail_dims as u8,
            postprocess_scale: 1.0 / (q_scale * CODEBOOK_SCALE),
            bias_correction: CODEBOOK_OFFSET * sum_q_signed,
        }
    }

    /// Number of `vector` bytes the encoded query expects: 4 bytes per full
    /// 16-dim chunk plus the packed tail (four 2-bit codes per byte).
    #[inline]
    pub(super) fn expected_vector_bytes(&self) -> usize {
        self.query_data.len() * 4 + (self.tail_dims as usize).div_ceil(4)
    }

    /// Score the encoded query against a 2-bit PQ-encoded `vector`
    /// (four centroid indices per byte; bits `[2k..2k+2]` hold the code for
    /// lane `k ∈ 0..=3`).  `vector.len()` must equal `ceil(dim * 2 / 8)` —
    /// full chunks first, then the tail bytes covering `tail_dims`.
    ///
    /// Dispatches at runtime to the best SIMD backend available on the host
    /// CPU (AVX-512 VNNI → AVX2 → SSE → NEON + SDOT → NEON → scalar).
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

    /// Compute `Σ q_signed[j] · c_raw[v[j]]` across all dims — full chunks
    /// plus tail.  Returns `acc_low + QUERY_HIGH_COEF · acc_high`.
    ///
    /// `vector` is PQ-encoded with four 2-bit codes packed per byte:
    /// bits `[2k..2k+2]` for `k ∈ 0..=3` hold codes `0..=3` of the byte,
    /// in low-to-high bit order.
    pub fn dotprod_raw(&self, vector: &[u8]) -> i64 {
        assert_eq!(
            vector.len(),
            self.expected_vector_bytes(),
            "Query2bitSimd::dotprod_raw: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.expected_vector_bytes(),
        );
        let mut acc_low: i64 = 0;
        let mut acc_high: i64 = 0;
        for (chunk_idx, [low, high]) in self.query_data.iter().enumerate() {
            let v = &vector[chunk_idx * 4..(chunk_idx + 1) * 4];
            for i in 0..16 {
                let byte = v[i / 4];
                let shift = 2 * (i % 4);
                let idx = (byte >> shift) & 0x03;
                let c = codebook_value_i64(idx);
                acc_low += i64::from(low[i]) * c;
                acc_high += i64::from(high[i]) * c;
            }
        }
        acc_low + QUERY_HIGH_COEF * acc_high + self.dotprod_raw_tail(vector)
    }

    /// Scalar contribution from the trailing `tail_dims` query entries.
    /// Shared by every SIMD backend so they only implement the full-chunk
    /// loop and forward the tail here.
    #[inline]
    pub(super) fn dotprod_raw_tail(&self, vector: &[u8]) -> i64 {
        if self.tail_dims == 0 {
            return 0;
        }
        let tail_byte_start = self.query_data.len() * 4;
        let mut acc_low: i64 = 0;
        let mut acc_high: i64 = 0;
        for i in 0..self.tail_dims as usize {
            let byte = vector[tail_byte_start + i / 4];
            let shift = 2 * (i % 4);
            let idx = (byte >> shift) & 0x03;
            let c = codebook_value_i64(idx);
            acc_low += i64::from(self.tail_low[i]) * c;
            acc_high += i64::from(self.tail_high[i]) * c;
        }
        acc_low + QUERY_HIGH_COEF * acc_high
    }
}

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
    score_2bit_internal_integer(a, b) as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
}

/// Integer-only scalar kernel — used by SIMD paths to fold in bytes that
/// didn't fit a full SIMD chunk, and by [`score_2bit_internal_scalar`] as its
/// inner loop.
#[inline]
pub(super) fn score_2bit_internal_integer(a: &[u8], b: &[u8]) -> i64 {
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

/// Weighted variant of [`score_2bit_internal`]: returns `Σ_j c[a[j]] · c[b[j]]
/// · weights[j]` in centroid-float space. `weights` is i16-quantized `D'_j²`
/// from TQ+ error correction (non-negative, capped at `i16::MAX − 1`); the
/// caller divides the integer sum by `weight_scale · CODEBOOK_SCALE²` to
/// recover the true f32 dot. The `i16` element type encodes the SIMD
/// invariant directly — every backend can multiply via `vmull_s16` /
/// `madd_epi16` without re-checking the high bit.
///
/// Each input byte holds four 2-bit indices (lanes 0..=3 from LSB). `weights
/// .len()` must equal `4 · a.len()`.
///
/// # Panics
/// Panics if `a` and `b` have different lengths or if `weights` has the
/// wrong length.
pub fn score_2bit_internal_weighted(a: &[u8], b: &[u8], weights: &[i16]) -> i64 {
    assert_eq!(
        a.len(),
        b.len(),
        "score_2bit_internal_weighted: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    assert_eq!(
        weights.len(),
        4 * a.len(),
        "score_2bit_internal_weighted: weights length {} != 4 · a.len() {}",
        weights.len(),
        4 * a.len(),
    );

    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2")
            && std::is_x86_feature_detected!("ssse3")
            && std::is_x86_feature_detected!("sse4.1")
        {
            return unsafe { x64::score_2bit_internal_weighted_avx2(a, b, weights) };
        }
        if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
            return unsafe { x64::score_2bit_internal_weighted_sse(a, b, weights) };
        }
    }
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        return unsafe { arm::score_2bit_internal_weighted_neon(a, b, weights) };
    }
    #[allow(unreachable_code)]
    score_2bit_internal_weighted_scalar(a, b, weights)
}

/// Scalar reference for [`score_2bit_internal_weighted`].
#[inline]
pub fn score_2bit_internal_weighted_scalar(a: &[u8], b: &[u8], weights: &[i16]) -> i64 {
    let mut acc: i64 = 0;
    for (i, (&byte_a, &byte_b)) in a.iter().zip(b.iter()).enumerate() {
        for k in 0..4 {
            let shift = 2 * k;
            let a_k = (byte_a >> shift) & 0x03;
            let b_k = (byte_b >> shift) & 0x03;
            let p = codebook_signed_i64(a_k) * codebook_signed_i64(b_k);
            acc += p * i64::from(weights[4 * i + k]);
        }
    }
    acc
}

/// Square of the codebook scale — the integer sum from
/// [`score_2bit_internal_weighted`] is divided by `weight_scale ·
/// CODEBOOK_SCALE_SQ` to get the f32 weighted dot.
pub const CODEBOOK_SCALE_SQ: f32 = CODEBOOK_SCALE * CODEBOOK_SCALE;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod arm;

#[cfg(target_arch = "x86_64")]
mod x64;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use arm::{
    score_2bit_internal_neon, score_2bit_internal_neon_sdot, score_2bit_internal_weighted_neon,
};
#[cfg(target_arch = "x86_64")]
pub use x64::{
    score_2bit_internal_avx2, score_2bit_internal_avx512_vnni, score_2bit_internal_sse,
    score_2bit_internal_weighted_avx2, score_2bit_internal_weighted_sse,
};

/// 2-bit-specific test helpers.  Bit-width-agnostic helpers (`pack_codes`,
/// `sample_normal_vec`, `encode_to_nearest_centroid`) live in
/// [`super::super::shared`].
#[cfg(test)]
pub(super) mod shared {
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

/// Accuracy / precision tests for `Query2bitSimd` and `score_2bit_internal`.
/// Per-arch SIMD parity tests live in the `arm` / `x64` submodules.
#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::shared::{encode_to_nearest_centroid, pack_codes, sample_normal_vec};
    use super::{CODEBOOK_ABS_MAX, Query2bitSimd, score_2bit_internal_scalar};
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

    #[rstest::rstest]
    #[case::full_chunks(256)]
    #[case::small_tail(20)]
    #[case::max_tail(28)]
    #[case::odd_chunks_only(48)]
    #[case::odd_chunks_plus_tail(60)]
    #[case::matryoshka(268)]
    #[case::large_with_tail(2044)]
    fn test_dotprod_matches_float(#[case] dim: usize) {
        let mut rng = StdRng::seed_from_u64(42);
        let n_trials = 64;

        let centroids = TQBits::Bits2.get_centroids();

        for _ in 0..n_trials {
            let query = sample_normal_vec(&mut rng, dim);
            let v_raw = sample_normal_vec(&mut rng, dim);
            let indices = encode_to_nearest_centroid(centroids, &v_raw);
            let v_pq: Vec<f32> = indices.iter().map(|&k| centroids[k as usize]).collect();

            let pq_dot: f32 = query.iter().zip(v_pq.iter()).map(|(a, b)| a * b).sum();
            let simd_dot = Query2bitSimd::new(&query).dotprod(&pack_codes(&indices, 2));

            // SIMD-added error scales like √dim · σ_q · ε_c.  Scale tolerance
            // with √dim so large-dim trials don't falsely fail on 3σ tails.
            let tol = (0.5_f32).max(0.03 * (dim as f32).sqrt());
            assert!(
                (pq_dot - simd_dot).abs() < tol,
                "dim={dim}: simd_dot {simd_dot} too far from ideal PQ dot {pq_dot} (tol={tol})",
            );
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

    /// Saturation-safety at 64K dims: every centroid index at max (`3`),
    /// every weight at `i16::MAX`. Same worst-case shape as the 4-bit
    /// counterpart — `weights[i]` is `i16` (the storage type is the
    /// SIMD-load contract). The i64 accumulator must hold the load.
    ///
    /// Per-coord product:
    ///   `c_signed² × weight = 127² × 32 767 ≈ 5.28e8` (fits i32 with ~4× headroom).
    /// Total over 65 536 coords: `≈ 3.46e13` — fits i64.
    #[test]
    fn test_score_2bit_internal_weighted_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![3; dim]; // max-magnitude centroid
        let vec_a = pack_codes(&indices, 2);
        let vec_b = pack_codes(&indices, 2);
        let max_weight: i16 = i16::MAX;
        let weights: Vec<i16> = vec![max_weight; dim];

        let raw_int = super::score_2bit_internal_weighted(&vec_a, &vec_b, &weights);

        let c_max_signed = super::codebook_signed_i64(3);
        let per_coord = c_max_signed * c_max_signed * i64::from(max_weight);
        let expected = per_coord * dim as i64;
        assert_eq!(
            raw_int, expected,
            "i64 sum overflow / mismatch at dim={dim} (per-coord={per_coord}, expected={expected}, got={raw_int})",
        );
    }

    /// `score_2bit_internal_weighted` should recover `Σ c[a[j]] · c[b[j]] ·
    /// D'_j²` after dividing by `weight_scale · CODEBOOK_SCALE²`.
    /// Weights are quantized into the `i16` storage form the SIMD kernels
    /// consume directly.
    #[test]
    fn test_score_2bit_internal_weighted_matches_reference() {
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
                .map(|((&a, &b), &w)| {
                    f64::from(centroids[a as usize])
                        * f64::from(centroids[b as usize])
                        * f64::from(w)
                })
                .sum();
            let raw_int = super::score_2bit_internal_weighted(
                &pack_codes(&idx_a, 2),
                &pack_codes(&idx_b, 2),
                &weights_i16,
            );
            let score = raw_int as f32 / (weight_scale * super::CODEBOOK_SCALE_SQ);

            assert!(
                (truth as f32 - score).abs() < 16.0,
                "weighted score {score} too far from reference {truth}",
            );
        }
    }
}
