//! 4-bit product-quantization scoring.
//!
//! # Encoding strategy (arch-specific)
//!
//! Both architectures share the same storage layout for `query_data`
//! (`Vec<[[i8; 16]; 2]>`) and the same `dot_raw` → float reconstruction formula
//! (`postprocess_scale · (dot_raw − bias_correction)`), but the numerical
//! encoding of the codebook and query differs to squeeze the most precision out
//! of each SIMD instruction set:
//!
//! * **aarch64** — `vmull_s8` and `sdot` are true `i8 × i8 → i16/i32` signed
//!   multiplies, so we can store the full `i8 ∈ [−127, 127]` codebook directly
//!   with no offset.  Query halves are full `i8 ∈ [−128, 127]` combined as
//!   `q_signed = 256 · high + low`, giving ~15.9-bit query precision.  The
//!   reconstruction needs no bias correction.
//!
//! * **x86_64** — `_mm_maddubs_epi16` and `VPDPBUSD` consume one `u8` and one
//!   `i8` operand.  To carry full 8-bit codebook magnitude we feed it unsigned
//!   `c_u ∈ [0, 255]` (shifted from signed by `+128`) and keep the query halves
//!   narrower to stay under i16 pair-sum saturation
//!   (`c_u ≤ 255, q ∈ [−64, 63]` → `|pair| ≤ 2·255·64 = 32 640 < 32 767` ✓).
//!   Query halves are 7-bit signed combined as `q_signed = 128 · high + low`,
//!   giving ~13.9-bit query precision.  The shift contributes a per-query
//!   bias `128 · Σ q_signed` that we subtract once in `dotprod`.
//!
//! Both codebooks derive from `CENTROIDS_4BIT` (Lloyd-Max on N(0,1)); see
//! `test_codebook_matches_lloyd_max` for the consistency check.

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

/// Encoded query for asymmetric 4-bit PQ scoring.
///
/// # Encoding
/// The f32 query is quantized to signed integers
/// `q_signed ∈ [−QUERY_ABS_MAX, QUERY_ABS_MAX]` and split into two i8 halves
/// combined as `q_signed = QUERY_HIGH_COEF · high + low` — see the module-level
/// docs for the per-arch values of `QUERY_HIGH_COEF` (256 on aarch64, 128 on
/// x86_64) and the reasoning behind the split.  Storage is 16-dim chunks of
/// `[low, high]` plus a scalar-handled tail of up to 14 dims (`dim % 2 == 0`).
/// A matryoshka-trimmed model at any even dim fits without re-encoding.
///
/// # Scoring
/// `dotprod_raw = Σ_j q_signed[j] · c_raw[v[j]]`.  The float result is
/// `postprocess_scale · (dot_raw − bias_correction)`, where `bias_correction`
/// absorbs the `+OFFSET` shift in the x86 unsigned-codebook layout and is 0
/// on aarch64 (signed codebook, no shift).
pub struct Query4bitSimd {
    /// Full 16-dim chunks of the query, each stored as `[low, high]` i8 halves.
    query_data: Vec<[[i8; 16]; 2]>,
    /// Trailing dims that don't fill a 16-dim chunk — up to 14 (since
    /// `dim % 2 == 0`).  Arrays are sized to a full 16-lane SIMD register
    /// (zero-padded beyond `tail_dims`) so arch backends can feed them
    /// straight into one extra `maddubs` / `vmull_s8` iteration on top of a
    /// zero-padded data chunk, replacing the scalar 14-iteration loop.
    tail_low: [i8; 16],
    tail_high: [i8; 16],
    /// Number of meaningful entries in the tail arrays (`0..=14`, always even).
    tail_dims: u8,
    /// `1 / (q_scale · c_scale)` — prefactor from integer to float dot product.
    postprocess_scale: f32,
    /// `CODEBOOK_OFFSET · Σ q_signed[j]` — sums over **all** dims (full
    /// chunks + tail).  Subtracted from `dot_raw` to recover the true signed
    /// dot product.  `0` on aarch64, where the codebook is already signed.
    bias_correction: i64,
}

impl Query4bitSimd {
    /// Query dim must be a multiple of 2 (the 4-bit packing width: two codes
    /// per byte).  Any such dim is accepted — dims that don't fill a full
    /// 16-dim chunk produce up to a 14-dim tail handled scalar-wise in every
    /// SIMD path.  This makes `Query4bitSimd` Matryoshka-friendly: a model
    /// trimmed to 640 / 768 / 896 / 1024 dims all work with the same storage.
    pub fn new(data: &[f32]) -> Self {
        assert!(
            data.len().is_multiple_of(2),
            "Query4bitSimd requires query dim to be a multiple of 2 (got {})",
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

        // Balanced signed split, same math for full chunks and tail.
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
        debug_assert!(tail_dims < 16 && tail_dims.is_multiple_of(2));

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

        let mut tail_low = [0_i8; 16];
        let mut tail_high = [0_i8; 16];
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

    /// Number of `vector` bytes the encoded query expects: 8 bytes per full
    /// 16-dim chunk plus the packed tail (two 4-bit codes per byte).
    #[inline]
    pub(super) fn expected_vector_bytes(&self) -> usize {
        self.query_data.len() * 8 + (self.tail_dims as usize).div_ceil(2)
    }

    /// Score the encoded query against a 4-bit PQ-encoded `vector`
    /// (two centroid indices per byte; low nibble = even lane, high nibble
    /// = odd lane).  `vector.len()` must equal `ceil(dim / 2)` — full chunks
    /// first, then the tail bytes covering `tail_dims`.
    ///
    /// Dispatches at runtime to the best SIMD backend available on the host
    /// CPU (AVX-512 VNNI → AVX2 → SSE → NEON + SDOT → NEON → scalar).
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

    /// Compute `Σ q_signed[j] · c_raw[v[j]]` over all dims — both the
    /// full-chunk section (SIMD-friendly 16 lanes per chunk) and the tail.
    /// Returns `acc_low + QUERY_HIGH_COEF · acc_high`.
    ///
    /// `vector` is PQ-encoded with two 4-bit codebook indices packed per byte:
    /// the low nibble is the index for the even lane (j = 2k), the high nibble
    /// for the odd lane (j = 2k + 1).
    pub fn dotprod_raw(&self, vector: &[u8]) -> i64 {
        assert_eq!(
            vector.len(),
            self.expected_vector_bytes(),
            "Query4bitSimd::dotprod_raw: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.expected_vector_bytes(),
        );
        let mut acc_low: i64 = 0;
        let mut acc_high: i64 = 0;
        for (chunk_idx, [low, high]) in self.query_data.iter().enumerate() {
            let v = &vector[chunk_idx * 8..(chunk_idx + 1) * 8];
            for i in 0..16 {
                let byte = v[i / 2];
                let idx = if i & 1 == 0 { byte & 0x0F } else { byte >> 4 };
                let c = codebook_value_i64(idx);
                acc_low += i64::from(low[i]) * c;
                acc_high += i64::from(high[i]) * c;
            }
        }
        acc_low + QUERY_HIGH_COEF * acc_high + self.dotprod_raw_tail(vector)
    }

    /// Scalar contribution from the `tail_dims` trailing query entries.
    /// Used by the scalar [`Self::dotprod_raw`] reference.  SIMD backends
    /// have their own tail helpers that feed one zero-padded chunk into the
    /// same kernel used for full chunks.
    #[inline]
    pub(super) fn dotprod_raw_tail(&self, vector: &[u8]) -> i64 {
        if self.tail_dims == 0 {
            return 0;
        }
        let tail_byte_start = self.query_data.len() * 8;
        let mut acc_low: i64 = 0;
        let mut acc_high: i64 = 0;
        for i in 0..self.tail_dims as usize {
            let byte = vector[tail_byte_start + i / 2];
            let idx = if i & 1 == 0 { byte & 0x0F } else { byte >> 4 };
            let c = codebook_value_i64(idx);
            acc_low += i64::from(self.tail_low[i]) * c;
            acc_high += i64::from(self.tail_high[i]) * c;
        }
        acc_low + QUERY_HIGH_COEF * acc_high
    }

    /// Prepare an 8-byte zero-padded scratch buffer with the packed tail data,
    /// suitable for feeding into a single SSE / NEON chunk kernel.
    /// Returns `None` when there is no tail.  The returned buffer has the
    /// same nibble layout as any full chunk in `vector` — low nibble = even
    /// lane, high nibble = odd lane.  Unused lanes are zero: with
    /// `tail_low[tail_dims..] = tail_high[tail_dims..] = 0` they contribute
    /// nothing to `maddubs` / `vmull` products.
    #[inline]
    pub(super) fn tail_chunk_scratch(&self, vector: &[u8]) -> Option<[u8; 8]> {
        if self.tail_dims == 0 {
            return None;
        }
        let tail_byte_start = self.query_data.len() * 8;
        let tail_bytes = (self.tail_dims as usize).div_ceil(2);
        let mut buf = [0u8; 8];
        buf[..tail_bytes].copy_from_slice(&vector[tail_byte_start..tail_byte_start + tail_bytes]);
        Some(buf)
    }
}

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

    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512bw")
            && std::is_x86_feature_detected!("avx512vnni")
        {
            return unsafe { x64::score_4bit_internal_avx512_vnni(a, b) };
        }
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { x64::score_4bit_internal_avx2(a, b) };
        }
        if std::is_x86_feature_detected!("sse4.1") && std::is_x86_feature_detected!("ssse3") {
            return unsafe { x64::score_4bit_internal_sse(a, b) };
        }
    }
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        if std::arch::is_aarch64_feature_detected!("dotprod") {
            return unsafe { arm::score_4bit_internal_neon_sdot(a, b) };
        }
        return unsafe { arm::score_4bit_internal_neon(a, b) };
    }
    #[allow(unreachable_code)]
    score_4bit_internal_scalar(a, b)
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
pub(super) fn score_4bit_internal_integer(a: &[u8], b: &[u8]) -> i64 {
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

    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { x64::score_4bit_internal_weighted_avx2(a, b, weights) };
        }
        if std::is_x86_feature_detected!("sse4.1") && std::is_x86_feature_detected!("ssse3") {
            return unsafe { x64::score_4bit_internal_weighted_sse(a, b, weights) };
        }
    }
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        return unsafe { arm::score_4bit_internal_weighted_neon(a, b, weights) };
    }
    #[allow(unreachable_code)]
    score_4bit_internal_weighted_scalar(a, b, weights)
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
pub(super) mod shared {
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

    use super::super::shared::{encode_to_nearest_centroid, pack_codes, sample_normal_vec};
    use super::{CODEBOOK_ABS_MAX, Query4bitSimd};
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

    /// Reconstruction accuracy on realistic PQ inputs: query ∼ N(0,1), vector
    /// drawn from N(0,1) then mapped to its nearest centroid.  We compare
    /// `simd.dotprod()` against the "ideal" PQ dot (sum of `q[j] · c[v[j]]`
    /// with float-precision centroid lookup) — the error our SIMD path adds
    /// over a hypothetical perfect-precision PQ should be tiny.
    ///
    /// Parameterized over matryoshka-style corner-case dims to exercise the
    /// tail-handling logic end-to-end (not just bit-exact parity).
    #[rstest::rstest]
    #[case::full_chunks(256)]
    #[case::small_tail(18)]
    #[case::max_tail(30)]
    #[case::odd_chunks_only(48)]
    #[case::odd_chunks_plus_tail(62)]
    #[case::matryoshka(270)]
    #[case::large_with_tail(2046)]
    fn test_dotprod_matches_float(#[case] dim: usize) {
        let mut rng = StdRng::seed_from_u64(42);
        let n_trials = 64;

        let centroids = TQBits::Bits4.get_centroids();

        for _ in 0..n_trials {
            let query = sample_normal_vec(&mut rng, dim);
            let v_raw = sample_normal_vec(&mut rng, dim);
            let indices = encode_to_nearest_centroid(centroids, &v_raw);
            let v_pq: Vec<f32> = indices.iter().map(|&k| centroids[k as usize]).collect();

            let pq_dot: f32 = query.iter().zip(v_pq.iter()).map(|(a, b)| a * b).sum();
            let simd_dot = Query4bitSimd::new(&query).dotprod(&pack_codes(&indices, 4));

            // Error scales roughly like √dim · σ_q · ε_c.  Allow a tolerance
            // that is comfortably above the 3σ tail for dim up to ~2K.
            let tol = (0.5_f32).max(0.03 * (dim as f32).sqrt());
            assert!(
                (pq_dot - simd_dot).abs() < tol,
                "dim={dim}: simd_dot {simd_dot} too far from ideal PQ dot {pq_dot} (tol={tol})",
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

        let centroids = TQBits::Bits4.get_centroids();

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
            let simd_dot = f64::from(Query4bitSimd::new(&query).dotprod(&pack_codes(&indices, 4)));

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
