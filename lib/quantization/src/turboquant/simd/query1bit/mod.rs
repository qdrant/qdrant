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
//! * [`Query1bitSimd<BITS>`] — asymmetric scoring of an original query against
//!   packed data.  The query is quantized to `BITS`-bit signed integers
//!   (default 8), bit-plane-transposed into a block-interleaved layout, and
//!   scored per block as `Σ_b w_b · popcount(data AND plane_b)` where
//!   `w_b = 2^b` for b < BITS−1 and `−2^(BITS−1)` for the sign plane.

/// `|c|` for the 1-bit codebook — Lloyd-Max on N(0, 1) gives `sqrt(2/π)`.
/// Kept in sync with `CENTROIDS_1BIT` in `lloyd_max.rs` by a test below.
const CENTROID_ABS: f32 = 0.797_884_6;

/// `c²` — converts the signed-agreement count into a centroid dot product:
/// each lane contributes `(±c)·(±c) = ±c²`, summing to `c² · sign_sum`.
const CENTROID_SQ: f32 = CENTROID_ABS * CENTROID_ABS;

/// Block size for the bit-plane interleave: 16 packed bytes = 128 data dims.
/// Matches an XMM register; AVX2 processes 2 blocks per iter, AVX-512 takes 4.
const BLOCK_BYTES: usize = 16;

/// Encoded query for 1-bit PQ scoring with `BITS`-bit signed query
/// quantization (default 8).
///
/// # Encoding
/// Query is quantized to signed `BITS`-bit integers `q ∈ [−2^(BITS−1),
/// 2^(BITS−1) − 1]` in two's complement, then bit-plane-transposed and
/// block-interleaved: for each group of 128 dims (16 packed data bytes),
/// `BITS` × 16 consecutive bytes hold plane 0's 16 bytes, then plane 1's
/// 16 bytes, ..., then plane `BITS−1`'s 16 bytes.  This lets scoring read
/// data once per block (16-byte SIMD load) and consume `BITS` contiguous
/// plane chunks in a tight inner loop.
///
/// # Scoring
/// The signed dot product is
/// ```text
///   signed_dot = 2 · v_dot_q − Σ q_signed,
///   v_dot_q    = Σ_b w_b · popcount(v_block AND plane_b),
///   w_b        = 2^b for b ∈ 0..BITS−1,  w_{BITS−1} = −2^(BITS−1).
/// ```
/// The `Σ q_signed` correction is query-side (stored in `sum_q_signed`),
/// so no per-data-vector precomputation is needed.  The final float output
/// is `(c / q_scale) · signed_dot`, folded into `postprocess_scale`.
pub struct Query1bitSimd<const BITS: usize = 8> {
    /// Block-interleaved bit-planes.  Length is `total_blocks · BITS · 16`
    /// bytes, where `total_blocks = num_full_blocks + (tail_bytes > 0)`.
    /// When a partial tail block is present it sits at the end, with bit-plane
    /// bytes beyond `tail_bytes` zero-padded so `data AND plane = 0` for the
    /// padding lanes.  Same SIMD kernel handles full and partial blocks once
    /// the data side is zero-padded into a 16-byte stack buffer.
    planes: Vec<u8>,
    /// Number of **full** 128-dim blocks (excludes the partial tail).
    num_full_blocks: usize,
    /// Bytes in the trailing partial block — `0` if dim is block-aligned,
    /// `1..=15` otherwise (`tail_bytes · 8 = tail_dims`).
    tail_bytes: u8,
    /// `c / q_scale` — single scalar reconstruction factor.
    postprocess_scale: f32,
    /// `Σ q_signed` over **all** dims (full blocks + tail).
    sum_q_signed: i64,
}

impl<const BITS: usize> Query1bitSimd<BITS> {
    /// Query dim must be a multiple of 8 (the 1-bit packing width).
    /// Matryoshka-friendly — any such dim is accepted, with a tail of up to
    /// 15 packed bytes (120 dims) handled by the same SIMD kernel as full
    /// blocks via a zero-padded 16-byte scratch buffer.
    pub fn new(data: &[f32]) -> Self {
        assert!(
            (2..=16).contains(&BITS),
            "Query1bitSimd: BITS must be in [2, 16], got {BITS}",
        );
        assert!(
            data.len().is_multiple_of(8),
            "Query1bitSimd: dim must be a multiple of 8 (got {})",
            data.len(),
        );

        let q_abs_max_int = (1i64 << (BITS - 1)) - 1;
        let q_abs_max = data
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max)
            .max(f32::EPSILON);
        let q_scale = q_abs_max_int as f32 / q_abs_max;
        let clamp_hi = q_abs_max_int as f32;
        let clamp_lo = -clamp_hi;

        let encode =
            |value: f32| -> i64 { (value * q_scale).round().clamp(clamp_lo, clamp_hi) as i64 };

        let num_full_blocks = data.len() / (8 * BLOCK_BYTES);
        let full_dims = num_full_blocks * 8 * BLOCK_BYTES;
        let tail_dims = data.len() - full_dims;
        debug_assert!(tail_dims < 8 * BLOCK_BYTES && tail_dims.is_multiple_of(8));
        let tail_bytes = tail_dims / 8;
        let has_tail = tail_bytes > 0;
        let total_blocks = num_full_blocks + usize::from(has_tail);

        let mut planes = vec![0u8; total_blocks * BITS * BLOCK_BYTES];
        let mut sum_q_signed: i64 = 0;
        let bits_mask = (1u64 << BITS) - 1;

        // Helper to deposit a single dim's bits into the bit-plane layout.
        let mut deposit = |q: i64, block_idx: usize, byte_in_block: usize, bit_in_byte: usize| {
            let q_bits = (q as u64) & bits_mask;
            let block_base = block_idx * BITS * BLOCK_BYTES;
            for b in 0..BITS {
                let bit = ((q_bits >> b) & 1) as u8;
                planes[block_base + b * BLOCK_BYTES + byte_in_block] |= bit << bit_in_byte;
            }
        };

        for block_idx in 0..num_full_blocks {
            for byte_in_block in 0..BLOCK_BYTES {
                for bit_in_byte in 0..8 {
                    let dim = block_idx * 8 * BLOCK_BYTES + byte_in_block * 8 + bit_in_byte;
                    let q = encode(data[dim]);
                    sum_q_signed += q;
                    deposit(q, block_idx, byte_in_block, bit_in_byte);
                }
            }
        }

        // Partial tail block: same bit-plane encoding, padding bytes stay zero.
        if has_tail {
            for i in 0..tail_dims {
                let q = encode(data[full_dims + i]);
                sum_q_signed += q;
                deposit(q, num_full_blocks, i / 8, i % 8);
            }
        }

        Self {
            planes,
            num_full_blocks,
            tail_bytes: tail_bytes as u8,
            postprocess_scale: CENTROID_ABS / q_scale,
            sum_q_signed,
        }
    }

    /// Score the encoded query against a PQ-encoded `vector` (8 lanes / byte).
    /// `vector.len()` must equal the original query `dim / 8` — full blocks
    /// first, then up to 15 tail bytes.
    pub fn dotprod(&self, vector: &[u8]) -> f32 {
        let v_dot_q = self.dotprod_raw_best(vector);
        let signed_dot = 2 * v_dot_q - self.sum_q_signed;
        self.postprocess_scale * signed_dot as f32
    }

    #[inline]
    fn dotprod_raw_best(&self, vector: &[u8]) -> i64 {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512vl")
                && std::is_x86_feature_detected!("avx512vpopcntdq")
            {
                return unsafe { self.dotprod_raw_avx512_vpopcntdq(vector) };
            }
            if std::is_x86_feature_detected!("sse4.1") && std::is_x86_feature_detected!("ssse3") {
                return unsafe { self.dotprod_raw_sse(vector) };
            }
        }
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            return unsafe { self.dotprod_raw_neon(vector) };
        }
        #[allow(unreachable_code)]
        self.dotprod_raw(vector)
    }

    /// Integer kernel: `Σ v_j · q_signed[j]` across all dims — full blocks
    /// decoded from the bit-plane AND-popcount form, plus scalar tail.
    /// Reference implementation — SIMD variants in [`arm`] / [`x64`] must
    /// match this bit-exactly.
    pub fn dotprod_raw(&self, vector: &[u8]) -> i64 {
        let mut v_dot_q: i64 = 0;
        for block_idx in 0..self.num_full_blocks {
            let data_block = &vector[block_idx * BLOCK_BYTES..(block_idx + 1) * BLOCK_BYTES];
            v_dot_q += self.score_block_scalar(data_block, block_idx);
        }
        if let Some((buf, block_idx)) = self.tail_block_scratch(vector) {
            v_dot_q += self.score_block_scalar(&buf, block_idx);
        }
        v_dot_q
    }

    /// Score a single 16-byte data block against the plane entries for
    /// `block_idx`.  Scalar reference used by [`Self::dotprod_raw`] and by
    /// the shared tail-copy helpers in arch modules.
    #[inline]
    fn score_block_scalar(&self, data_block: &[u8], block_idx: usize) -> i64 {
        let plane_base = block_idx * BITS * BLOCK_BYTES;
        let mut v_dot_q: i64 = 0;
        for b in 0..BITS {
            let plane = &self.planes[plane_base + b * BLOCK_BYTES..][..BLOCK_BYTES];
            let mut c: u32 = 0;
            for i in 0..BLOCK_BYTES {
                c += (data_block[i] & plane[i]).count_ones();
            }
            let w_b: i64 = if b == BITS - 1 {
                -(1i64 << (BITS - 1))
            } else {
                1i64 << b
            };
            v_dot_q += w_b * i64::from(c);
        }
        v_dot_q
    }

    /// Copy `tail_bytes` bytes from `vector`'s trailing partial block into a
    /// zero-padded 16-byte scratch buffer.  Returns `None` if the query has
    /// no tail; otherwise returns `Some((buf, block_idx))` — the block index
    /// is where the SIMD backend should read plane bytes from.
    #[inline]
    pub(super) fn tail_block_scratch(&self, vector: &[u8]) -> Option<([u8; BLOCK_BYTES], usize)> {
        if self.tail_bytes == 0 {
            return None;
        }
        let mut buf = [0u8; BLOCK_BYTES];
        let tail_start = self.num_full_blocks * BLOCK_BYTES;
        let tail_len = self.tail_bytes as usize;
        buf[..tail_len].copy_from_slice(&vector[tail_start..tail_start + tail_len]);
        Some((buf, self.num_full_blocks))
    }

    /// Number of full 128-dim blocks the SIMD main-loop should iterate over.
    /// Exposed to arch backends so they don't depend on the internal field name.
    #[inline]
    pub(super) fn num_full_blocks(&self) -> usize {
        self.num_full_blocks
    }
}

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
pub(super) fn popcount_to_score(byte_len: usize, popcnt: u64) -> f32 {
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
pub(super) mod shared {
    /// Byte lengths used by parity tests.  Cover: below SSE width, a single
    /// SSE chunk, an AVX2 chunk, an AVX-512 chunk, two AVX-512 chunks + tail.
    pub const PARITY_BYTE_LENS: &[usize] = &[1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 257, 513];
}

/// Accuracy / precision tests for `score_1bit_internal` and `Query1bitSimd`.
/// Per-arch SIMD parity tests live in the `arm` / `x64` submodules.
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

    /// Round-trip test: `Query1bitSimd::dotprod(data)` should approximate
    /// the exact centroid-dot-product `Σ query_i · sign(v_i) · c`, modulo
    /// query quantization noise.  Uses BITS=8 (default) and BITS=12 to
    /// sanity-check that widening actually reduces error.
    ///
    /// Parameterized over matryoshka-style corner-case dims — every case
    /// exercises the tail path for BITS=8 and BITS=12 independently.
    #[rstest::rstest]
    #[case::full_blocks(1024)]
    #[case::tail_only(120)]
    #[case::block_plus_small_tail(136)]
    #[case::block_plus_max_tail(1144)] // 8 blocks + 120 tail dims
    #[case::matryoshka_640(640)]
    #[case::matryoshka_768(768)]
    #[case::matryoshka_896(896)]
    fn test_query_dotprod_matches_reference(#[case] dim: usize) {
        use rand_distr::{Distribution, StandardNormal};

        let byte_len = dim / 8;
        let mut rng = StdRng::seed_from_u64(1234);

        let query: Vec<f32> = (0..dim).map(|_| StandardNormal.sample(&mut rng)).collect();
        let data: Vec<u8> = random_bytes(&mut rng, byte_len);

        // Reference: decode data bits to ±c and compute plain float dot.
        let mut expected = 0.0_f32;
        for (i, &q_i) in query.iter().enumerate() {
            let bit = (data[i / 8] >> (i % 8)) & 1;
            let sign = if bit == 1 {
                CENTROID_ABS
            } else {
                -CENTROID_ABS
            };
            expected += q_i * sign;
        }

        let q8 = Query1bitSimd::<8>::new(&query);
        let got8 = q8.dotprod(&data);
        let q12 = Query1bitSimd::<12>::new(&query);
        let got12 = q12.dotprod(&data);

        // Compare against `|expected| + sqrt(dim)` so small dims (where
        // `expected` may be near zero by chance) don't artificially blow up
        // the relative error.
        let scale = expected.abs().max((dim as f32).sqrt());
        let rel_err_8 = (got8 - expected).abs() / scale;
        let rel_err_12 = (got12 - expected).abs() / scale;

        assert!(
            rel_err_8 < 2e-2,
            "dim={dim} BITS=8 rel_err={rel_err_8} (got {got8} vs {expected})"
        );
        assert!(
            rel_err_12 < 2e-3,
            "dim={dim} BITS=12 rel_err={rel_err_12} (got {got12} vs {expected})"
        );
        // Widening must shrink error (strictly, with overwhelming probability
        // — tightens to `<=` here to be robust against tiny-dim rng noise).
        assert!(
            rel_err_12 <= rel_err_8,
            "dim={dim} BITS=12 err {rel_err_12} should be ≤ BITS=8 err {rel_err_8}",
        );
    }
}
