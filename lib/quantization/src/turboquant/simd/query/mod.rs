//! Asymmetric scoring — an f32 query against vectors of packed codebook
//! indices — generic over the packing width.
//!
//! [`QuerySimd<PLANES>`] scores against vectors whose codes are packed
//! `PLANES` per byte: two 4-bit codes, four 2-bit codes or eight 1-bit
//! codes (`PLANES = 8 / bits`).  The widths differ only in their integer
//! codebook (see [`Encoding`]); the query layout, the scalar reference and
//! every SIMD kernel are shared.
//!
//! # Query layout
//!
//! The query is quantized to signed integers and split into two i8 halves,
//! `q_signed = QUERY_HIGH_COEF · high + low`, which the kernels multiply
//! against the codebook values with `u8 × i8` (x86_64) or `i8 × i8`
//! (aarch64) instructions.  The halves are stored as [`QueryPlanes`]: one
//! plane per code position within a packed byte, so a wide load of raw data
//! bytes lines up with the query without unpacking into dim order.
//!
//! # Integer encoding
//!
//! Both architectures share the reconstruction
//! `postprocess_scale · (dot_raw − bias_correction)`, but encode the
//! codebook and the query differently to get the most precision out of
//! their instruction sets:
//!
//! * **aarch64** — `vmull_s8` and `SDOT` are true `i8 × i8 → i16/i32`
//!   signed multiplies, so the codebook is stored as signed `i8 ∈ [−127,
//!   127]` with no offset.  Query halves are full `i8` combined with
//!   `K = 256` (~15.9-bit query precision), and there is no bias.
//!
//! * **x86_64** — `maddubs` / `VPDPBUSD` consume one `u8` and one `i8`
//!   operand.  The codebook is stored unsigned (`c_u = c_signed + offset`),
//!   and the query halves are kept narrow enough that the `maddubs` pair
//!   sum stays inside i16: with the full `u8` codebooks of the 2- and 4-bit
//!   widths (`c_u ≤ 255`) the halves are 7-bit (`K = 128`, `|pair| ≤
//!   2·255·64 = 32 640`); the 1-bit width only needs `c_u ∈ {0, 128}` and
//!   keeps full i8 halves.  The offset contributes a per-query bias
//!   `offset · Σ q_signed`, subtracted once per vector.

use super::{SimdBackend, query1bit, query2bit, query4bit};

/// Codebook entry in the storage form the kernels multiply: signed on
/// aarch64 (`vmull_s8` / `SDOT`), unsigned everywhere else (`maddubs` /
/// `VPDPBUSD` take a `u8` operand; the scalar fallback mirrors x86_64).
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub(crate) type Code = i8;
#[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
pub(crate) type Code = u8;

/// Integer encoding of one packing width: the codebook the kernels multiply
/// and the query quantization that keeps every intermediate inside its lane.
#[derive(Clone, Copy)]
pub(crate) struct Encoding {
    /// Codebook value per code in the arch-native storage form, padded to
    /// a full 16-entry shuffle table; entries past the width's code range
    /// are never addressed.
    pub codebook: [Code; 16],
    /// `c_signed = codebook[k] − offset`; `0` where the codebook is stored
    /// signed.
    pub offset: i64,
    /// Integer units per centroid unit: `c_signed ≈ c_float · scale`.
    pub scale: f32,
    /// `q_signed = query_high_coef · high + low` with i8 halves.
    pub query_high_coef: i64,
    /// Largest `|q_signed|` the query is scaled to — the symmetric range
    /// both halves cover.
    pub query_abs_max: f32,
}

/// Pad a width's codebook to the 16-entry shuffle table [`Encoding`] holds.
pub(crate) const fn pad_codebook<const N: usize>(codebook: [Code; N]) -> [Code; 16] {
    let mut padded = [0; 16];
    let mut k = 0;
    while k < N {
        padded[k] = codebook[k];
        k += 1;
    }
    padded
}

/// The encoding of the width packing `planes` codes per byte.
const fn encoding(planes: usize) -> Encoding {
    match planes {
        2 => query4bit::ENCODING,
        4 => query2bit::ENCODING,
        8 => query1bit::ENCODING,
        _ => panic!("QuerySimd: PLANES must be 2, 4 or 8"),
    }
}

/// Widest block of packed data bytes any kernel consumes at once (AVX-512:
/// 64 bytes).  Every query plane is padded to a multiple of it.
const PLANE_BLOCK: usize = 64;

/// Query halves regrouped by code position — the layout the SIMD kernels
/// consume.
///
/// A packed data byte `j` holds the codes of dims `PLANES · j + k` for
/// `k ∈ 0..PLANES`, code `k` in bits `[k · bits, (k + 1) · bits)`.  Plane
/// `k` lines the query halves up with exactly that order: entry `j` of
/// `low[k]` / `high[k]` is the half of query dim `PLANES · j + k`.  So a
/// kernel shifts the raw data bytes by `k · bits`, masks off one code per
/// byte and multiplies against plane `k` — no unpacking into dim order.
///
/// Each plane is zero-padded to a multiple of [`PLANE_BLOCK`] bytes, so a
/// kernel can always load a whole block on the query side; whatever the
/// data lanes past the vector's end hold, they multiply against zeros.
pub(crate) struct QueryPlanes<const PLANES: usize> {
    low: [Vec<i8>; PLANES],
    high: [Vec<i8>; PLANES],
}

impl<const PLANES: usize> QueryPlanes<PLANES> {
    /// Build the planes from the encoded query, one `(low, high)` pair per
    /// dim; the number of dims must be a multiple of `PLANES`.
    fn new(encoded: impl ExactSizeIterator<Item = (i8, i8)>) -> Self {
        let dim = encoded.len();
        debug_assert!(dim.is_multiple_of(PLANES));
        let len = (dim / PLANES).next_multiple_of(PLANE_BLOCK);
        let mut low: [Vec<i8>; PLANES] = std::array::from_fn(|_| vec![0; len]);
        let mut high: [Vec<i8>; PLANES] = std::array::from_fn(|_| vec![0; len]);
        for (i, (l, h)) in encoded.enumerate() {
            let (j, k) = (i / PLANES, i % PLANES);
            low[k][j] = l;
            high[k][j] = h;
        }
        Self { low, high }
    }
}

/// Encoded query for asymmetric scoring against vectors packing `PLANES`
/// codes per byte.
///
/// The f32 query is quantized to `q_signed ∈ [−query_abs_max,
/// query_abs_max]` and split into two i8 halves stored as [`QueryPlanes`]
/// (see the module docs for the per-arch encoding).  Any dim that is a
/// multiple of `PLANES` works, so a matryoshka-trimmed model fits without
/// re-encoding.
///
/// `dotprod` computes `dot_raw = Σ_j q_signed[j] · codebook[v[j]]` and
/// returns `postprocess_scale · (dot_raw − bias_correction)`.
pub struct QuerySimd<const PLANES: usize> {
    /// Query halves, regrouped by code position.
    planes: QueryPlanes<PLANES>,
    /// Packed bytes per encoded vector: `dim / PLANES`.
    vector_bytes: usize,
    /// `1 / (q_scale · c_scale)` — prefactor from integer to float dot
    /// product.
    postprocess_scale: f32,
    /// `offset · Σ q_signed[j]` over all dims, subtracted from `dot_raw` to
    /// recover the signed dot product; `0` where the codebook is signed.
    bias_correction: i64,
    /// SIMD backend resolved once at construction, so scoring doesn't re-run
    /// CPU feature detection for every vector.
    backend: SimdBackend,
}

impl<const PLANES: usize> QuerySimd<PLANES> {
    const ENCODING: Encoding = encoding(PLANES);

    /// Bits per code.
    const BITS: usize = 8 / PLANES;

    /// Encode `data`; its length must be a multiple of `PLANES`.
    pub fn new(data: &[f32]) -> Self {
        assert!(
            data.len().is_multiple_of(PLANES),
            "QuerySimd<{PLANES}> requires query dim to be a multiple of {PLANES} (got {})",
            data.len(),
        );

        let encoding = Self::ENCODING;
        let q_abs_max = data
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max)
            .max(f32::EPSILON);
        let q_scale = encoding.query_abs_max / q_abs_max;

        let k = encoding.query_high_coef as i32;
        let half_k = k / 2;
        let clamp_hi = encoding.query_abs_max;
        let clamp_lo = -encoding.query_abs_max;

        // Balanced signed split: `q_signed = k · high + low`, `low ∈ [−k/2, k/2)`.
        let mut sum_q_signed: i64 = 0;
        let planes = QueryPlanes::new(data.iter().map(|&value| {
            let q_signed = (value * q_scale).round().clamp(clamp_lo, clamp_hi) as i32;
            sum_q_signed += i64::from(q_signed);
            let l_mod = q_signed.rem_euclid(k);
            let l = if l_mod >= half_k { l_mod - k } else { l_mod } as i8;
            let h = ((q_signed - i32::from(l)) / k) as i8;
            (l, h)
        }));

        Self {
            planes,
            vector_bytes: data.len() / PLANES,
            postprocess_scale: 1.0 / (q_scale * encoding.scale),
            bias_correction: encoding.offset * sum_q_signed,
            backend: SimdBackend::detect(),
        }
    }

    /// Packed bytes per encoded vector: `dim / PLANES`.
    #[inline]
    pub fn vector_bytes(&self) -> usize {
        self.vector_bytes
    }

    /// Score the encoded query against a `vector` of packed codes (`PLANES`
    /// codes per byte, code `k` of byte `j` in bits `[k · bits, (k + 1) ·
    /// bits)`).  `vector.len()` must equal [`Self::vector_bytes`].
    ///
    /// Dispatches to the SIMD backend resolved at construction (see
    /// [`SimdBackend`]).
    pub fn dotprod(&self, vector: &[u8]) -> f32 {
        self.postprocess(self.dotprod_raw_best(vector))
    }

    /// Float reconstruction of a raw integer dot product.
    #[inline]
    fn postprocess(&self, dot_raw: i64) -> f32 {
        self.postprocess_scale * (dot_raw - self.bias_correction) as f32
    }

    #[inline]
    fn dotprod_raw_best(&self, vector: &[u8]) -> i64 {
        match self.backend {
            #[cfg(target_arch = "x86_64")]
            SimdBackend::Avx512Vnni => unsafe { self.dotprod_raw_avx512_vnni(vector) },
            #[cfg(target_arch = "x86_64")]
            SimdBackend::Avx2 => unsafe { self.dotprod_raw_avx2(vector) },
            #[cfg(target_arch = "x86_64")]
            SimdBackend::Sse => self.dotprod_raw(vector),
            #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
            SimdBackend::NeonSdot => unsafe { self.dotprod_raw_neon_sdot(vector) },
            #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
            SimdBackend::Neon => self.dotprod_raw(vector),
            SimdBackend::Scalar => self.dotprod_raw(vector),
        }
    }

    /// Scalar reference: `Σ_j q_signed[j] · codebook[v[j]]` over all dims,
    /// computed as `low + query_high_coef · high` with i64 accumulators for
    /// each query half — saturation-free by construction, which is what the
    /// SIMD parity tests check against.
    pub fn dotprod_raw(&self, vector: &[u8]) -> i64 {
        assert_eq!(
            vector.len(),
            self.vector_bytes,
            "QuerySimd<{PLANES}>::dotprod_raw: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.vector_bytes,
        );
        let encoding = Self::ENCODING;
        let mask = (1u8 << Self::BITS) - 1;
        let mut low: i64 = 0;
        let mut high: i64 = 0;
        for (j, &byte) in vector.iter().enumerate() {
            for k in 0..PLANES {
                let code = (byte >> (k * Self::BITS)) & mask;
                let c = i64::from(encoding.codebook[code as usize]);
                low += i64::from(self.planes.low[k][j]) * c;
                high += i64::from(self.planes.high[k][j]) * c;
            }
        }
        low + encoding.query_high_coef * high
    }
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod arm;

#[cfg(target_arch = "x86_64")]
mod x64;

/// Test helpers shared by the kernel parity tests of every backend.
#[cfg(test)]
pub(crate) mod shared {
    use rand::prelude::StdRng;

    use super::super::shared::{random_bytes, sample_normal_vec};
    use super::QuerySimd;

    /// Corner-case vector lengths (packed bytes) for the block loops of
    /// every backend — blocks of 16 (SSE, NEON), 32 (AVX2) and 64 (AVX-512)
    /// bytes, with the last partial block padded or masked:
    ///   • `16, 64, 128, 512, 1024` — whole blocks only, at every width.
    ///   • `1` — a single data byte.
    ///   • `15, 31, 63` — one byte short of a 16-, 32- and 64-byte block;
    ///     `65` — one byte past a 64-byte block.
    ///   • `8, 9, 23, 24, 135, 513, 1023` — assorted partial blocks,
    ///     including a realistic matryoshka slice.
    pub const PARITY_BYTES: &[usize] = &[
        1, 8, 9, 15, 16, 23, 24, 31, 63, 64, 65, 128, 135, 512, 513, 1023, 1024,
    ];

    /// The dims of [`PARITY_BYTES`] at a width packing `PLANES` codes per
    /// byte.
    pub fn parity_dims<const PLANES: usize>() -> impl Iterator<Item = usize> {
        PARITY_BYTES.iter().map(move |&bytes| bytes * PLANES)
    }

    /// Parity-test inputs: a query ~ N(0, 1) and a vector of uniformly
    /// random codes (random bytes are random packed codes at every width).
    pub fn random_inputs<const PLANES: usize>(
        rng: &mut StdRng,
        dim: usize,
    ) -> (QuerySimd<PLANES>, Vec<u8>) {
        let query = sample_normal_vec(rng, dim);
        (QuerySimd::new(&query), random_bytes(rng, dim / PLANES))
    }
}

/// Accuracy tests of the public `QuerySimd` API against float ground truth,
/// at every width.  Per-backend parity tests (SIMD kernel vs the scalar
/// reference `dotprod_raw`) live in the `arm` / `x64` submodules.
#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::shared::{encode_to_nearest_centroid, pack_codes, sample_normal_vec};
    use super::QuerySimd;
    use crate::turboquant::TQBits;

    /// The width packing `PLANES` codes per byte.
    fn bits<const PLANES: usize>() -> TQBits {
        match PLANES {
            2 => TQBits::Bits4,
            4 => TQBits::Bits2,
            8 => TQBits::Bits1,
            _ => unreachable!(),
        }
    }

    /// Reconstruction accuracy on realistic PQ inputs: query ∼ N(0,1), vector
    /// drawn from N(0,1) then mapped to its nearest centroid.  `dotprod` is
    /// compared against the ideal PQ dot (`Σ q[j] · c[v[j]]` with
    /// float-precision centroid lookup) — the error the integer encoding
    /// adds over a hypothetical perfect-precision PQ should be tiny.
    ///
    /// Parameterized over matryoshka-style corner-case dims to exercise the
    /// tail handling end-to-end (not just bit-exact parity).
    fn dotprod_matches_float<const PLANES: usize>(dim: usize) {
        let mut rng = StdRng::seed_from_u64(42);
        let n_trials = 64;
        let bits = bits::<PLANES>();
        let centroids = bits.get_centroids();

        for _ in 0..n_trials {
            let query = sample_normal_vec(&mut rng, dim);
            let v_raw = sample_normal_vec(&mut rng, dim);
            let indices = encode_to_nearest_centroid(centroids, &v_raw);
            let v_pq: Vec<f32> = indices.iter().map(|&k| centroids[k as usize]).collect();

            let pq_dot: f32 = query.iter().zip(v_pq.iter()).map(|(a, b)| a * b).sum();
            let simd_dot =
                QuerySimd::<PLANES>::new(&query).dotprod(&pack_codes(&indices, bits.bit_size()));

            // Error scales roughly like √dim · σ_q · ε_c.  Allow a tolerance
            // that is comfortably above the 3σ tail for dim up to ~2K.
            let tol = (0.5_f32).max(0.03 * (dim as f32).sqrt());
            assert!(
                (pq_dot - simd_dot).abs() < tol,
                "PLANES={PLANES} dim={dim}: simd_dot {simd_dot} too far from ideal PQ dot \
                 {pq_dot} (tol={tol})",
            );
        }
    }

    /// Corner-case packed lengths: whole blocks, small and maximal tails,
    /// odd block counts and a realistic matryoshka slice.
    #[rstest::rstest]
    #[case::full_blocks(128)]
    #[case::small_tail(9)]
    #[case::max_tail(15)]
    #[case::odd_blocks_only(24)]
    #[case::odd_blocks_plus_tail(31)]
    #[case::matryoshka(135)]
    #[case::large_with_tail(1023)]
    fn test_dotprod_matches_float(#[case] bytes: usize) {
        dotprod_matches_float::<2>(bytes * 2);
        dotprod_matches_float::<4>(bytes * 4);
        dotprod_matches_float::<8>(bytes * 8);
    }

    /// Quantitative proof that the integer encoding is negligible next to
    /// PQ centroid snapping: RMS error added by the encoding is at least 5×
    /// smaller than the RMS error PQ itself introduces.  If this invariant
    /// ever flips, something in the quantization pipeline lost precision.
    fn simd_noise_below_pq_noise<const PLANES: usize>() {
        let mut rng = StdRng::seed_from_u64(123);
        let dim = 256;
        let n_trials = 256;
        let bits = bits::<PLANES>();
        let centroids = bits.get_centroids();

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
            let simd_dot = f64::from(
                QuerySimd::<PLANES>::new(&query).dotprod(&pack_codes(&indices, bits.bit_size())),
            );

            sq_pq_noise += (pq_dot - true_dot).powi(2);
            sq_simd_noise += (simd_dot - pq_dot).powi(2);
        }

        let rms_pq_noise = (sq_pq_noise / f64::from(n_trials)).sqrt();
        let rms_simd_noise = (sq_simd_noise / f64::from(n_trials)).sqrt();

        // Print for easy comparison across encoding variants.
        eprintln!(
            "NOISE at PLANES={PLANES} dim={dim}: pq_rms={rms_pq_noise:.4} \
             simd_rms={rms_simd_noise:.4} ratio={:.2}×",
            rms_pq_noise / rms_simd_noise,
        );

        assert!(
            rms_simd_noise * 5.0 < rms_pq_noise,
            "PLANES={PLANES}: SIMD noise RMS {rms_simd_noise:.4} should be << PQ noise RMS \
             {rms_pq_noise:.4} (ratio {:.2}×)",
            rms_pq_noise / rms_simd_noise,
        );
    }

    #[test]
    fn test_simd_noise_below_pq_noise() {
        simd_noise_below_pq_noise::<2>();
        simd_noise_below_pq_noise::<4>();
        simd_noise_below_pq_noise::<8>();
    }
}
