use crate::DistanceType;
use crate::turboquant::encoding::TqVectorExtras;
use crate::turboquant::rotation::HadamardRotation;
use crate::turboquant::simd::{
    CODEBOOK_SCALE_SQ_2BIT, CODEBOOK_SCALE_SQ_4BIT, Query1bitSimd, Query2bitSimd, Query4bitSimd,
    score_1bit_internal, score_2bit_internal, score_2bit_internal_weighted, score_4bit_internal,
    score_4bit_internal_weighted,
};
use crate::turboquant::{
    EncodedQueryTQ, EncodedQueryTQData, ErrorCorrectionMetadata, Metadata, TQBits, TQMode,
};

/// Quantize vectors using TurboQuant.
pub struct TurboQuantizer {
    pub(super) rotation: HadamardRotation,
    pub(super) bits: TQBits,
    pub(super) mode: TQMode,
    pub(super) distance: DistanceType,
    pub(super) padded_dim: usize,
    pub(super) error_correction: Option<ErrorCorrection>,
}

/// TQ+ per-coordinate shift+scale: pulls each rotated, length-rescaled
/// coordinate onto the Lloyd-Max codebook's N(0, 1) grid before quantization.
///
/// `apply` maps `x → (x + shift) · scale = (x − M) / D'` where `M = -shift`
/// and `D' = 1 / scale`. `revert` is the exact inverse.
pub struct ErrorCorrection {
    pub shift: Vec<f32>,
    pub scale: Vec<f32>,
    /// Signed 16-bit quantized `D'_i² = 1 / scale_i²` per coordinate, used
    /// by the SIMD weighted-dot path of `score_symmetric_ec` for Bits2/Bits4.
    /// `D'²_f32 ≈ d_prime_sq_i16[i] / weight_scale`. Values are non-negative
    /// and clamped to `[0, i16::MAX − 1]` so SIMD kernels can use
    /// `_mm256_madd_epi16` / `vmlal_s16` directly — the `i16` element type
    /// makes that contract a type-system invariant. Recomputed from `scale`
    /// rather than persisted — `scale` is the single source of truth.
    pub(super) d_prime_sq_i16: Vec<i16>,
    /// `(i16::MAX − 1) / max(D'²)` — quantization scale. The 1-bit cap
    /// (vs. full u16) costs ~3e-5 relative precision but lets SIMD use
    /// signed `i16` multiply-adds. `1.0` if all `D'²` are effectively zero
    /// (degenerate quantizer).
    pub(super) weight_scale: f32,
    /// `⟨M, M⟩ = Σ shift²` global constant. Used in symmetric TQ+ scoring to
    /// cancel the double-counted `⟨M, M⟩` from `xm_a + xm_b`.
    pub(super) mm_const: f32,
}

impl ErrorCorrection {
    /// Reconstruct from persisted metadata, validating that `shift` and
    /// `scale` have the expected length. Returns an error if the metadata
    /// has been truncated or otherwise corrupted.
    pub fn new_from_metadata(
        metadata: &ErrorCorrectionMetadata,
        padded_dim: usize,
    ) -> std::io::Result<Self> {
        if metadata.shift.len() != padded_dim || metadata.scale.len() != padded_dim {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "ErrorCorrection metadata length mismatch: shift={}, scale={}, expected={}",
                    metadata.shift.len(),
                    metadata.scale.len(),
                    padded_dim,
                ),
            ));
        }
        Ok(Self::new(metadata.shift.clone(), metadata.scale.clone()))
    }

    pub(super) fn new(shift: Vec<f32>, scale: Vec<f32>) -> Self {
        debug_assert_eq!(
            shift.len(),
            scale.len(),
            "ErrorCorrection shift/scale length mismatch",
        );
        let mm_const = shift.iter().map(|&s| s * s).sum();

        // Build f32 D'² per coord, then quantize to u16 with a single global
        // scale chosen from the largest D'². Per-coord precision lower bound:
        // `1/weight_scale = max(D'²)/65534`; relative error on the smallest
        // coord is `(min/max)·(1/65534)` — overwhelmingly tighter than f32
        // precision in this dynamic range.
        let d_prime_sq_f32: Vec<f32> = scale
            .iter()
            .map(|&s| {
                if s.abs() > f32::EPSILON {
                    (s * s).recip()
                } else {
                    0.0
                }
            })
            .collect();
        let max_d_prime_sq = d_prime_sq_f32.iter().copied().fold(0.0f32, f32::max);
        // Cap quantized values to `i16::MAX − 1` so SIMD kernels can
        // multiply them as signed `i16` without sign flips. See
        // `d_prime_sq_i16` field doc for the full rationale.
        const QUANT_CAP: i16 = i16::MAX - 1;
        let weight_scale = if max_d_prime_sq > f32::EPSILON {
            f32::from(QUANT_CAP) / max_d_prime_sq
        } else {
            1.0
        };
        let d_prime_sq_i16: Vec<i16> = d_prime_sq_f32
            .iter()
            .map(|&x| (x * weight_scale).round().clamp(0.0, f32::from(QUANT_CAP)) as i16)
            .collect();

        ErrorCorrection {
            shift,
            scale,
            d_prime_sq_i16,
            weight_scale,
            mm_const,
        }
    }
}

impl TurboQuantizer {
    /// Initialize a new TurboQuantizer.
    pub fn new(
        dim: usize,
        bits: TQBits,
        mode: TQMode,
        distance: DistanceType,
        error_correction: Option<ErrorCorrection>,
    ) -> Self {
        let padded_dim = Self::padded_dim(dim, bits);
        let rotation = HadamardRotation::new(padded_dim);
        TurboQuantizer {
            rotation,
            bits,
            mode,
            distance,
            padded_dim,
            error_correction,
        }
    }

    /// Initialize a new TurboQuantizer from metadata. Returns `Err` if the
    /// persisted `ErrorCorrection` shift/scale lengths don't match the
    /// expected `padded_dim`.
    pub fn new_from_metadata(metadata: &Metadata) -> std::io::Result<Self> {
        let padded_dim = Self::padded_dim(metadata.vector_parameters.dim, metadata.bits);
        let error_correction = metadata
            .error_correction
            .as_ref()
            .map(|m| ErrorCorrection::new_from_metadata(m, padded_dim))
            .transpose()?;
        Ok(Self::new(
            metadata.vector_parameters.dim,
            metadata.bits,
            metadata.mode,
            metadata.vector_parameters.distance_type,
            error_correction,
        ))
    }

    /// Pad, rotate, and length-rescale `vec` into `buf`. After this call `buf`
    /// holds rotated coordinates whose per-vector L2 norm is `sqrt(padded_dim)`,
    /// matching the Lloyd-Max N(0, 1) centroid grid. Returns the original L2
    /// length (pre-rescale) for distance metrics that store it; `None` for
    /// Cosine.
    ///
    /// Used both by [`Self::quantize`] and by the TQ+ first pass in
    /// [`crate::turboquant::EncodedVectorsTQ::encode`] when computing per-
    /// coordinate stats over rescaled rotated samples.
    pub(crate) fn preprocess_into(&self, vec: &[f32], buf: &mut [f64]) -> Option<f32> {
        debug_assert!(vec.len() <= self.padded_dim);
        debug_assert_eq!(buf.len(), self.padded_dim);

        // Convert to f64 and zero-pad up to `padded_dim`.
        let padded = vec
            .iter()
            .map(|&x| f64::from(x))
            .chain(std::iter::repeat(0.0));
        for (b, v) in buf.iter_mut().zip(padded) {
            *b = v;
        }

        // Rotate the vector.
        self.rotation.apply(buf);

        let l2_length = self.compute_l2_length(buf);

        // Rescale so per-coordinate variance is ~1 — matching the Lloyd-Max
        // N(0, 1) centroid grid. Guard against zero-length inputs: the rescale
        // would be 0 · ∞ = NaN, which corrupts both the packing (via NaN
        // partition_point) and the TQ+ stats first pass (Welford on NaN poisons
        // every coordinate).
        let length = f64::from(l2_length.unwrap_or(1.0));
        if length > 0.0 {
            let length_scale = (self.padded_dim as f64).sqrt() / length;
            for v in buf.iter_mut() {
                *v *= length_scale;
            }
        }

        l2_length
    }

    /// Quantize a given vector with TurboQuant.
    pub fn quantize(&self, vec: &[f32], buf: &mut [f64]) -> Vec<u8> {
        let l2_length = self.preprocess_into(vec, buf);
        // After `preprocess_into` the rescale is already in `buf`; from here on
        // we treat `buf` as the rescaled vector and don't re-multiply by
        // `scale`. Centroid-norm and packing operate on `buf` directly.
        let scale = 1.0_f64;

        // TQ+: stash `xm = ⟨X, M⟩ = -Σ X_i · shift_i` (computed on the
        // rescaled pre-EC vector) for the symmetric-scoring slow path, then
        // apply the per-coordinate shift+scale that pulls each coord onto the
        // codebook's N(0, 1) grid.
        //
        // Zero-input guard: if the rescaled vector is identically zero (e.g.
        // Cosine zero vector — the length-rescale guard preserves zero), the
        // true scoring contract is `score == 0`. Applying EC would inject
        // `shift · scale` into every coord and the resulting quantization
        // noise overwhelms the small centroid_norm, blowing past the test
        // tolerance. Skip EC for zero inputs and store `xm = 0`.
        let xm = self.error_correction.as_ref().map(|ec| {
            let rescaled_l2_sq: f64 = buf.iter().map(|&x| x * x).sum();
            if rescaled_l2_sq < 1e-12 {
                return 0.0_f32;
            }
            let xm: f64 = buf
                .iter()
                .zip(ec.shift.iter())
                .map(|(&x, &s)| x * f64::from(-s))
                .sum();
            for (i, v) in buf.iter_mut().enumerate() {
                *v = (*v + f64::from(ec.shift[i])) * f64::from(ec.scale[i]);
            }
            xm as f32
        });

        // Compute the post-quantization centroid norm for Dot and Cosine so
        // re-normalized scoring can divide by ||c|| instead of sqrt(d).
        //
        // Cosine guard: a zero (or near-zero) input produces an all-zero
        // rotated buf, which quantizes to the small "middle" centroids.
        // ||c|| collapses, and renormalizing would amplify quantization
        // noise instead of correcting bias. Substitute sqrt(padded_dim) so
        // the renormalized denominator equals padded_dim — i.e. renorm is a
        // no-op for these degenerate inputs, preserving baseline behavior.
        let centroid_norm = match self.distance {
            DistanceType::Dot | DistanceType::L2 => Some(self.compute_centroid_norm(buf, scale)),
            DistanceType::Cosine => {
                let rotated_l2_sq: f64 = buf.iter().map(|&x| x * x).sum();
                if rotated_l2_sq < 1e-12 {
                    Some((self.padded_dim as f32).sqrt())
                } else {
                    Some(self.compute_centroid_norm(buf, scale))
                }
            }
            DistanceType::L1 => None,
        };

        let mut extras_bytes = Vec::with_capacity(TqVectorExtras::size_for(
            self.bits,
            self.distance,
            self.mode,
        ));
        self.pack_extras_into(l2_length, centroid_norm, xm, &mut extras_bytes);
        let extras = TqVectorExtras::from_bytes(&extras_bytes);

        // Encode and return packed vector.
        self.pack_vector(buf.iter().map(|&val| val * scale), extras)
    }

    /// L2 norm of the centroid vector chosen by quantizing `buf * scale`.
    /// For TQ+, the stored centroids approximate `X+`, but renorm scoring
    /// needs `cn` to track `‖rescaled_quantized‖` so the per-vector correction
    /// stays close to deterministic `sqrt(d)`. We undo EC on each centroid
    /// (`c · D' + M`) before measuring, matching llama-turbo-quant's approach.
    fn compute_centroid_norm(&self, buf: &[f64], scale: f64) -> f32 {
        let centroids = self.bits.get_centroids();
        let boundaries = self.bits.get_centroid_boundaries();
        let mut sq_sum = 0.0_f64;
        for (i, &val) in buf.iter().enumerate() {
            let scaled = val * scale;
            let idx = boundaries.partition_point(|&b| (scaled as f32) > b);
            let c = f64::from(centroids[idx]);
            // For TQ+, revert EC so `c_reverted` lives in rescaled-space.
            // `‖rescaled‖ = sqrt(d)` deterministically across vectors, so
            // `cn` only drifts from `sqrt(d)` due to quantization noise —
            // exactly what renorm's `l2 / cn` is designed to correct.
            let c_reverted = match &self.error_correction {
                Some(ec) => c / f64::from(ec.scale[i]) - f64::from(ec.shift[i]),
                None => c,
            };
            sq_sum += c_reverted * c_reverted;
        }
        let norm = sq_sum.sqrt() as f32;
        debug_assert!(
            norm.is_finite() && norm > 0.0,
            "centroid_norm must be finite and positive, got {norm}"
        );
        norm
    }

    pub fn dequantize(&self, quantized: &[u8]) -> Vec<f64> {
        let (unpacked_iter, extras) = self.unpack_vector(quantized);
        let scaling_factor = f64::from(extras.scaling_factor());
        // Materialize the unpacked centroids once. `unpack_vector` returns a
        // streaming `BitReader` iterator and used to be invoked twice (here
        // and in the `cn_quant` recompute below) — bit-unpacking the same
        // bytes twice for every dequantize call.
        let unpacked: Vec<f64> = unpacked_iter.collect();

        // Stored `scaling_factor` is `l2/cn_quant` for Dot/Cosine/L2 (`l2 ==
        // 1.0` for Cosine). To recover the original l2 length we either
        // multiply by `cn_quant` recomputed from the unpacked centroids
        // (Dot/Cosine — `l2` isn't stored separately) or read the dedicated
        // `l2_length` field (L2 stores it directly, no recompute needed).
        // For TQ+, `cn_quant` is measured in the EC-reverted (rescaled) space,
        // matching `compute_centroid_norm`'s convention.
        let recovered_l2 = match self.distance {
            DistanceType::Dot | DistanceType::Cosine => {
                let cn_quant = match &self.error_correction {
                    Some(ec) => unpacked
                        .iter()
                        .enumerate()
                        .map(|(i, &x)| {
                            let r = x / f64::from(ec.scale[i]) - f64::from(ec.shift[i]);
                            r * r
                        })
                        .sum::<f64>()
                        .sqrt(),
                    None => unpacked.iter().map(|&x| x * x).sum::<f64>().sqrt(),
                };
                scaling_factor * cn_quant
            }
            DistanceType::L2 => f64::from(extras.l2_length()),
            DistanceType::L1 => scaling_factor,
        };

        let scale = recovered_l2 / (self.padded_dim as f64).sqrt();
        match &self.error_correction {
            // TQ+: stored centroids approximate `X+`; revert EC to recover the
            // rescaled coordinate before applying the length scale.
            Some(ec) => unpacked
                .into_iter()
                .enumerate()
                .map(|(i, x)| {
                    let rescaled = x / f64::from(ec.scale[i]) - f64::from(ec.shift[i]);
                    rescaled * scale
                })
                .collect(),
            None => unpacked.into_iter().map(|x| x * scale).collect(),
        }
    }

    /// Similarity score between two vectors that were both encoded with this
    /// quantizer. Returns an approximate `<v1, v2>` for Dot and `cos(θ)` for
    /// Cosine.
    ///
    /// For asymmetric scoring (original vector against quantized vector), refer to [`Self::score_precomputed`]
    /// and precompute the query first.
    pub fn score_symmetric(&self, v1: &[u8], v2: &[u8]) -> f32 {
        let (data_v1, extra_v1) = self.split_vector(v1);
        let (data_v2, extra_v2) = self.split_vector(v2);

        // For TQ+, the SIMD kernels' uniform-weight dot can't produce the
        // per-coord-weighted `Σ X+_a_i · X+_b_i · D'_i²` we need. Fall back to
        // a scalar loop and add the precomputed `xm_a + xm_b − ⟨M, M⟩`.
        // SIMD support for this path is a follow-up.
        let raw_dot = match &self.error_correction {
            Some(ec) => self.score_symmetric_ec(data_v1, data_v2, &extra_v1, &extra_v2, ec),
            None => match self.bits {
                TQBits::Bits1 => score_1bit_internal(data_v1, data_v2),
                TQBits::Bits1_5 => score_1bit_internal(data_v1, data_v2),
                TQBits::Bits2 => score_2bit_internal(data_v1, data_v2),
                TQBits::Bits4 => score_4bit_internal(data_v1, data_v2),
            },
        };

        let v1_scale = extra_v1.scaling_factor();
        let v2_scale = extra_v2.scaling_factor();

        match self.distance {
            DistanceType::Cosine | DistanceType::Dot => raw_dot * v1_scale * v2_scale,
            DistanceType::L2 => {
                // ||v1 - v2||² = ||v1||² + ||v2||² - 2·⟨v1, v2⟩.
                // `v*_scale = l2 / cn` already folds renorm in, so the cross
                // term `2 · raw_dot · sf_a · sf_b` reconstructs `2·⟨v1, v2⟩`
                // exactly the way Dot/Cosine do. `l2_length` is fetched
                // separately for the `||v||²` term — we can't read it back
                // out of `sf` without `cn`.
                let l2_a = extra_v1.l2_length();
                let l2_b = extra_v2.l2_length();
                l2_a * l2_a + l2_b * l2_b - 2.0 * v1_scale * v2_scale * raw_dot
            }
            DistanceType::L1 => {
                // Fallback case for L1, where we need to fully dequantize both vectors.
                let mut deq_v1: Vec<f64> = self.dequantize(v1);
                self.rotation.apply_inverse(deq_v1.as_mut_slice());
                let mut deq_v2: Vec<f64> = self.dequantize(v2);
                self.rotation.apply_inverse(deq_v2.as_mut_slice());
                deq_v1
                    .iter()
                    .zip(deq_v2.iter())
                    .map(|(&x, &y)| (x - y).abs() as f32)
                    .sum()
            }
        }
    }

    /// TQ+ symmetric-scoring slow path.
    ///
    /// **Bits1 / Bits1_5**: plain `Σ c_a · c_b` — no `D'²` weighting, no
    /// `xm`/`mm` fold-in. Matches llama-turbo-quant's 1-bit symmetric path.
    /// This computes `⟨X⁺_a, X⁺_b⟩` (centered+normalized space), not the
    /// rescaled-space dot. Mathematically a different quantity, but for
    /// HNSW ranking it's stable: no `D'⁴` variance amplification on
    /// anisotropic coords. Adding `xm + xm − ⟨M, M⟩` *without* `D'²`
    /// scrambles scales (centroid dot is in `X⁺`-space units, `xm`/`mm`
    /// are in `X`-space units) and destroys ranking entirely.
    ///
    /// **Bits2 / Bits4**: full math-correct form `Σ c_a c_b D'_i² + xm_a
    /// + xm_b − ⟨M, M⟩`. Codebooks are fine-grained enough that the
    /// quantization noise stays small even after `D'⁴` amplification.
    fn score_symmetric_ec(
        &self,
        data_v1: &[u8],
        data_v2: &[u8],
        extra_v1: &TqVectorExtras<'_>,
        extra_v2: &TqVectorExtras<'_>,
        ec: &ErrorCorrection,
    ) -> f32 {
        // Bits2 / Bits4: integer SIMD-amenable weighted dot. The kernel
        // returns `Σ c_a_int · c_b_int · weights_i16` as i64; we divide back
        // by `weight_scale · CODEBOOK_SCALE²` to recover the f32 centroid dot.
        let (raw_int, codebook_scale_sq) = match self.bits {
            TQBits::Bits2 => (
                score_2bit_internal_weighted(data_v1, data_v2, &ec.d_prime_sq_i16),
                CODEBOOK_SCALE_SQ_2BIT,
            ),
            TQBits::Bits4 => (
                score_4bit_internal_weighted(data_v1, data_v2, &ec.d_prime_sq_i16),
                CODEBOOK_SCALE_SQ_4BIT,
            ),
            TQBits::Bits1 | TQBits::Bits1_5 => {
                // 1-bit TQ+ ignores `D'²` weighting and `xm`/`mm` corrections (see
                // doc comment above `score_symmetric_ec`), so the score reduces to
                // the plain centroid dot — exactly what `score_1bit_internal`
                // computes via SIMD XOR-popcount. Reuse it.
                return score_1bit_internal(data_v1, data_v2);
            }
        };
        let weighted = raw_int as f32 / (ec.weight_scale * codebook_scale_sq);

        let xm_a = extra_v1.ec_correction();
        let xm_b = extra_v2.ec_correction();
        weighted + xm_a + xm_b - ec.mm_const
    }

    /// Precompute the Hadamard rotation of `query` and hand it to the
    /// bit-width's SIMD encoder.  Subsequent [`Self::score_precomputed`]
    /// calls reuse this precomputation — rotation runs once, not per score.
    pub fn precompute_query(&self, query: &[f32]) -> EncodedQueryTQ {
        debug_assert!(query.len() <= self.padded_dim);

        let mut rotated: Vec<f64> = query
            .iter()
            .map(|&x| f64::from(x))
            .chain(std::iter::repeat(0.0))
            .take(self.padded_dim)
            .collect();
        self.rotation.apply(&mut rotated);

        let l2_norm = match self.distance {
            DistanceType::L1 | DistanceType::L2 | DistanceType::Dot => {
                Some(rotated.iter().map(|&i| i * i).sum::<f64>().sqrt() as f32)
            }
            DistanceType::Cosine => None,
        };

        // TQ+ asymmetric: ⟨Q, X⟩ = ⟨Q .* D', X+⟩ + ⟨Q, M⟩, where D' = 1/scale
        // and M = -shift. Pre-scale `rotated` by `D'` so the existing SIMD
        // raw_dot computes ⟨Q+, X+⟩, and stash `qm = ⟨Q, M⟩` for the scalar
        // correction. The whole point of TQ+ is that scoring code-paths stay
        // identical — only the query precomputation changes.
        let ec_correction: f32 = if let Some(ec) = &self.error_correction {
            let qm: f64 = rotated
                .iter()
                .zip(ec.shift.iter())
                .map(|(&q, &s)| q * f64::from(-s))
                .sum();
            for (q, &s) in rotated.iter_mut().zip(ec.scale.iter()) {
                *q /= f64::from(s);
            }
            qm as f32
        } else {
            0.0
        };

        let query = match self.distance {
            DistanceType::L1 => Some(query.to_vec()),
            DistanceType::Cosine | DistanceType::Dot | DistanceType::L2 => None,
        };

        // SIMD encoders consume f32 (the quantization step will re-bucket into
        // integer codebooks anyway, so the extra f64 precision from rotation
        // has no downstream benefit here).
        let rotated_f32: Vec<f32> = rotated.iter().map(|&x| x as f32).collect();

        // For TQ+ + Bits1 storage, widen query quantization from the default
        // 8 bits to the kernel's max of 16. The per-coord `D'` pre-scaling
        // can push some coords toward the small end of the integer range;
        // 8 bits loses too much there.
        let use_wide_query =
            self.error_correction.is_some() && matches!(self.bits, TQBits::Bits1 | TQBits::Bits1_5);
        let data = match self.bits {
            TQBits::Bits1 | TQBits::Bits1_5 if use_wide_query => {
                EncodedQueryTQData::Bits1Wide(Query1bitSimd::<16>::new(&rotated_f32))
            }
            TQBits::Bits1 => EncodedQueryTQData::Bits1(Query1bitSimd::new(&rotated_f32)),
            TQBits::Bits1_5 => EncodedQueryTQData::Bits1(Query1bitSimd::new(&rotated_f32)),
            TQBits::Bits2 => EncodedQueryTQData::Bits2(Query2bitSimd::new(&rotated_f32)),
            TQBits::Bits4 => EncodedQueryTQData::Bits4(Query4bitSimd::new(&rotated_f32)),
        };
        EncodedQueryTQ {
            data,
            l2_norm,
            query,
            ec_correction,
        }
    }

    /// Similarity score with a query that has already been rotated via
    /// [`Self::precompute_query`]. Returns an approximate `<query, v>` for Dot
    /// and `cos(θ)` for Cosine.
    pub fn score_precomputed(&self, query: &EncodedQueryTQ, vec: &[u8]) -> f32 {
        let (data_bytes, vector_extras) = self.split_vector(vec);
        let raw_dot = match &query.data {
            EncodedQueryTQData::Bits1(q) => q.dotprod(data_bytes),
            EncodedQueryTQData::Bits1Wide(q) => q.dotprod(data_bytes),
            EncodedQueryTQData::Bits2(q) => q.dotprod(data_bytes),
            EncodedQueryTQData::Bits4(q) => q.dotprod(data_bytes),
        };
        // TQ+: SIMD raw_dot ≈ ⟨Q · D', X+⟩; add `qm = ⟨Q, M⟩` to recover
        // ⟨Q, rescaled_v⟩, which is the quantity the existing arms expect.
        let dot = raw_dot + query.ec_correction;

        match self.distance {
            DistanceType::Cosine | DistanceType::Dot => {
                let scaling_factor = vector_extras.scaling_factor();
                dot * scaling_factor
            }
            DistanceType::L2 => {
                // ||q - v||² = ||q||² + ||v||² - 2·⟨q, v⟩.
                // `scaling_factor = l2 / cn` folds renorm in, so `dot · sf`
                // reconstructs `⟨q, v⟩`. Query side isn't quantized → no `cn`
                // factor on that side. `l2_length` is the raw `||v||`.
                let scaling_factor = vector_extras.scaling_factor();
                let l2 = vector_extras.l2_length();
                let query_l2 = query.l2_norm.unwrap_or(1.0);
                query_l2 * query_l2 + l2 * l2 - 2.0 * dot * scaling_factor
            }
            DistanceType::L1 => {
                let mut deq_v: Vec<f64> = self.dequantize(vec);
                self.rotation.apply_inverse(deq_v.as_mut_slice());
                query
                    .query
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(deq_v.iter())
                    .map(|(&q, &v)| (f64::from(q) - v).abs() as f32)
                    .sum()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use common::bitpacking::BitReader;
    use rand::prelude::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;

    fn make_tq(dim: usize, bits: TQBits, distance: DistanceType) -> TurboQuantizer {
        TurboQuantizer::new(dim, bits, TQMode::Normal, distance, None)
    }

    /// Build a vector pair that has a given magnitude of similarity, tuned by `similarity`.
    /// `similarity = 1` means identical and `similarity = 0` means independent random vectors.
    fn generate_random_vector_pair_with_similarity(
        dim: usize,
        similarity: f32,
        rng: &mut rand::prelude::StdRng,
    ) -> (Vec<f32>, Vec<f32>) {
        use rand::RngExt;
        let a: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
        let noise: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
        let b: Vec<f32> = a
            .iter()
            .zip(noise.iter())
            .map(|(&x, &n)| similarity * x + (1.0 - similarity) * n)
            .collect();
        (a, b)
    }

    /// Build a single random vector with components uniformly in `[-1, 1]`.
    fn random_vector(dim: usize, rng: &mut StdRng) -> Vec<f32> {
        (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect()
    }

    fn l2_norm(v: &[f32]) -> f64 {
        v.iter()
            .map(|&x| f64::from(x) * f64::from(x))
            .sum::<f64>()
            .sqrt()
    }

    /// Score an original vector against a quantized one.
    fn asymmetric_score_helper(tq: &TurboQuantizer, query: &[f32], vec: &[u8]) -> f32 {
        let precomputed = tq.precompute_query(query);
        tq.score_precomputed(&precomputed, vec)
    }

    /// Normalize `v` onto the unit sphere — required input for the Cosine
    /// quantizer path.
    fn normalize_vector(v: &[f32]) -> Vec<f32> {
        let len = l2_norm(v) as f32;
        v.iter().map(|&x| x / len).collect()
    }

    /// Helper: unpack all centroid indices from a quantized byte vector.
    fn unpack_indices(packed: &[u8], dim: usize, bits: TQBits) -> Vec<u8> {
        let mut reader = BitReader::new(packed);
        reader.set_bits(bits.bit_size());
        (0..dim).map(|_| reader.read()).collect()
    }

    #[inline]
    fn dot_f32_impl<I, J>(left: I, right: J) -> f32
    where
        I: Iterator<Item = f32>,
        J: Iterator<Item = f32>,
    {
        left.zip(right).map(|(q, u)| q * u).sum::<f32>()
    }

    /// Extreme-magnitude inputs (including values near f32::MAX) must still
    /// produce in-range centroid indices rather than panicking or wrapping.
    #[test]
    fn quantize_extreme_values() {
        for &dim in &[127, 128, 513] {
            for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
                let tq = make_tq(dim, bits, DistanceType::Cosine);
                let mut buf = vec![0.0f64; tq.padded_dim];
                let n_centroids = 1u8 << bits.bit_size();

                for &val in &[1000.0f32, -1000.0, f32::MAX / 2.0, f32::MIN / 2.0] {
                    let vec = vec![val; dim];
                    let result = tq.quantize(&vec, &mut buf);

                    let indices = unpack_indices(&result, dim, bits);
                    for &idx in &indices {
                        assert!(
                            idx < n_centroids,
                            "dim={dim}, index {idx} out of range for {bits:?}"
                        );
                    }
                }
            }
        }
    }

    /// Output byte length must be ceil(dim * bit_size / 8).
    #[test]
    fn quantize_output_byte_length() {
        let dims = [64, 128, 300, 384, 512, 768, 1024, 1536];
        let bit_widths = [TQBits::Bits1, TQBits::Bits2, TQBits::Bits4];

        for &bits in &bit_widths {
            for &dim in &dims {
                let tq = make_tq(dim, bits, DistanceType::Cosine);
                let mut buf = vec![0.0f64; tq.padded_dim];
                let vec = vec![0.1; dim];
                let result = tq.quantize(&vec, &mut buf);
                let expected_bytes = tq.quantized_size();
                assert_eq!(
                    result.len(),
                    expected_bytes,
                    "dim={dim}, bits={bits:?}: expected {expected_bytes} bytes, got {}",
                    result.len()
                );
            }
        }
    }

    /// Quantizing the same vector twice must produce identical output.
    #[test]
    fn quantize_deterministic() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let mut rng = StdRng::seed_from_u64(123);

        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            for &dim in &[127, 128, 300, 513, 768] {
                let tq = make_tq(dim, bits, DistanceType::Cosine);
                let mut buf = vec![0.0f64; tq.padded_dim];
                let vec: Vec<f32> = (0..dim).map(|_| rng.random_range(-2.0..2.0)).collect();

                let r1 = tq.quantize(&vec, &mut buf);
                let r2 = tq.quantize(&vec, &mut buf);
                assert_eq!(r1, r2, "dim={dim}, bits={bits:?}: non-deterministic output");
            }
        }
    }

    /// A zero vector, after rotation, stays zero. All indices should map to the
    /// middle boundary region (centroids are symmetric around 0).
    #[test]
    fn quantize_zero_vector() {
        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let n_centroids = 1u8 << bits.bit_size();
            // For symmetric centroids around 0, zero maps to either of the two
            // middle indices. With boundaries being midpoints of consecutive
            // centroids, 0.0 lands at boundary n_centroids/2 - 1 or n_centroids/2.
            let middle_low = n_centroids / 2 - 1;
            let middle_high = n_centroids / 2;

            for &dim in &[127, 128, 256, 512, 513] {
                let tq = make_tq(dim, bits, DistanceType::Cosine);
                let mut buf = vec![0.0f64; tq.padded_dim];
                let vec = vec![0.0; dim];
                let result = tq.quantize(&vec, &mut buf);
                let indices = unpack_indices(&result, dim, bits);

                for (d, &idx) in indices.iter().enumerate() {
                    assert!(
                        idx == middle_low || idx == middle_high,
                        "dim={dim}, bits={bits:?}, d={d}: zero-vector index {idx} \
                         not in [{middle_low}, {middle_high}]"
                    );
                }
            }
        }
    }

    /// Non-power-of-2 dimensions should work correctly (the rotation decomposes
    /// into power-of-2 chunks internally).
    #[test]
    fn quantize_non_power_of_two_dims() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let mut rng = StdRng::seed_from_u64(42);
        // Focus on small and odd dims where the Hadamard decomposition
        // into power-of-2 chunks is most likely to trip. The larger
        // non-pow-2 sizes are already covered by quantize_output_byte_length.
        let odd_dims = [3, 50, 127, 700, 1025];

        for &dim in &odd_dims {
            for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
                let tq = make_tq(dim, bits, DistanceType::Cosine);
                let n_centroids = 1u8 << bits.bit_size();
                let vec: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

                let mut buf = vec![0.0f64; tq.padded_dim];
                let result = tq.quantize(&vec, &mut buf);

                // Correct length.
                let expected_bytes = tq.quantized_size();
                assert_eq!(result.len(), expected_bytes, "dim={dim}, bits={bits:?}");

                // All indices in range.
                let indices = unpack_indices(&result, dim, bits);
                for &idx in &indices {
                    assert!(
                        idx < n_centroids,
                        "dim={dim}, bits={bits:?}: index {idx} out of range"
                    );
                }
            }
        }
    }

    /// Feeding the centroid values yielded by `unpack_vector` back into
    /// `pack_vector` must reproduce the same bytes — and decode to the same
    /// values — across a variety of dims and bit widths.
    #[test]
    fn pack_unpack_vector_roundtrip() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let mut rng = StdRng::seed_from_u64(321);

        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let centroids = bits.get_centroids();
            let n_centroids = 1u8 << bits.bit_size();

            for &dim in &[1, 64, 127, 128, 300, 768, 1025] {
                let tq = make_tq(dim, bits, DistanceType::Cosine);
                let indices: Vec<u8> = (0..dim).map(|_| rng.random_range(0..n_centroids)).collect();
                let values: Vec<f64> = indices
                    .iter()
                    .map(|&idx| f64::from(centroids[idx as usize]))
                    .collect();

                // Cosine extras are 4 bytes (centroid_norm); dummy value is fine
                // for this test since we only inspect the unpacked centroid values.
                let dummy_extras = 1.0_f32.to_le_bytes();
                let packed = tq.pack_vector(
                    values.iter().copied(),
                    TqVectorExtras::from_bytes(&dummy_extras),
                );

                let out: Vec<f64> = tq.unpack_vector(&packed).0.collect();

                for (i, (&expected, &value)) in values.iter().zip(out.iter()).enumerate() {
                    assert_eq!(
                        value, expected,
                        "dim={dim}, bits={bits:?}, i={i}: \
                         decoded to {value}, expected {expected}"
                    );
                }
            }
        }
    }

    /// Quantized scores (symmetric and asymmetric paths) stay within tolerance
    /// of the true dot/cosine similarity across a range of pair similarities.
    #[test]
    fn score_approximates_true_similarity() {
        for dim in [127, 128, 300, 512, 513, 1000, 1024, 1025, 2000, 4000] {
            let bits = TQBits::Bits4;
            let mut rng = StdRng::seed_from_u64(42);

            for &distance in &[DistanceType::Dot, DistanceType::Cosine] {
                let tq = make_tq(dim, bits, distance);
                let mut buf = vec![0.0f64; tq.padded_dim];

                for &similarity in &[0.2f32, 0.5, 0.8] {
                    let (a_raw, b_raw) =
                        generate_random_vector_pair_with_similarity(dim, similarity, &mut rng);

                    // Cosine path requires unit-norm inputs.
                    let (a, b) = match distance {
                        DistanceType::Cosine => {
                            (normalize_vector(&a_raw), normalize_vector(&b_raw))
                        }
                        _ => (a_raw, b_raw),
                    };

                    let true_score = dot_f32_impl(a.iter().copied(), b.iter().copied());

                    let a_q = tq.quantize(&a, &mut buf);
                    let b_q = tq.quantize(&b, &mut buf);

                    let sym = tq.score_symmetric(&a_q, &b_q);
                    let asym = asymmetric_score_helper(&tq, &a, &b_q);

                    // Cosine scores are bounded in [-1, 1]; Dot scales with ||a||*||b||.
                    let scale = match distance {
                        DistanceType::Cosine => 1.0,
                        _ => (l2_norm(&a) * l2_norm(&b)) as f32,
                    };
                    let tol = 0.05 * scale;

                    assert!(
                        (sym - true_score).abs() < tol,
                        "symmetric: distance={distance:?}, similarity={similarity}, \
                     got {sym}, expected {true_score} (tol {tol})"
                    );
                    assert!(
                        (asym - true_score).abs() < tol,
                        "asymmetric: distance={distance:?}, similarity={similarity}, \
                     got {asym}, expected {true_score} (tol {tol})"
                    );
                }
            }
        }
    }

    /// score(v, v) must recover ‖v‖² for Dot and 1.0 for Cosine, across both
    /// symmetric and asymmetric scoring paths.
    #[test]
    fn score_self_similarity() {
        let bits = TQBits::Bits4;

        for dim in [127, 128, 300, 512, 513, 1024, 1025, 2000] {
            let mut rng = StdRng::seed_from_u64(42);

            for &distance in &[DistanceType::Dot, DistanceType::Cosine] {
                let tq = make_tq(dim, bits, distance);
                let mut buf = vec![0.0f64; tq.padded_dim];

                let raw = random_vector(dim, &mut rng);
                let v = match distance {
                    DistanceType::Cosine => normalize_vector(&raw),
                    _ => raw,
                };

                let expected = match distance {
                    DistanceType::Cosine => 1.0,
                    _ => (l2_norm(&v) * l2_norm(&v)) as f32,
                };
                let tol = 0.05 * expected.abs().max(1.0);

                let v_q = tq.quantize(&v, &mut buf);

                let sym = tq.score_symmetric(&v_q, &v_q);
                let asym = asymmetric_score_helper(&tq, &v, &v_q);

                assert!(
                    (sym - expected).abs() < tol,
                    "symmetric: dim={dim}, distance={distance:?}, \
                     got {sym}, expected {expected} (tol {tol})"
                );
                assert!(
                    (asym - expected).abs() < tol,
                    "asymmetric: dim={dim}, distance={distance:?}, \
                     got {asym}, expected {expected} (tol {tol})"
                );
            }
        }
    }

    /// score(v, -v) must recover -‖v‖² for Dot and -1.0 for Cosine across both
    /// scoring paths — the sign correctness check on the negative end.
    #[test]
    fn score_antipodal_is_negative() {
        let bits = TQBits::Bits4;

        for dim in [127, 128, 300, 512, 513, 1024, 1025, 2000] {
            let mut rng = StdRng::seed_from_u64(42);

            for &distance in &[DistanceType::Dot, DistanceType::Cosine] {
                let tq = make_tq(dim, bits, distance);
                let mut buf = vec![0.0f64; tq.padded_dim];

                let raw = random_vector(dim, &mut rng);
                let v = match distance {
                    DistanceType::Cosine => normalize_vector(&raw),
                    _ => raw,
                };
                let neg_v: Vec<f32> = v.iter().map(|&x| -x).collect();

                let expected = match distance {
                    DistanceType::Cosine => -1.0,
                    _ => -(l2_norm(&v) * l2_norm(&v)) as f32,
                };
                let tol = 0.05 * expected.abs().max(1.0);

                let v_q = tq.quantize(&v, &mut buf);
                let neg_q = tq.quantize(&neg_v, &mut buf);

                let sym = tq.score_symmetric(&v_q, &neg_q);
                let asym = asymmetric_score_helper(&tq, &v, &neg_q);

                assert!(
                    (sym - expected).abs() < tol,
                    "symmetric: dim={dim}, distance={distance:?}, \
                     got {sym}, expected {expected} (tol {tol})"
                );
                assert!(
                    (asym - expected).abs() < tol,
                    "asymmetric: dim={dim}, distance={distance:?}, \
                     got {asym}, expected {expected} (tol {tol})"
                );
            }
        }
    }

    /// Higher bit widths must produce lower mean-absolute scoring error:
    /// MAE(Bits4) ≤ MAE(Bits2) ≤ MAE(Bits1) across a batch of random pairs.
    #[test]
    fn higher_bits_reduce_error() {
        let n_pairs = 32;

        for dim in [512, 513] {
            for &distance in &[DistanceType::Dot, DistanceType::Cosine] {
                let mae = |bits: TQBits| -> f32 {
                    let mut rng = StdRng::seed_from_u64(42);
                    let tq = make_tq(dim, bits, distance);
                    let mut buf = vec![0.0f64; tq.padded_dim];

                    let total: f32 = (0..n_pairs)
                        .map(|_| {
                            let (a_raw, b_raw) =
                                generate_random_vector_pair_with_similarity(dim, 0.5, &mut rng);
                            let (a, b) = match distance {
                                DistanceType::Cosine => {
                                    (normalize_vector(&a_raw), normalize_vector(&b_raw))
                                }
                                _ => (a_raw, b_raw),
                            };
                            let truth = dot_f32_impl(a.iter().copied(), b.iter().copied());
                            let a_q = tq.quantize(&a, &mut buf);
                            let b_q = tq.quantize(&b, &mut buf);
                            (tq.score_symmetric(&a_q, &b_q) - truth).abs()
                        })
                        .sum();
                    total / n_pairs as f32
                };

                let mae_1 = mae(TQBits::Bits1);
                let mae_2 = mae(TQBits::Bits2);
                let mae_4 = mae(TQBits::Bits4);

                assert!(
                    mae_4 <= mae_2 && mae_2 <= mae_1,
                    "dim={dim}, distance={distance:?}: MAE not monotonic in bits — \
                     Bits1={mae_1}, Bits2={mae_2}, Bits4={mae_4}"
                );
            }
        }
    }

    /// Scoring extreme-magnitude (but f32-in-range) vectors must produce finite
    /// results — not NaN or ±Inf — on both symmetric and asymmetric paths.
    #[test]
    fn score_extreme_magnitudes_finite() {
        let bits = TQBits::Bits4;

        for dim in [127, 128, 513] {
            for &distance in &[DistanceType::Dot, DistanceType::Cosine] {
                let tq = make_tq(dim, bits, distance);
                let mut buf = vec![0.0f64; tq.padded_dim];

                for &val in &[1000.0f32, -1000.0, 1e6, -1e6] {
                    let raw = vec![val; dim];
                    let v = match distance {
                        DistanceType::Cosine => normalize_vector(&raw),
                        _ => raw,
                    };

                    let v_q = tq.quantize(&v, &mut buf);
                    let sym = tq.score_symmetric(&v_q, &v_q);
                    let asym = asymmetric_score_helper(&tq, &v, &v_q);

                    assert!(
                        sym.is_finite(),
                        "symmetric: dim={dim}, distance={distance:?}, val={val}: got {sym}"
                    );
                    assert!(
                        asym.is_finite(),
                        "asymmetric: dim={dim}, distance={distance:?}, val={val}: got {asym}"
                    );
                }
            }
        }
    }

    /// Quantized scoring preserves candidate ordering: ranking by quantized
    /// scores matches ranking by true scores on all but a small fraction of
    /// pairwise comparisons.
    #[test]
    fn rank_preservation() {
        let bits = TQBits::Bits4;

        for dim in [512, 513] {
            for &distance in &[DistanceType::Dot, DistanceType::Cosine] {
                let mut rng = StdRng::seed_from_u64(42);
                let tq = make_tq(dim, bits, distance);
                let mut buf = vec![0.0f64; tq.padded_dim];

                let query_raw = random_vector(dim, &mut rng);
                let similarities: Vec<f32> = (1..=10).map(|i| i as f32 / 10.0).collect();
                let candidates_raw: Vec<Vec<f32>> = similarities
                    .iter()
                    .map(|&s| {
                        let noise = random_vector(dim, &mut rng);
                        query_raw
                            .iter()
                            .zip(&noise)
                            .map(|(&q, &n)| s * q + (1.0 - s) * n)
                            .collect()
                    })
                    .collect();

                let query = match distance {
                    DistanceType::Cosine => normalize_vector(&query_raw),
                    _ => query_raw,
                };
                let candidates: Vec<Vec<f32>> = candidates_raw
                    .iter()
                    .map(|c| match distance {
                        DistanceType::Cosine => normalize_vector(c),
                        _ => c.clone(),
                    })
                    .collect();

                let true_scores: Vec<f32> = candidates
                    .iter()
                    .map(|c| dot_f32_impl(query.iter().copied(), c.iter().copied()))
                    .collect();
                let quant_scores: Vec<f32> = candidates
                    .iter()
                    .map(|c| {
                        let cq = tq.quantize(c, &mut buf);
                        asymmetric_score_helper(&tq, &query, &cq)
                    })
                    .collect();

                let n = candidates.len();
                let mut inversions = 0;
                for i in 0..n {
                    for j in (i + 1)..n {
                        let true_sign = (true_scores[i] - true_scores[j]).signum();
                        let quant_sign = (quant_scores[i] - quant_scores[j]).signum();
                        if true_sign != 0.0 && true_sign != quant_sign {
                            inversions += 1;
                        }
                    }
                }
                let total_pairs = n * (n - 1) / 2;

                assert!(
                    inversions * 100 < 15 * total_pairs,
                    "dim={dim}, distance={distance:?}: {inversions}/{total_pairs} pairs inverted"
                );
            }
        }
    }

    /// Dot scoring scales linearly with vector magnitude:
    /// score(q, k·v) ≈ k·⟨q,v⟩  and  score(k·q, k·v) ≈ k²·⟨q,v⟩.
    /// Guards that the stored l2_length extras correctly restore scale.
    /// (Cosine's contract requires unit-norm inputs, so scaling is out-of-scope.)
    #[test]
    fn score_linearity_dot() {
        let bits = TQBits::Bits4;

        for dim in [512, 513] {
            let mut rng = StdRng::seed_from_u64(42);
            let tq = make_tq(dim, bits, DistanceType::Dot);
            let mut buf = vec![0.0f64; tq.padded_dim];

            let (q, v) = generate_random_vector_pair_with_similarity(dim, 0.5, &mut rng);
            let true_dot = dot_f32_impl(q.iter().copied(), v.iter().copied());

            for &k in &[0.5f32, 2.0, 5.0] {
                let q_scaled: Vec<f32> = q.iter().map(|&x| x * k).collect();
                let v_scaled: Vec<f32> = v.iter().map(|&x| x * k).collect();

                let q_scaled_q = tq.quantize(&q_scaled, &mut buf);
                let v_scaled_q = tq.quantize(&v_scaled, &mut buf);

                let expected_asym = k * true_dot;
                let expected_sym = k * k * true_dot;

                let asym = asymmetric_score_helper(&tq, &q, &v_scaled_q);
                let sym = tq.score_symmetric(&q_scaled_q, &v_scaled_q);

                let tol_asym = 0.05 * (l2_norm(&q) * l2_norm(&v_scaled)) as f32;
                let tol_sym = 0.05 * (l2_norm(&q_scaled) * l2_norm(&v_scaled)) as f32;

                assert!(
                    (asym - expected_asym).abs() < tol_asym,
                    "asymmetric: dim={dim}, k={k}, got {asym}, expected {expected_asym} (tol {tol_asym})"
                );
                assert!(
                    (sym - expected_sym).abs() < tol_sym,
                    "symmetric: dim={dim}, k={k}, got {sym}, expected {expected_sym} (tol {tol_sym})"
                );
            }
        }
    }

    /// Edge cases for the pack/unpack roundtrip: uniform all-min and all-max
    /// centroid values exercise the bit-packing boundaries.
    #[test]
    fn pack_unpack_vector_uniform_indices() {
        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let centroids = bits.get_centroids();
            let max_idx = (1u8 << bits.bit_size()) - 1;

            for &dim in &[1, 8, 128, 513] {
                let tq = make_tq(dim, bits, DistanceType::Cosine);

                for &idx in &[0u8, max_idx] {
                    let expected = f64::from(centroids[idx as usize]);
                    let values = vec![expected; dim];
                    // Cosine extras are 4 bytes (centroid_norm); dummy value is
                    // fine here since we only inspect the unpacked centroid values.
                    let dummy_extras = 1.0_f32.to_le_bytes();
                    let packed = tq.pack_vector(
                        values.iter().copied(),
                        TqVectorExtras::from_bytes(&dummy_extras),
                    );

                    let out: Vec<f64> = tq.unpack_vector(&packed).0.collect();

                    // unpack_vector yields padded_dim values; only the first
                    // dim correspond to caller input, the rest are padding.
                    for (i, &v) in out.iter().take(dim).enumerate() {
                        assert_eq!(v, expected, "dim={dim}, bits={bits:?}, idx={idx}, i={i}");
                    }
                }
            }
        }
    }

    /// Sanity-check that [`TurboQuantizer::precompute_query`] +
    /// [`TurboQuantizer::score_precomputed`] dispatch works for every
    /// supported bit width.  The precision-oriented tests above lock
    /// `TQBits::Bits4`, so the `EncodedQueryTQData::Bits1`/`Bits2` dispatch
    /// arms (and the Query1bitSimd / Query2bitSimd wiring behind them) would
    /// otherwise only be exercised by each kernel's own module-level parity
    /// tests, never through the integrated `precompute_query` path.
    ///
    /// The checks here are deliberately loose — 1-bit scoring discards almost
    /// all amplitude info so tight `|got − truth| < ε` asserts would be
    /// meaningless.  We require: finite output, self-similarity positive,
    /// antipodal negative, and a non-trivial gap between the two.
    #[rstest::rstest]
    #[case::bits1(TQBits::Bits1)]
    #[case::bits2(TQBits::Bits2)]
    #[case::bits4(TQBits::Bits4)]
    fn score_precomputed_dispatches_all_bit_widths(#[case] bits: TQBits) {
        let dim = 512;

        for &distance in &[DistanceType::Dot, DistanceType::Cosine] {
            let mut rng = StdRng::seed_from_u64(0xD15_DA7C4); // same across distances → stable
            let tq = make_tq(dim, bits, distance);
            let mut buf = vec![0.0f64; tq.padded_dim];

            let raw = random_vector(dim, &mut rng);
            let v = match distance {
                DistanceType::Cosine => normalize_vector(&raw),
                _ => raw,
            };
            let neg_v: Vec<f32> = v.iter().map(|&x| -x).collect();

            let v_q = tq.quantize(&v, &mut buf);
            let neg_q = tq.quantize(&neg_v, &mut buf);

            let self_score = asymmetric_score_helper(&tq, &v, &v_q);
            let anti_score = asymmetric_score_helper(&tq, &v, &neg_q);

            assert!(
                self_score.is_finite() && anti_score.is_finite(),
                "non-finite score for {bits:?}/{distance:?}: self={self_score}, anti={anti_score}",
            );
            assert!(
                self_score > 0.0,
                "self-similarity should be positive for {bits:?}/{distance:?}, got {self_score}",
            );
            assert!(
                anti_score < 0.0,
                "antipodal score should be negative for {bits:?}/{distance:?}, got {anti_score}",
            );
            // Gap must be large relative to the score magnitudes — guards
            // against a constant-output or sign-swapped dispatch bug.
            let gap = self_score - anti_score;
            let ref_mag = self_score.abs().max(anti_score.abs());
            assert!(
                gap > ref_mag,
                "score spread too small for {bits:?}/{distance:?}: self={self_score}, anti={anti_score}",
            );
        }
    }
}
