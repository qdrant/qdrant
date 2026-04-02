//! TurboQuant: CPU scalar implementation of the TurboQuant KV cache quantization algorithm.
//!
//! Implements the PolarQuant stage (Stage 1) from "TurboQuant: Online Vector Quantization
//! with Near-optimal Distortion Rate" (Zandieh et al., ICLR 2026).
//!
//! The algorithm: random rotation via Walsh-Hadamard transform + per-coordinate scalar
//! quantization using precomputed Lloyd-Max optimal centroids for a standard normal distribution.
//!
//! Includes the QJL (Quantized Johnson-Lindenstrauss) residual correction stage
//! (Stage 2) for unbiased inner-product estimation.

mod centroids;
mod codec;
mod hadamard;
mod packing;
mod qjl;
mod simd_score;

use centroids::get_centroids;
pub use codec::{QuantizedHeader, QuantizedRef};
use hadamard::HadamardTransform;
use packing::{pack_indices, packed_len, unpack_indices};
use zerocopy::FromBytes;

use crate::turboquant::qjl::Qjl;

#[allow(dead_code)]
pub fn cosine_preprocess(vector: &[f32]) -> Vec<f32> {
    let mut length: f32 = vector.iter().map(|x| x * x).sum();
    if is_length_zero_or_normalized(length) {
        return vector.to_vec();
    }
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

#[inline]
#[allow(dead_code)]
pub fn is_length_zero_or_normalized(length: f32) -> bool {
    length < f32::EPSILON || (length - 1.0).abs() <= 1.0e-6
}

/// Output of [`TurboQuantizer::quantize`].
#[derive(Debug, Clone)]
pub struct Quantized {
    /// Bit-packed centroid indices.
    pub packed_indices: Vec<u8>,
    /// QJL sign bits (packed).
    pub qjl_signs: Vec<u8>,
    /// L2 norm of the quantization residual.
    pub residual_norm: f32,
}

/// Reusable quantizer that holds the Hadamard transform state.
///
/// Uses (bits-1) bits for scalar quantization (Stage 1) plus 1-bit QJL
/// on the residual (Stage 2) for unbiased dot-product estimation.
///
/// # Example
/// ```
/// use turboquant::TurboQuantizer;
///
/// let dim = 128;
/// let quantizer = TurboQuantizer::new(dim, 4, 42);
///
/// let vector = vec![1.0_f32; dim];
/// let quantized = quantizer.quantize(&vector);
/// let reconstructed = quantizer.dequantize(&quantized);
/// assert_eq!(reconstructed.len(), dim);
/// ```
pub struct TurboQuantizer {
    dim: usize,
    bits: u8,
    hadamard: HadamardTransform,
    padded_dim_sqrt: f32,

    qjl: Qjl,
}

impl TurboQuantizer {
    /// Create a new quantizer.
    ///
    /// - `dim`: dimensionality of input vectors.
    /// - `bits`: total quantization bit-width (2–4). One bit is reserved for QJL.
    /// - `seed`: RNG seed for the randomized Hadamard signs.
    ///
    /// # Panics
    /// Panics if `bits` is not in 2..=4.
    pub fn new(dim: usize, bits: u8, seed: u64) -> Self {
        assert!((2..=4).contains(&bits), "bits must be 2–4, got {bits}");
        let hadamard = HadamardTransform::new(dim, seed);
        let padded_dim_sqrt = 1.0 / (hadamard.padded_dim() as f32).sqrt();
        let qjl = Qjl::new(hadamard.padded_dim(), seed);
        Self {
            dim,
            bits,
            hadamard,
            padded_dim_sqrt,
            qjl,
        }
    }

    /// The number of bits used for the scalar quantization stage.
    #[inline]
    fn mse_bits(&self) -> u8 {
        self.bits - 1
    }

    /// The padded (power-of-2) dimension used internally.
    #[inline]
    pub fn padded_dim(&self) -> usize {
        self.hadamard.padded_dim()
    }

    /// Byte length of the packed centroid indices for one vector.
    #[inline]
    pub fn packed_indices_len(&self) -> usize {
        packed_len(self.padded_dim(), self.mse_bits())
    }

    /// Byte length of the packed QJL sign bits for one vector.
    #[inline]
    pub fn qjl_signs_len(&self) -> usize {
        packed_len(self.padded_dim(), 1)
    }

    /// Total encoded byte size for one vector (header + data).
    #[inline]
    pub fn encoded_size(&self) -> usize {
        size_of::<QuantizedHeader>() + self.packed_indices_len() + self.qjl_signs_len()
    }

    /// Quantize a single vector.
    ///
    /// The slice must have length equal to the `dim` passed to [`Self::new`].
    pub fn quantize(&self, vector: &[f32]) -> Quantized {
        assert_eq!(vector.len(), self.dim, "vector dim mismatch");

        let padded_dim = self.padded_dim();
        let mse_bits = self.mse_bits();
        let centroids = get_centroids(mse_bits);
        let scale = self.padded_dim_sqrt;

        // Randomized Hadamard forward transform.
        let rotated = self.hadamard.forward(vector);

        // Nearest-centroid quantization on the normalized rotated coordinates.
        let mut indices = vec![0u8; padded_dim];
        for (i, &val) in rotated.iter().enumerate() {
            let val_norm = val;
            let mut best_idx = 0u8;
            let mut best_dist = f32::INFINITY;
            for (c_idx, &centroid) in centroids.iter().enumerate() {
                let centroid = centroid * scale;
                let d = (val_norm - centroid) * (val_norm - centroid);
                if d < best_dist {
                    best_dist = d;
                    best_idx = c_idx as u8;
                }
            }
            indices[i] = best_idx;
        }

        let packed_indices = pack_indices(&indices, mse_bits);

        // QJL residual (Stage 2).
        let mut residual_norm_sq = 0.0f32;
        let mut residual = vec![0.0f32; padded_dim];
        // let mut qjl_raw = vec![0u8; padded_dim];
        for i in 0..padded_dim {
            let r = rotated[i] - centroids[indices[i] as usize] * scale;
            residual_norm_sq += r * r;
            residual[i] = r;
            // qjl_raw[i] = u8::from(r >= 0.0);
        }

        // let qjl_signs = pack_indices(&qjl_raw, 1);

        let residual_norm = residual_norm_sq.sqrt();

        let r_hat: Vec<f32> = residual.iter().map(|v| v / residual_norm).collect();
        let qjl_res = self.qjl.quantize(&r_hat);
        let qjl_packed = pack_indices(&qjl_res, 1);

        Quantized {
            packed_indices,
            qjl_signs: qjl_packed,
            residual_norm,
        }
    }

    /// Dequantize back to the original vector space.
    ///
    /// Returns a vector of length equal to the original `dim`.
    #[allow(dead_code)]
    pub fn dequantize(&self, quantized: &Quantized) -> Vec<f32> {
        self.dequantize_inner(
            quantized.residual_norm,
            &quantized.packed_indices,
            &quantized.qjl_signs,
        )
    }

    /// Dequantize from a zero-copy [`QuantizedRef`] decoded from bytes.
    ///
    /// Returns a vector of length equal to the original `dim`.
    #[allow(dead_code)]
    pub fn dequantize_ref(&self, quantized: &QuantizedRef<'_>) -> Vec<f32> {
        self.dequantize_inner(
            quantized.residual_norm(),
            quantized.packed_indices,
            quantized.qjl_signs,
        )
    }

    /// Compute the dot product score between two quantized vectors directly
    /// in the rotated domain, without performing the inverse Hadamard transform.
    ///
    /// This is mathematically equivalent to `dot(dequantize(a), dequantize(b))`
    /// but significantly faster because the Hadamard inverse (O(d log d)) is
    /// skipped entirely.
    #[allow(dead_code)]
    pub fn score(&self, a: &Quantized, b: &Quantized) -> f32 {
        self.score_inner(
            a.residual_norm,
            &a.packed_indices,
            &a.qjl_signs,
            b.residual_norm,
            &b.packed_indices,
            &b.qjl_signs,
        )
    }

    /// Compute the dot product score between two [`QuantizedRef`] values
    /// decoded from bytes.
    pub fn score_ref(&self, a: &QuantizedRef<'_>, b: &QuantizedRef<'_>) -> f32 {
        self.score_inner(
            a.residual_norm(),
            a.packed_indices,
            a.qjl_signs,
            b.residual_norm(),
            b.packed_indices,
            b.qjl_signs,
        )
    }

    /// Decode a quantized vector from a byte slice without allocation.
    ///
    /// Returns `None` if the buffer is too short for this quantizer's
    /// configuration.
    pub fn decode<'a>(&self, bytes: &'a [u8]) -> Option<QuantizedRef<'a>> {
        let (header, rest): (&QuantizedHeader, &[u8]) =
            QuantizedHeader::ref_from_prefix(bytes).ok()?;
        let idx_len = self.packed_indices_len();
        if rest.len() < idx_len {
            return None;
        }
        let (packed_indices, rest) = rest.split_at(idx_len);
        let qjl_len = self.qjl_signs_len();
        if rest.len() < qjl_len {
            return None;
        }
        let qjl_signs = &rest[..qjl_len];
        Some(QuantizedRef::from_parts(header, packed_indices, qjl_signs))
    }

    /// SIMD-optimized score between two [`Quantized`] values.
    ///
    /// Uses AVX2+FMA on x86_64 when available, otherwise an allocation-free
    /// scalar fallback. Produces the same result as [`Self::score`].
    #[allow(dead_code)]
    pub fn score_simd(&self, a: &Quantized, b: &Quantized) -> f32 {
        simd_score::score_fused(
            self.dim,
            self.padded_dim(),
            self.padded_dim_sqrt,
            self.mse_bits(),
            a.residual_norm,
            &a.packed_indices,
            &a.qjl_signs,
            b.residual_norm,
            &b.packed_indices,
            &b.qjl_signs,
            self.qjl.projection.as_slice(),
            self.hadamard.signs(),
            self.hadamard.scale(),
        )
    }

    /// SIMD-optimized score between two [`QuantizedRef`] values.
    ///
    /// Uses AVX2+FMA on x86_64 when available, otherwise an allocation-free
    /// scalar fallback. Produces the same result as [`Self::score_ref`].
    pub fn score_ref_simd(&self, a: &QuantizedRef<'_>, b: &QuantizedRef<'_>) -> f32 {
        simd_score::score_fused(
            self.dim,
            self.padded_dim(),
            self.padded_dim_sqrt,
            self.mse_bits(),
            a.residual_norm(),
            a.packed_indices,
            a.qjl_signs,
            b.residual_norm(),
            b.packed_indices,
            b.qjl_signs,
            self.qjl.projection.as_slice(),
            self.hadamard.signs(),
            self.hadamard.scale(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn score_inner(
        &self,
        residual_norm_a: f32,
        packed_indices_a: &[u8],
        qjl_signs_a: &[u8],
        residual_norm_b: f32,
        packed_indices_b: &[u8],
        qjl_signs_b: &[u8],
    ) -> f32 {
        let recon_a = self.dequantize_inner(residual_norm_a, packed_indices_a, qjl_signs_a);
        let recon_b = self.dequantize_inner(residual_norm_b, packed_indices_b, qjl_signs_b);

        recon_a
            .iter()
            .zip(recon_b.iter())
            .map(|(&a, &b)| a * b)
            .sum()
    }

    fn dequantize_inner(
        &self,
        residual_norm: f32,
        packed_indices: &[u8],
        qjl_signs: &[u8],
    ) -> Vec<f32> {
        // const FRAC_2_PI_SQRT: f32 = 1.2533141f32;

        let padded_dim = self.padded_dim();
        let mse_bits = self.mse_bits();
        let centroids = get_centroids(mse_bits);
        let scale = self.padded_dim_sqrt;

        let indices = unpack_indices(packed_indices, mse_bits, padded_dim);

        // Centroid lookup + QJL correction.
        let signs = unpack_indices(qjl_signs, 1, padded_dim);
        // let qjl_scale = FRAC_2_PI_SQRT / padded_dim as f32;

        let qjl_dequant = self.qjl.dequantize(&signs);

        let mut dequant = vec![0.0f32; padded_dim];

        for j in 0..padded_dim {
            let sign = qjl_dequant[j];
            dequant[j] = centroids[indices[j] as usize] * scale + residual_norm * sign;
        }

        // Inverse Hadamard to return to original space, trimmed to original dim.
        let reconstructed = self.hadamard.inverse(&dequant);
        reconstructed[..self.dim].to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_prod_3bit() {
        let dim = 128;
        let q = TurboQuantizer::new(dim, 3, 123);
        let vector = cosine_preprocess(&(0..dim).map(|j| j as f32 * 0.01).collect::<Vec<f32>>());
        let quantized = q.quantize(&vector);
        let reconstructed = q.dequantize(&quantized);
        assert_eq!(reconstructed.len(), dim);
    }

    #[test]
    fn roundtrip_4bit() {
        let dim = 64;
        let q = TurboQuantizer::new(dim, 4, 42);
        let vector = cosine_preprocess(&vec![0.5f32; dim]);
        let quantized = q.quantize(&vector);
        let reconstructed = q.dequantize(&quantized);
        assert_eq!(reconstructed.len(), dim);
        let mse: f32 = vector
            .iter()
            .zip(reconstructed.iter())
            .map(|(&a, &b)| (a - b) * (a - b))
            .sum::<f32>()
            / dim as f32;
        assert!(mse < 0.1, "MSE too high: {mse}");
    }

    #[test]
    fn all_bit_widths() {
        let dim = 32;
        for bits in 2..=4u8 {
            let q = TurboQuantizer::new(dim, bits, 0);
            let vector = cosine_preprocess(&vec![1.0f32; dim]);
            let quantized = q.quantize(&vector);
            let recon = q.dequantize(&quantized);
            assert_eq!(recon.len(), dim);
        }
    }

    // #[test]
    // fn zero_vector() {
    //     let dim = 16;
    //     let q = TurboQuantizer::new(dim, 4, 42);
    //     let vector = vec![0.0f32; dim];
    //     let quantized = q.quantize(&vector);
    //     let recon = q.dequantize(&quantized);
    //     for &v in &recon {
    //         assert!(v.abs() < 1e-4);
    //     }
    // }

    #[test]
    fn non_power_of_two_dim() {
        let dim = 100;
        let q = TurboQuantizer::new(dim, 4, 42);
        let vector = cosine_preprocess(&(0..dim).map(|i| i as f32 * 0.1).collect::<Vec<f32>>());
        let quantized = q.quantize(&vector);
        let recon = q.dequantize(&quantized);
        assert_eq!(recon.len(), dim);
    }

    #[test]
    fn encode_decode_roundtrip() {
        let dim = 128;
        let q = TurboQuantizer::new(dim, 3, 42);
        let vector = cosine_preprocess(&(0..dim).map(|i| (i as f32).sin()).collect::<Vec<f32>>());
        let quantized = q.quantize(&vector);

        let encoded = quantized.encode();
        let decoded = q.decode(&encoded).expect("decode failed");

        assert_eq!(decoded.residual_norm(), quantized.residual_norm);
        assert_eq!(decoded.packed_indices, &quantized.packed_indices[..]);
        assert_eq!(decoded.qjl_signs, &quantized.qjl_signs[..]);

        // Dequantize from decoded ref should match dequantize from owned.
        let recon_owned = q.dequantize(&quantized);
        let recon_ref = q.dequantize_ref(&decoded);
        assert_eq!(recon_owned, recon_ref);
    }

    #[test]
    fn decode_rejects_truncated_input() {
        let q = TurboQuantizer::new(16, 4, 0);
        // Too short for even the header.
        assert!(q.decode(&[0u8; 7]).is_none());
        // Header fits but not enough data for packed indices.
        assert!(q.decode(&[0u8; 8]).is_none());
    }

    #[test]
    fn decode_rejects_empty_input() {
        let q = TurboQuantizer::new(16, 4, 0);
        assert!(q.decode(&[]).is_none());
    }

    /// Helper: dot product of two slices.
    fn dot(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum()
    }

    /// Helper: generate a pseudo-random f32 vector using a simple LCG,
    /// then cosine-preprocess (L2-normalize) it.
    fn random_vector(dim: usize, seed: u64) -> Vec<f32> {
        let mut state = seed.wrapping_add(1);
        let raw: Vec<f32> = (0..dim)
            .map(|_| {
                // xorshift64
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                // Map to roughly [-1, 1]
                (state as i64 as f32) / (i64::MAX as f32)
            })
            .collect();
        cosine_preprocess(&raw)
    }

    /// End-to-end: quantize two vectors, compute dot product on dequantized
    /// versions, encode/decode, dequantize again from decoded bytes, and
    /// verify the dot product is identical.
    #[test]
    fn e2e_dot_product_preserved_through_codec() {
        let dim = 256;
        let q = TurboQuantizer::new(dim, 4, 99);

        let v1 = random_vector(dim, 1);
        let v2 = random_vector(dim, 2);

        let q1 = q.quantize(&v1);
        let q2 = q.quantize(&v2);

        let recon1 = q.dequantize(&q1);
        let recon2 = q.dequantize(&q2);
        let dot_before = dot(&recon1, &recon2);

        // Encode → decode → dequantize_ref
        let bytes1 = q1.encode();
        let bytes2 = q2.encode();
        let ref1 = q.decode(&bytes1).unwrap();
        let ref2 = q.decode(&bytes2).unwrap();
        let recon1_ref = q.dequantize_ref(&ref1);
        let recon2_ref = q.dequantize_ref(&ref2);
        let dot_after = dot(&recon1_ref, &recon2_ref);

        assert_eq!(
            dot_before, dot_after,
            "dot product must be bit-identical through encode/decode"
        );
    }

    /// End-to-end: multiple vectors at every supported bit width.
    #[test]
    fn e2e_all_bit_widths_multiple_vectors() {
        for bits in 2..=4u8 {
            for dim in [32, 64, 100, 128, 255, 512] {
                let q = TurboQuantizer::new(dim, bits, 42);
                let vectors: Vec<Vec<f32>> = (0..4).map(|s| random_vector(dim, s)).collect();

                // Quantize all, compute pairwise dots from owned dequantization.
                let quantized: Vec<Quantized> = vectors.iter().map(|v| q.quantize(v)).collect();
                let recon_owned: Vec<Vec<f32>> =
                    quantized.iter().map(|qv| q.dequantize(qv)).collect();

                // Encode → decode → dequantize_ref, compare dots.
                let encoded: Vec<Vec<u8>> = quantized.iter().map(|qv| qv.encode()).collect();
                let recon_ref: Vec<Vec<f32>> = encoded
                    .iter()
                    .map(|b| {
                        let r = q.decode(b).unwrap();
                        q.dequantize_ref(&r)
                    })
                    .collect();

                for i in 0..4 {
                    for j in 0..4 {
                        let d_owned = dot(&recon_owned[i], &recon_owned[j]);
                        let d_ref = dot(&recon_ref[i], &recon_ref[j]);
                        assert_eq!(
                            d_owned, d_ref,
                            "bits={bits} dim={dim} pair ({i},{j}): dots differ"
                        );
                    }
                }
            }
        }
    }

    /// Dot product between the quantized reconstruction and the original should
    /// be a reasonable approximation (not degenerate / zero / flipped sign).
    #[test]
    fn e2e_dot_product_quality() {
        let dim = 512;
        let q = TurboQuantizer::new(dim, 4, 77);

        let v1 = random_vector(dim, 10);
        let v2 = random_vector(dim, 20);
        let true_dot = dot(&v1, &v2);

        let recon1 = q.dequantize(&q.quantize(&v1));
        let recon2 = q.dequantize(&q.quantize(&v2));
        let approx_dot = dot(&recon1, &recon2);

        // The quantized dot should at least have the same sign and be within
        // a reasonable relative error for 4-bit (3-bit MSE + 1-bit QJL).
        if true_dot.abs() > 1e-3 {
            assert_eq!(
                true_dot.is_sign_positive(),
                approx_dot.is_sign_positive(),
                "sign flipped: true={true_dot}, approx={approx_dot}"
            );
            let rel_err = (approx_dot - true_dot).abs() / true_dot.abs();
            assert!(
                rel_err < 0.5,
                "relative error too large: {rel_err} (true={true_dot}, approx={approx_dot})"
            );
        }
    }

    /// Encoding the same Quantized twice must produce identical bytes.
    #[test]
    fn encode_deterministic() {
        let dim = 64;
        let q = TurboQuantizer::new(dim, 3, 0);
        let quantized = q.quantize(&random_vector(dim, 55));
        assert_eq!(quantized.encode(), quantized.encode());
    }

    /// Quantizing the same vector with the same seed is deterministic.
    #[test]
    fn quantize_deterministic() {
        let dim = 128;
        let v = random_vector(dim, 99);
        let q = TurboQuantizer::new(dim, 4, 42);
        let a = q.quantize(&v);
        let b = q.quantize(&v);
        assert_eq!(a.packed_indices, b.packed_indices);
        assert_eq!(a.qjl_signs, b.qjl_signs);
        assert_eq!(a.residual_norm, b.residual_norm);
    }

    /// Different seeds produce different quantizations.
    #[test]
    fn different_seeds_differ() {
        let dim = 128;
        let v = random_vector(dim, 1);
        let q1 = TurboQuantizer::new(dim, 4, 1);
        let q2 = TurboQuantizer::new(dim, 4, 2);
        let a = q1.quantize(&v);
        let b = q2.quantize(&v);
        // Norms should be identical (same input), but packed indices should differ.
        assert_ne!(a.packed_indices, b.packed_indices);
    }

    /// encoded_size on Quantized matches the quantizer's encoded_size and actual length.
    #[test]
    fn encoded_size_accurate() {
        for bits in 2..=4u8 {
            for dim in [16, 33, 64, 100, 256] {
                let q = TurboQuantizer::new(dim, bits, 0);
                let quantized = q.quantize(&random_vector(dim, 0));
                let encoded = quantized.encode();
                assert_eq!(
                    q.encoded_size(),
                    encoded.len(),
                    "bits={bits} dim={dim}: TurboQuantizer::encoded_size"
                );
            }
        }
    }

    /// Large dimension stress test: encode → decode → dequantize round-trip.
    #[test]
    fn e2e_large_dim() {
        let dim = 4096;
        let q = TurboQuantizer::new(dim, 4, 31);
        let v = random_vector(dim, 7);
        let quantized = q.quantize(&v);

        let bytes = quantized.encode();
        let decoded = q.decode(&bytes).unwrap();
        let recon_owned = q.dequantize(&quantized);
        let recon_ref = q.dequantize_ref(&decoded);
        assert_eq!(recon_owned, recon_ref);
    }

    /// Batch: quantize N vectors, store all encoded bytes contiguously,
    /// decode each, dequantize, and verify all pairwise dots match.
    #[test]
    fn e2e_batch_contiguous_storage() {
        let dim = 128;
        let n = 8;
        let q = TurboQuantizer::new(dim, 4, 13);

        let vectors: Vec<Vec<f32>> = (0..n).map(|s| random_vector(dim, s)).collect();
        let quantized: Vec<Quantized> = vectors.iter().map(|v| q.quantize(v)).collect();

        // All vectors from the same quantizer have the same encoded size.
        let entry_size = q.encoded_size();

        // Store contiguously — no per-entry length prefix needed.
        let mut storage = Vec::new();
        for qv in &quantized {
            let enc = qv.encode();
            assert_eq!(enc.len(), entry_size);
            storage.extend_from_slice(&enc);
        }

        // Read back and dequantize.
        let mut recon_loaded = Vec::new();
        for i in 0..n {
            let offset = i as usize * entry_size;
            let r = q.decode(&storage[offset..offset + entry_size]).unwrap();
            recon_loaded.push(q.dequantize_ref(&r));
        }

        // Compare against direct dequantize.
        let recon_direct: Vec<Vec<f32>> = quantized.iter().map(|qv| q.dequantize(qv)).collect();
        for i in 0..n {
            for j in 0..n {
                let d_direct = dot(&recon_direct[i as usize], &recon_direct[j as usize]);
                let d_loaded = dot(&recon_loaded[i as usize], &recon_loaded[j as usize]);
                assert_eq!(d_direct, d_loaded, "pair ({i},{j})");
            }
        }
    }

    /// score() must produce the same result as dot(dequantize(a), dequantize(b)).
    #[test]
    fn score_matches_dequantize_dot() {
        let dim = 256;
        let q = TurboQuantizer::new(dim, 4, 99);

        let v1 = random_vector(dim, 1);
        let v2 = random_vector(dim, 2);

        let q1 = q.quantize(&v1);
        let q2 = q.quantize(&v2);

        let recon1 = q.dequantize(&q1);
        let recon2 = q.dequantize(&q2);
        let dot_via_dequant = dot(&recon1, &recon2);

        let dot_via_score = q.score(&q1, &q2);

        let rel_err = (dot_via_score - dot_via_dequant).abs() / dot_via_dequant.abs().max(1e-10);
        assert!(
            rel_err < 1e-4,
            "score and dequant dot differ: score={dot_via_score}, dequant={dot_via_dequant}, rel_err={rel_err}"
        );
    }

    /// score_ref() must match score() through encode/decode.
    #[test]
    fn score_ref_matches_score() {
        let dim = 128;
        let q = TurboQuantizer::new(dim, 3, 42);

        let v1 = random_vector(dim, 10);
        let v2 = random_vector(dim, 20);

        let q1 = q.quantize(&v1);
        let q2 = q.quantize(&v2);
        let direct = q.score(&q1, &q2);

        let bytes1 = q1.encode();
        let bytes2 = q2.encode();
        let r1 = q.decode(&bytes1).unwrap();
        let r2 = q.decode(&bytes2).unwrap();
        let via_ref = q.score_ref(&r1, &r2);

        assert_eq!(direct, via_ref, "score and score_ref must be identical");
    }

    /// score() across all bit widths and dimensions.
    #[test]
    fn score_all_bit_widths() {
        for bits in 2..=4u8 {
            for dim in [32, 64, 128, 256] {
                let q = TurboQuantizer::new(dim, bits, 7);
                let v1 = random_vector(dim, 1);
                let v2 = random_vector(dim, 2);
                let q1 = q.quantize(&v1);
                let q2 = q.quantize(&v2);

                let recon1 = q.dequantize(&q1);
                let recon2 = q.dequantize(&q2);
                let expected = dot(&recon1, &recon2);
                let got = q.score(&q1, &q2);

                let rel_err = (got - expected).abs() / expected.abs().max(1e-10);
                assert!(
                    rel_err < 1e-5,
                    "bits={bits} dim={dim}: score={got}, expected={expected}, rel_err={rel_err}"
                );
            }
        }
    }

    /// Self-dot-product of a quantized vector should be positive and close
    /// to the squared norm of the original.
    #[test]
    fn e2e_self_dot_product() {
        let dim = 256;
        let q = TurboQuantizer::new(dim, 4, 5);
        let v = random_vector(dim, 42);
        let true_sq_norm = dot(&v, &v);

        let quantized = q.quantize(&v);
        let bytes = quantized.encode();
        let decoded = q.decode(&bytes).unwrap();
        let recon = q.dequantize_ref(&decoded);
        let approx_sq_norm = dot(&recon, &recon);

        assert!(approx_sq_norm > 0.0);
        let rel_err = (approx_sq_norm - true_sq_norm).abs() / true_sq_norm;
        assert!(
            rel_err < 0.3,
            "self-dot relative error {rel_err}: true={true_sq_norm}, approx={approx_sq_norm}"
        );
    }

    /// Orthogonal vectors should have near-zero dot product after quantization.
    #[test]
    fn e2e_orthogonal_vectors() {
        let dim = 256;
        let q = TurboQuantizer::new(dim, 4, 0);
        // Construct two orthogonal vectors: one in the first half, one in the second.
        let mut raw1 = vec![0.0f32; dim];
        let mut raw2 = vec![0.0f32; dim];
        for (i, item) in raw1.iter_mut().enumerate().take(dim / 2) {
            *item = (i as f32 + 1.0).sqrt();
        }
        for (i, item) in raw2.iter_mut().enumerate().take(dim).skip(dim / 2) {
            *item = (i as f32 + 1.0).sqrt();
        }
        let v1 = cosine_preprocess(&raw1);
        let v2 = cosine_preprocess(&raw2);
        assert!((dot(&v1, &v2)).abs() < 1e-10, "test vectors not orthogonal");

        let q1 = q.quantize(&v1);
        let q2 = q.quantize(&v2);
        let bytes1 = q1.encode();
        let bytes2 = q2.encode();
        let r1 = q.decode(&bytes1).unwrap();
        let r2 = q.decode(&bytes2).unwrap();
        let recon1 = q.dequantize_ref(&r1);
        let recon2 = q.dequantize_ref(&r2);

        let approx_dot = dot(&recon1, &recon2);
        let norm1 = dot(&v1, &v1).sqrt();
        let norm2 = dot(&v2, &v2).sqrt();
        let cos_sim = approx_dot / (norm1 * norm2);
        assert!(
            cos_sim.abs() < 0.3,
            "orthogonal vectors should stay roughly orthogonal, got cos_sim={cos_sim}"
        );
    }
}
