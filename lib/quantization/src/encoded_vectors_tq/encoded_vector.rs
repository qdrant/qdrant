use super::codebook::Codebook;
use super::packing::{pack_indices, unpack_index};
use super::qjl::QjlProjection;
use super::rotation::RotationImpl;
use super::TqCorrection;

/// Size of the stored norm (f32) in bytes.
pub(crate) const NORM_SIZE: usize = size_of::<f32>();

/// A view into a single TurboQuant-encoded vector's bytes.
///
/// Layout without QJL:
///   `[norm: f32 (4 bytes)] [packed indices: ⌈padded_dim·bits/8⌉ bytes]`
///
/// Layout with QJL:
///   `[norm: 4B] [packed indices] [residual_norm: 4B] [qjl_bits: ⌈padded_dim/8⌉ B]`
pub struct EncodedVectorTQ<'a> {
    bytes: &'a [u8],
}

impl<'a> EncodedVectorTQ<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// The stored L2 norm of the original (pre-rotation) vector.
    pub fn norm(&self) -> f32 {
        f32::from_ne_bytes(self.bytes[..NORM_SIZE].try_into().unwrap())
    }

    /// The packed quantization indices (everything after the norm).
    /// Safe even when QJL data follows: unpack_index only reads the bits it needs.
    pub fn packed_data(&self) -> &[u8] {
        &self.bytes[NORM_SIZE..]
    }

    /// The residual norm stored after packed indices (only when QJL is active).
    pub(super) fn residual_norm(&self, codebook_packed_size: usize) -> f32 {
        let offset = NORM_SIZE + codebook_packed_size;
        f32::from_ne_bytes(self.bytes[offset..offset + NORM_SIZE].try_into().unwrap())
    }

    /// The packed QJL sign bits (only when QJL is active).
    pub(super) fn qjl_bits(&self, codebook_packed_size: usize) -> &[u8] {
        let offset = NORM_SIZE + codebook_packed_size + NORM_SIZE;
        &self.bytes[offset..]
    }

    /// The precomputed norm of the decoded centroid vector (Normalization mode only).
    /// Stored after packed indices: `[norm][packed][centroid_norm]`.
    pub(super) fn centroid_norm(&self, codebook_packed_size: usize) -> f32 {
        let offset = NORM_SIZE + codebook_packed_size;
        f32::from_ne_bytes(self.bytes[offset..offset + NORM_SIZE].try_into().unwrap())
    }

    /// Compute dot product on the fly using shared codebook centroids.
    /// `dot = Σ_i effective_query[i] * centroids[unpack_index(packed, i, bits)]`
    /// Zero allocation. Codebook centroids (K values) stay hot in cache.
    pub(super) fn codebook_dot(
        &self,
        effective_query: &[f32],
        codebook: &Codebook,
    ) -> f32 {
        let packed = self.packed_data();
        let bits = codebook.bits;
        let centroids = &codebook.centroids;
        let mut sum = 0.0f32;
        for (i, &eq) in effective_query.iter().enumerate() {
            let idx = unpack_index(packed, i, bits) as usize;
            sum += eq * centroids[idx];
        }
        sum
    }

    /// Decode into (norm, corrected centroid vector in rotated space).
    ///
    /// Decoding order:
    /// 1. Decode raw centroids from codebook
    /// 2. If QJL: add correction in quantized space (before undoing shift+scale)
    /// 3. If TQ+: undo shift+scale
    /// 4. If Normalization or QjlNormalization: normalize to unit
    pub(super) fn decode_rotated(
        &self,
        codebook: &Codebook,
        padded_dim: usize,
        medians: &[f32],
        scales: &[f32],
        correction: TqCorrection,
        qjl: Option<&QjlProjection>,
    ) -> (f32, Vec<f32>) {
        let norm = self.norm();
        let packed = self.packed_data();

        // Step 1: Decode raw centroids
        let mut decoded = codebook.decode_raw(packed, padded_dim);

        // Step 2: Apply QJL correction in quantized space
        if let Some(qjl_proj) = qjl {
            let codebook_packed_size = codebook.packed_size(padded_dim);
            let res_norm = self.residual_norm(codebook_packed_size);
            let qjl_packed = self.qjl_bits(codebook_packed_size);
            let correction_vec = qjl_proj.decode_correction(qjl_packed, res_norm);
            for (d, c) in decoded.iter_mut().zip(correction_vec.iter()) {
                *d += c;
            }
        }

        // Step 3: Undo shift+scale if TQ+
        if !medians.is_empty() {
            for (i, d) in decoded.iter_mut().enumerate() {
                *d = *d / scales[i] + medians[i];
            }
        }

        // Step 4: Normalize if applicable
        match correction {
            TqCorrection::Normalization
            | TqCorrection::QjlNormalization
            | TqCorrection::QjlShortNormalization => {
                Codebook::normalize_to_unit(&mut decoded);
            }
            TqCorrection::NoCorrection | TqCorrection::Qjl | TqCorrection::QjlShort => {}
        }

        (norm, decoded)
    }

    /// Dot product score: γ · ⟨Rq, ŷ⟩
    pub(super) fn score_dot(
        &self,
        rotated_query: &[f32],
        codebook: &Codebook,
        padded_dim: usize,
        medians: &[f32],
        scales: &[f32],
        correction: TqCorrection,
        qjl: Option<&QjlProjection>,
    ) -> f32 {
        let (gamma, y_corrected) =
            self.decode_rotated(codebook, padded_dim, medians, scales, correction, qjl);
        let dot: f32 = rotated_query
            .iter()
            .zip(y_corrected.iter())
            .map(|(&q, &v)| q * v)
            .sum();
        gamma * dot
    }

    /// L2 score: ||q||² + γ² − 2γ⟨Rq, ŷ⟩
    pub(super) fn score_l2(
        &self,
        rotated_query: &[f32],
        query_norm_sq: f32,
        codebook: &Codebook,
        padded_dim: usize,
        medians: &[f32],
        scales: &[f32],
        correction: TqCorrection,
        qjl: Option<&QjlProjection>,
    ) -> f32 {
        let (gamma, y_corrected) =
            self.decode_rotated(codebook, padded_dim, medians, scales, correction, qjl);
        let dot: f32 = rotated_query
            .iter()
            .zip(y_corrected.iter())
            .map(|(&q, &v)| q * v)
            .sum();
        query_norm_sq + gamma * gamma - 2.0 * gamma * dot
    }

    /// L1 score requires full reconstruction via inverse rotation.
    pub(super) fn score_l1(
        &self,
        original_query: &[f32],
        codebook: &Codebook,
        rotation_impl: &RotationImpl,
        dim: usize,
        medians: &[f32],
        scales: &[f32],
        correction: TqCorrection,
        qjl: Option<&QjlProjection>,
    ) -> f32 {
        let padded_dim = rotation_impl.padded_dim();
        let (gamma, y_corrected) =
            self.decode_rotated(codebook, padded_dim, medians, scales, correction, qjl);
        let x_hat = rotation_impl.apply_inverse(&y_corrected, dim);
        original_query
            .iter()
            .zip(x_hat.iter())
            .map(|(&q, &x)| (q - gamma * x).abs())
            .sum()
    }
}

/// Encode a vector into TurboQuant binary format.
///
/// Layout without QJL:
///   `[norm: f32 (4 bytes)] [packed indices: ⌈padded_dim·bits/8⌉ bytes]`
///
/// Layout with QJL:
///   `[norm: 4B] [packed indices] [residual_norm: 4B] [qjl_bits: ⌈padded_dim/8⌉ B]`
///
/// When `medians`/`scales` are non-empty (TQ+ mode), each rotated coordinate
/// is shifted and scaled before quantization: `(rotated[i] - median[i]) * scale[i]`.
pub(super) fn encode_vector_data(
    vector_data: &[f32],
    rotation: &RotationImpl,
    codebook: &Codebook,
    medians: &[f32],
    scales: &[f32],
    correction: TqCorrection,
    qjl: Option<&QjlProjection>,
) -> Vec<u8> {
    // 1. Extract norm
    let norm_sq: f32 = vector_data.iter().map(|&x| x * x).sum();
    let norm = norm_sq.sqrt();

    // 2. Normalize (handle zero vector)
    let normalized: Vec<f32> = if norm > 0.0 {
        vector_data.iter().map(|&x| x / norm).collect()
    } else {
        vec![0.0; vector_data.len()]
    };

    // 3. Rotate
    let mut rotated = rotation.apply(&normalized);

    // 4. TQ+: shift and scale to match codebook distribution
    if !medians.is_empty() {
        for (i, val) in rotated.iter_mut().enumerate() {
            *val = (*val - medians[i]) * scales[i];
        }
    }

    // 5. Quantize each coordinate to nearest centroid
    let indices: Vec<u8> = rotated.iter().map(|&val| codebook.quantize(val)).collect();

    // 6. Pack codebook indices
    let packed = pack_indices(&indices, codebook.bits);

    // 7. Compute QJL data if applicable
    let (qjl_norm_bytes, qjl_bits) = if let Some(qjl_proj) = qjl {
        // Residual = quantized-space vector - dequantized centroids
        let residual: Vec<f32> = rotated
            .iter()
            .zip(indices.iter())
            .map(|(&r, &idx)| r - codebook.centroid(idx))
            .collect();
        let (res_norm, bits) = qjl_proj.encode_residual(&residual);
        (Some(res_norm.to_ne_bytes()), Some(bits))
    } else {
        (None, None)
    };

    // 8. Compute centroid norm for Normalization mode
    let centroid_norm_bytes = if matches!(correction, TqCorrection::Normalization) {
        let centroid_norm_sq: f32 = if medians.is_empty() {
            indices
                .iter()
                .map(|&idx| {
                    let c = codebook.centroid(idx);
                    c * c
                })
                .sum()
        } else {
            indices
                .iter()
                .enumerate()
                .map(|(i, &idx)| {
                    let c = codebook.centroid(idx);
                    let decoded = c / scales[i] + medians[i];
                    decoded * decoded
                })
                .sum()
        };
        Some(centroid_norm_sq.sqrt().to_ne_bytes())
    } else {
        None
    };

    // 9. Pack everything
    let mut result = Vec::with_capacity(
        NORM_SIZE
            + packed.len()
            + centroid_norm_bytes.as_ref().map_or(0, |b| b.len())
            + qjl_norm_bytes.as_ref().map_or(0, |b| b.len())
            + qjl_bits.as_ref().map_or(0, |b| b.len()),
    );
    result.extend_from_slice(&norm.to_ne_bytes());
    result.extend_from_slice(&packed);
    if let Some(cn_bytes) = centroid_norm_bytes {
        result.extend_from_slice(&cn_bytes);
    }
    if let (Some(norm_bytes), Some(bits)) = (qjl_norm_bytes, qjl_bits) {
        result.extend_from_slice(&norm_bytes);
        result.extend_from_slice(&bits);
    }
    result
}

/// Normalize a vector and apply rotation; return the rotated coordinates.
pub(super) fn normalize_and_rotate(v: &[f32], rotation: &RotationImpl) -> Vec<f32> {
    let norm_sq: f32 = v.iter().map(|&x| x * x).sum();
    let norm = norm_sq.sqrt();
    let normalized: Vec<f32> = if norm > 0.0 {
        v.iter().map(|&x| x / norm).collect()
    } else {
        vec![0.0; v.len()]
    };
    rotation.apply(&normalized)
}
