use std::borrow::Cow;
use std::f32::consts::PI;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use common::fs::atomic_save_json;
use common::mmap::MmapFlusher;
use common::typelevel::True;
use common::types::PointOffsetType;
use fs_err as fs;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::encoded_storage::{EncodedStorage, EncodedStorageBuilder};
use crate::encoded_vectors::{EncodedVectors, VectorParameters, validate_vector_parameters};
use crate::EncodingError;

/// Default l for the recursive polar decomposition
pub const DEFAULT_LEVELS: usize = 4;

/// Bits per angle at level 1 (range [0, 2π)).
const LEVEL1_BITS: u8 = 4;

/// Bits per angle at levels 2+ (range [0, π/2]).
const HIGHER_LEVEL_BITS: u8 = 2;

/// Number of codebook entries at level 1.
/// We bitshift by LEVEL1_BITS to get the number of entries: 2^4 = 16.
const LEVEL1_CODEBOOK_SIZE: usize = 1 << LEVEL1_BITS; // 16

/// Number of codebook entries at levels 2+.
const HIGHER_LEVEL_CODEBOOK_SIZE: usize = 1 << HIGHER_LEVEL_BITS; // 4

pub struct EncodedVectorsPolar<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: PolarMetadata,
    metadata_path: Option<PathBuf>,
    rotation_matrix: Vec<f32>,
}

/// Encoded queries are preconditioned (rotated) query vector.
pub struct EncodedQueryPolar {
    rotated_query: Vec<f32>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PolarMetadata {
    pub vector_parameters: VectorParameters,
    pub rotation_seed: u64,
    pub codebooks: Vec<Vec<f32>>,
    pub bits_per_level: Vec<u8>,
    pub levels: usize,
    pub padded_dim: usize, 
    pub original_dim: usize,
}

/// Generate a random orthogonal matrix of size `n x n` using QR decomposition
/// via modified Gram-Schmidt on random Gaussian columns.
/// Returns a row-major flattened Vec<f32>.
/// 
/// GS-algo is necessary for orthogonality
fn generate_rotation_matrix(n: usize, seed: u64) -> Vec<f32> {
    let mut rng = StdRng::seed_from_u64(seed);

    let mut cols: Vec<Vec<f32>> = (0..n)
        .map(|_| {
            (0..n)
                .map(|_| {
                    let u1: f32 = rng.random::<f32>().max(f32::EPSILON);
                    let u2: f32 = rng.random::<f32>();
                    (-2.0 * u1.ln()).sqrt() * (2.0 * PI * u2).cos()
                })
                .collect()
        })
        .collect();

    // Modified Gram-Schmidt
    for i in 0..n {
        // Normalize column i
        let norm: f32 = cols[i].iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > f32::EPSILON {
            for x in cols[i].iter_mut() {
                *x /= norm;
            }
        }
        // Orthogonalize subsequent columns
        for j in (i + 1)..n {
            let dot: f32 = cols[i].iter().zip(cols[j].iter()).map(|(a, b)| a * b).sum();
            for k in 0..n {
                cols[j][k] -= dot * cols[i][k];
            }
        }
    }

    // Convert to flat vector
    let mut matrix = vec![0.0f32; n * n];
    for row in 0..n {
        for col in 0..n {
            matrix[row * n + col] = cols[col][row];
        }
    }
    matrix
}

/// Multiply a vector by a row-major n×n rotation matrix: result = M * v
fn rotate_vector(matrix: &[f32], v: &[f32], n: usize) -> Vec<f32> {
    let mut result = vec![0.0f32; n];
    for row in 0..n {
        let row_start = row * n;
        let mut sum = 0.0f32;
        for col in 0..n {
            sum += matrix[row_start + col] * v[col];
        }
        result[row] = sum;
    }
    result
}


/// Build default codebooks for the given number of levels.
///
/// Level 1: uniform over [0, 2π) with LEVEL1_CODEBOOK_SIZE entries.
/// Levels 2+: optimal centroids over [0, π/2] for the sin^(2^(l-1)-1)(2θ) distribution,
/// approximated via numerical integration + Lloyd-Max for HIGHER_LEVEL_CODEBOOK_SIZE entries.
fn build_default_codebooks(levels: usize) -> Vec<Vec<f32>> {
    let mut codebooks = Vec::with_capacity(levels);

    // Level 1: uniform centroids over [0, 2π)
    let step = 2.0 * PI / LEVEL1_CODEBOOK_SIZE as f32;
    let level1: Vec<f32> = (0..LEVEL1_CODEBOOK_SIZE)
        .map(|i| (i as f32 + 0.5) * step)
        .collect();
    codebooks.push(level1);

    // Levels 2+: centroids for the concentrated sin^(2^(l-1)-1)(2θ) distribution over [0, π/2]
    for level_idx in 1..levels {
        let exponent = (1usize << level_idx) - 1; // 2^(l-1) - 1 where l is 1-indexed level = level_idx+1
        let codebook = compute_optimal_centroids(exponent, HIGHER_LEVEL_CODEBOOK_SIZE);
        codebooks.push(codebook);
    }

    codebooks
}

/// Compute optimal centroids for the distribution proportional to sin^exponent(2θ)
/// over [0, π/2] using Lloyd-Max iteration.
fn compute_optimal_centroids(exponent: usize, k: usize) -> Vec<f32> {
    let half_pi = PI / 2.0;
    let num_samples = 10_000;

    // Sample the CDF to find initial quantile-based centroids
    let mut cdf = Vec::with_capacity(num_samples + 1);
    let mut cumulative = 0.0f64;
    cdf.push(0.0f64);
    let dt = half_pi / num_samples as f64;
    for i in 0..num_samples {
        let theta = (i as f64 + 0.5) * dt;
        let pdf_val = (2.0 * theta).sin().powi(exponent as i32);
        cumulative += pdf_val * dt;
        cdf.push(cumulative);
    }
    // Normalize CDF
    let total = cumulative;
    if total < 1e-15 {
        // Degenerate case: uniform
        return (0..k).map(|i| (i as f32 + 0.5) * half_pi / k as f32).collect();
    }
    for v in cdf.iter_mut() {
        *v /= total;
    }

    // Initial centroids at quantile midpoints
    let mut centroids: Vec<f64> = (0..k)
        .map(|i| {
            let target = (i as f64 + 0.5) / k as f64;
            // Binary search in CDF
            let mut lo = 0usize;
            let mut hi = num_samples;
            while lo < hi {
                let mid = (lo + hi) / 2;
                if cdf[mid] < target {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            lo as f64 * dt
        })
        .collect();

    // Lloyd-Max iterations
    for _ in 0..50 {
        // Compute boundaries (midpoints between adjacent centroids)
        let mut boundaries = Vec::with_capacity(k + 1);
        boundaries.push(0.0f64);
        for i in 0..k - 1 {
            boundaries.push((centroids[i] + centroids[i + 1]) / 2.0);
        }
        boundaries.push(half_pi as f64);

        // Recompute centroids as weighted mean within each partition
        let fine_steps = 1000;
        for i in 0..k {
            let lo = boundaries[i];
            let hi = boundaries[i + 1];
            let step = (hi - lo) / fine_steps as f64;
            if step < 1e-15 {
                continue;
            }
            let mut numerator = 0.0f64;
            let mut denominator = 0.0f64;
            for j in 0..fine_steps {
                let theta = lo + (j as f64 + 0.5) * step;
                let w = (2.0 * theta).sin().powi(exponent as i32);
                numerator += theta * w * step;
                denominator += w * step;
            }
            if denominator > 1e-15 {
                centroids[i] = numerator / denominator;
            }
        }
    }

    centroids.iter().map(|&c| c as f32).collect()
}

// ---------------------------------------------------------------------------
// Bit packing utilities
// ---------------------------------------------------------------------------

/// Pack angle indices into a byte buffer.
///
/// Layout: level 1 angles first (4-bit each, 2 per byte), then levels 2+ (2-bit each, 4 per byte).
fn pack_angles(all_angles: &[Vec<u8>], bits_per_level: &[u8]) -> Vec<u8> {
    let total_bits: usize = all_angles
        .iter()
        .zip(bits_per_level.iter())
        .map(|(angles, &bits)| angles.len() * bits as usize)
        .sum();
    let total_bytes = (total_bits + 7) / 8;
    let mut packed = vec![0u8; total_bytes];

    let mut bit_offset = 0usize;
    for (level_angles, &bits) in all_angles.iter().zip(bits_per_level.iter()) {
        let mask = (1u8 << bits) - 1;
        for &idx in level_angles {
            let byte_pos = bit_offset / 8;
            let bit_pos = bit_offset % 8;
            let val = idx & mask;
            packed[byte_pos] |= val << bit_pos;
            // Handle spanning two bytes
            if bit_pos + bits as usize > 8 {
                packed[byte_pos + 1] |= val >> (8 - bit_pos);
            }
            bit_offset += bits as usize;
        }
    }
    packed
}

/// Unpack angle indices from a byte buffer.
///
/// Returns a Vec of Vecs, one per level, containing the angle indices.
fn unpack_angles(
    packed: &[u8],
    angles_per_level: &[usize],
    bits_per_level: &[u8],
) -> Vec<Vec<u8>> {
    let mut result = Vec::with_capacity(angles_per_level.len());
    let mut bit_offset = 0usize;

    for (&count, &bits) in angles_per_level.iter().zip(bits_per_level.iter()) {
        let mask = (1u8 << bits) - 1;
        let mut level_indices = Vec::with_capacity(count);
        for _ in 0..count {
            let byte_pos = bit_offset / 8;
            let bit_pos = bit_offset % 8;
            let mut val = (packed[byte_pos] >> bit_pos) & mask;
            // Handle spanning two bytes
            if bit_pos + bits as usize > 8 && byte_pos + 1 < packed.len() {
                val |= (packed[byte_pos + 1] << (8 - bit_pos)) & mask;
            }
            level_indices.push(val);
            bit_offset += bits as usize;
        }
        result.push(level_indices);
    }
    result
}

/// Forward recursive polar transform.
/// https://arxiv.org/abs/2502.02617 (Theorem 3.1)
/// 
/// Input: preconditioned vector of length `padded_dim`.
/// Output: (radii, angles_per_level) where radii has padded_dim / 2^levels entries
/// and angles_per_level[l] has padded_dim / 2^(l+1) entries.
fn polar_transform(vec: &[f32], levels: usize) -> (Vec<f32>, Vec<Vec<f32>>) {
    let mut radii = vec.to_vec();
    let mut all_angles = Vec::with_capacity(levels);

    for level in 0..levels {
        let n = radii.len() / 2;
        let mut new_radii = Vec::with_capacity(n);
        let mut angles = Vec::with_capacity(n);

        for j in 0..n {
            let a = radii[2 * j];
            let b = radii[2 * j + 1];
            let r = (a * a + b * b).sqrt();
            let angle = if level == 0 {
                // Level 1: full atan2 in [0, 2π)
                let mut theta = b.atan2(a);
                if theta < 0.0 {
                    theta += 2.0 * PI;
                }
                theta
            } else {
                // Levels 2+: atan of ratio of norms in [0, π/2]
                // a and b are non-negative radii
                b.atan2(a.max(f32::EPSILON))
            };
            new_radii.push(r);
            angles.push(angle);
        }

        all_angles.push(angles);
        radii = new_radii;
    }

    (radii, all_angles)
}

/// Inverse polar transform: reconstruct approximate Cartesian coordinates
/// from radii and quantized angle values.
/// https://arxiv.org/abs/2502.02617 (Theorem 3.1)
///
/// Process levels in reverse: from the top-level radii, expand using cos/sin of angles.
fn inverse_polar_transform(
    radii: &[f32],
    angle_values: &[Vec<f32>],
    levels: usize,
    padded_dim: usize,
) -> Vec<f32> {
    let mut current_radii = radii.to_vec();

    for level in (0..levels).rev() {
        let angles = &angle_values[level];
        let mut expanded = Vec::with_capacity(current_radii.len() * 2);
        for (j, &r) in current_radii.iter().enumerate() {
            let theta = angles[j];
            let (sin_val, cos_val) = theta.sin_cos();
            expanded.push(r * cos_val);
            expanded.push(r * sin_val);
        }
        current_radii = expanded;
    }

    debug_assert_eq!(current_radii.len(), padded_dim);
    current_radii
}

/// Quantize a single angle to the nearest codebook centroid, returning the index.
fn quantize_angle(angle: f32, codebook: &[f32]) -> u8 {
    let mut best_idx = 0u8;
    let mut best_dist = f32::MAX;
    for (i, &centroid) in codebook.iter().enumerate() {
        let dist = (angle - centroid).abs();
        if dist < best_dist {
            best_dist = dist;
            best_idx = i as u8;
        }
    }
    best_idx
}

impl<TStorage: EncodedStorage> EncodedVectorsPolar<TStorage> {
    pub fn storage(&self) -> &TStorage {
        &self.encoded_vectors
    }

    /// Compute the number of angles at each level for a given padded_dim and levels.
    fn angles_per_level(padded_dim: usize, levels: usize) -> Vec<usize> {
        (0..levels).map(|l| padded_dim / (1 << (l + 1))).collect()
    }

    /// Compute the number of radii stored per vector.
    fn num_radii(padded_dim: usize, levels: usize) -> usize {
        padded_dim / (1 << levels)
    }

    /// Compute the padded dimension: next multiple of 2^levels >= dim.
    pub fn compute_padded_dim(dim: usize, levels: usize) -> usize {
        let block = 1 << levels;
        ((dim + block - 1) / block) * block
    }

    /// Total bits for all angle indices per vector.
    fn total_angle_bits(padded_dim: usize, levels: usize, bits_per_level: &[u8]) -> usize {
        let apl = Self::angles_per_level(padded_dim, levels);
        apl.iter()
            .zip(bits_per_level.iter())
            .map(|(&count, &bits)| count * bits as usize)
            .sum()
    }

    /// Compute the quantized vector size in bytes.
    pub fn get_quantized_vector_size_for_params(
        padded_dim: usize,
        levels: usize,
        bits_per_level: &[u8],
    ) -> usize {
        let radii_bytes = Self::num_radii(padded_dim, levels) * std::mem::size_of::<f32>();
        let angle_bits = Self::total_angle_bits(padded_dim, levels, bits_per_level);
        let angle_bytes = (angle_bits + 7) / 8;
        radii_bytes + angle_bytes
    }

    /// Pad a vector to padded_dim by appending zeros.
    fn pad_vector(vector: &[f32], padded_dim: usize) -> Vec<f32> {
        let mut padded = vec![0.0f32; padded_dim];
        padded[..vector.len()].copy_from_slice(vector);
        padded
    }

    /// Encode a single vector: pad → rotate → polar transform → quantize → pack.
    fn encode_single_vector(
        vector: &[f32],
        metadata: &PolarMetadata,
        rotation_matrix: &[f32],
    ) -> Vec<u8> {
        let padded_dim = metadata.padded_dim;
        let levels = metadata.levels;

        // 1. Pad
        let padded = Self::pad_vector(vector, padded_dim);

        // 2. Rotate (precondition)
        let rotated = rotate_vector(rotation_matrix, &padded, padded_dim);

        // 3. Polar transform
        let (radii, angle_values) = polar_transform(&rotated, levels);

        // 4. Quantize angles
        let mut quantized_indices: Vec<Vec<u8>> = Vec::with_capacity(levels);
        for (level_idx, angles) in angle_values.iter().enumerate() {
            let codebook = &metadata.codebooks[level_idx];
            let indices: Vec<u8> = angles
                .iter()
                .map(|&a| quantize_angle(a, codebook))
                .collect();
            quantized_indices.push(indices);
        }

        // 5. Pack into bytes: radii first, then bit-packed angles
        let radii_bytes: &[u8] = bytemuck::cast_slice(&radii);
        let angle_bytes = pack_angles(&quantized_indices, &metadata.bits_per_level);

        let mut result = Vec::with_capacity(radii_bytes.len() + angle_bytes.len());
        result.extend_from_slice(radii_bytes);
        result.extend_from_slice(&angle_bytes);
        result
    }

    /// Dequantize a stored vector from its packed byte representation.
    fn dequantize_vector(bytes: &[u8], metadata: &PolarMetadata) -> Vec<f32> {
        let padded_dim = metadata.padded_dim;
        let levels = metadata.levels;
        let num_radii = Self::num_radii(padded_dim, levels);
        let radii_byte_len = num_radii * std::mem::size_of::<f32>();

        // Extract radii
        let radii: &[f32] = bytemuck::cast_slice(&bytes[..radii_byte_len]);

        // Extract and unpack angle indices
        let angle_bytes = &bytes[radii_byte_len..];
        let apl = Self::angles_per_level(padded_dim, levels);
        let angle_indices = unpack_angles(angle_bytes, &apl, &metadata.bits_per_level);

        // Convert indices to angle values via codebook lookup
        let mut angle_values: Vec<Vec<f32>> = Vec::with_capacity(levels);
        for (level_idx, indices) in angle_indices.iter().enumerate() {
            let codebook = &metadata.codebooks[level_idx];
            let values: Vec<f32> = indices.iter().map(|&idx| codebook[idx as usize]).collect();
            angle_values.push(values);
        }

        // Inverse polar transform
        inverse_polar_transform(radii, &angle_values, levels, padded_dim)
    }

    /// Encode vector data using PolarQuant.
    #[allow(clippy::too_many_arguments)]
    pub fn encode<'a>(
        orig_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
        mut storage_builder: impl EncodedStorageBuilder<Storage = TStorage>,
        vector_parameters: &VectorParameters,
        count: usize,
        levels: usize,
        meta_path: Option<&Path>,
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        debug_assert!(validate_vector_parameters(orig_data.clone(), vector_parameters).is_ok());

        let original_dim = vector_parameters.dim;
        let padded_dim = Self::compute_padded_dim(original_dim, levels);

        // Generate rotation matrix
        let rotation_seed: u64 = {
            let mut rng = StdRng::from_entropy();
            rng.random()
        };
        let rotation_matrix = generate_rotation_matrix(padded_dim, rotation_seed);

        // Build codebooks
        let codebooks = build_default_codebooks(levels);

        // Build bits_per_level
        let mut bits_per_level = Vec::with_capacity(levels);
        bits_per_level.push(LEVEL1_BITS);
        for _ in 1..levels {
            bits_per_level.push(HIGHER_LEVEL_BITS);
        }

        let metadata = PolarMetadata {
            vector_parameters: vector_parameters.clone(),
            rotation_seed,
            codebooks,
            bits_per_level,
            levels,
            padded_dim,
            original_dim,
        };

        // Encode each vector
        for (i, vector) in orig_data.enumerate() {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            // Periodic check every 1000 vectors
            if i % 1000 == 0 && i > 0 && stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let encoded = Self::encode_single_vector(vector.as_ref(), &metadata, &rotation_matrix);
            storage_builder
                .push_vector_data(&encoded)
                .map_err(|e| EncodingError::IOError(format!("Failed to push encoded vector: {e}")))?;
        }

        let encoded_vectors = storage_builder
            .build()
            .map_err(|e| EncodingError::IOError(format!("Failed to build storage: {e}")))?;

        // Save metadata
        if let Some(meta_path) = meta_path {
            atomic_save_json(meta_path, &metadata)
                .map_err(|e| EncodingError::IOError(format!("Failed to save metadata: {e}")))?;
        }

        if stopped.load(Ordering::Relaxed) {
            return Err(EncodingError::Stopped);
        }

        Ok(Self {
            encoded_vectors,
            metadata,
            metadata_path: meta_path.map(PathBuf::from),
            rotation_matrix,
        })
    }

    /// Load a previously encoded PolarQuant from disk.
    pub fn load(encoded_vectors: TStorage, meta_path: &Path) -> std::io::Result<Self> {
        let contents = fs::read_to_string(meta_path)?;
        let metadata: PolarMetadata = serde_json::from_str(&contents)?;

        // Regenerate rotation matrix from seed
        let rotation_matrix = generate_rotation_matrix(metadata.padded_dim, metadata.rotation_seed);

        Ok(Self {
            encoded_vectors,
            metadata,
            metadata_path: Some(meta_path.to_path_buf()),
            rotation_matrix,
        })
    }

    /// Get the quantized vector size in bytes for this instance.
    pub fn get_quantized_vector_size_instance(&self) -> usize {
        Self::get_quantized_vector_size_for_params(
            self.metadata.padded_dim,
            self.metadata.levels,
            &self.metadata.bits_per_level,
        )
    }

    pub fn get_metadata(&self) -> &PolarMetadata {
        &self.metadata
    }
}

impl<TStorage: EncodedStorage> EncodedVectors for EncodedVectorsPolar<TStorage> {
    type EncodedQuery = EncodedQueryPolar;

    fn is_on_disk(&self) -> bool {
        self.encoded_vectors.is_on_disk()
    }

    fn encode_query(&self, query: &[f32]) -> EncodedQueryPolar {
        // Pad and rotate the query
        let padded = Self::pad_vector(query, self.metadata.padded_dim);
        let rotated = rotate_vector(&self.rotation_matrix, &padded, self.metadata.padded_dim);
        EncodedQueryPolar {
            rotated_query: rotated,
        }
    }

    fn score_point(
        &self,
        query: &EncodedQueryPolar,
        i: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let bytes = self.encoded_vectors.get_vector_data(i);
        self.score_bytes(True, query, &bytes, hw_counter)
    }

    fn score_internal(
        &self,
        i: PointOffsetType,
        j: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let bytes_i = self.encoded_vectors.get_vector_data(i);
        let bytes_j = self.encoded_vectors.get_vector_data(j);

        hw_counter
            .vector_io_read()
            .incr_delta(self.metadata.padded_dim * 2);

        // Dequantize both vectors and compute distance in preconditioned space
        let vec_i = Self::dequantize_vector(&bytes_i, &self.metadata);
        let vec_j = Self::dequantize_vector(&bytes_j, &self.metadata);

        hw_counter.cpu_counter().incr_delta(self.metadata.padded_dim);

        let distance = self
            .metadata
            .vector_parameters
            .distance_type
            .distance(&vec_i, &vec_j);

        if self.metadata.vector_parameters.invert {
            -distance
        } else {
            distance
        }
    }

    fn quantized_vector_size(&self) -> usize {
        self.get_quantized_vector_size_instance()
    }

    fn encode_internal_vector(&self, _id: PointOffsetType) -> Option<EncodedQueryPolar> {
        // Dequantizing and re-encoding as a query introduces error; not supported.
        None
    }

    fn upsert_vector(
        &mut self,
        _id: PointOffsetType,
        _vector: &[f32],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        debug_assert!(
            false,
            "PolarQuant does not support upsert_vector",
        );
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "PolarQuant does not support upsert_vector",
        ))
    }

    fn vectors_count(&self) -> usize {
        self.encoded_vectors.vectors_count()
    }

    fn flusher(&self) -> MmapFlusher {
        self.encoded_vectors.flusher()
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.encoded_vectors.files();
        if let Some(meta_path) = &self.metadata_path {
            files.push(meta_path.clone());
        }
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = self.encoded_vectors.immutable_files();
        if let Some(meta_path) = &self.metadata_path {
            files.push(meta_path.clone());
        }
        files
    }

    type SupportsBytes = True;

    fn score_bytes(
        &self,
        _: Self::SupportsBytes,
        query: &Self::EncodedQuery,
        bytes: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        hw_counter
            .vector_io_read()
            .incr_delta(self.metadata.padded_dim);

        // Dequantize the stored vector
        let reconstructed = Self::dequantize_vector(bytes, &self.metadata);

        hw_counter.cpu_counter().incr_delta(self.metadata.padded_dim);

        // Compute distance in preconditioned space: <rotated_query, reconstructed>
        let distance = self
            .metadata
            .vector_parameters
            .distance_type
            .distance(&query.rotated_query, &reconstructed);

        if self.metadata.vector_parameters.invert {
            -distance
        } else {
            distance
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rotation_matrix_is_orthogonal() {
        let n = 16;
        let seed = 42;
        let m = generate_rotation_matrix(n, seed);

        // Check M * M^T ≈ I
        for i in 0..n {
            for j in 0..n {
                let dot: f32 = (0..n).map(|k| m[i * n + k] * m[j * n + k]).sum();
                if i == j {
                    assert!(
                        (dot - 1.0).abs() < 1e-4,
                        "Diagonal entry ({i},{j}) = {dot}, expected 1.0"
                    );
                } else {
                    assert!(
                        dot.abs() < 1e-4,
                        "Off-diagonal entry ({i},{j}) = {dot}, expected 0.0"
                    );
                }
            }
        }
    }

    #[test]
    fn test_rotation_preserves_norm() {
        let n = 32;
        let seed = 123;
        let m = generate_rotation_matrix(n, seed);

        let v: Vec<f32> = (0..n).map(|i| (i as f32 + 1.0) * 0.1).collect();
        let rotated = rotate_vector(&m, &v, n);

        let norm_orig: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_rot: f32 = rotated.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm_orig - norm_rot).abs() < 1e-3,
            "Norms differ: {norm_orig} vs {norm_rot}"
        );
    }

    #[test]
    fn test_rotation_preserves_inner_product() {
        let n = 32;
        let seed = 456;
        let m = generate_rotation_matrix(n, seed);

        let v1: Vec<f32> = (0..n).map(|i| (i as f32) * 0.05).collect();
        let v2: Vec<f32> = (0..n).map(|i| ((n - i) as f32) * 0.03).collect();

        let r1 = rotate_vector(&m, &v1, n);
        let r2 = rotate_vector(&m, &v2, n);

        let dot_orig: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
        let dot_rot: f32 = r1.iter().zip(r2.iter()).map(|(a, b)| a * b).sum();

        assert!(
            (dot_orig - dot_rot).abs() < 1e-2,
            "Dot products differ: {dot_orig} vs {dot_rot}"
        );
    }

    #[test]
    fn test_polar_transform_roundtrip() {
        let dim = 16;
        let levels = 4;
        let vec: Vec<f32> = (0..dim).map(|i| (i as f32 + 1.0) * 0.5).collect();

        let (radii, angle_values) = polar_transform(&vec, levels);
        let reconstructed = inverse_polar_transform(&radii, &angle_values, levels, dim);

        for (a, b) in vec.iter().zip(reconstructed.iter()) {
            assert!(
                (a - b).abs() < 1e-4,
                "Polar roundtrip error: {a} vs {b}"
            );
        }
    }

    #[test]
    fn test_bit_packing_roundtrip() {
        let angles = vec![
            vec![0u8, 15, 7, 3, 12, 1, 8, 5], // 4-bit indices
            vec![0u8, 3, 1, 2],                 // 2-bit indices
            vec![1u8, 0],                       // 2-bit indices
            vec![3u8],                           // 2-bit indices
        ];
        let bits_per_level = vec![4u8, 2, 2, 2];

        let packed = pack_angles(&angles, &bits_per_level);

        let counts: Vec<usize> = angles.iter().map(|a| a.len()).collect();
        let unpacked = unpack_angles(&packed, &counts, &bits_per_level);

        assert_eq!(angles, unpacked);
    }

    #[test]
    fn test_codebook_construction() {
        let codebooks = build_default_codebooks(4);

        assert_eq!(codebooks.len(), 4);
        assert_eq!(codebooks[0].len(), LEVEL1_CODEBOOK_SIZE); // 16
        assert_eq!(codebooks[1].len(), HIGHER_LEVEL_CODEBOOK_SIZE); // 4
        assert_eq!(codebooks[2].len(), HIGHER_LEVEL_CODEBOOK_SIZE); // 4
        assert_eq!(codebooks[3].len(), HIGHER_LEVEL_CODEBOOK_SIZE); // 4

        // Level 1 centroids should be in [0, 2π)
        for &c in &codebooks[0] {
            assert!(c >= 0.0 && c < 2.0 * PI);
        }
        // Levels 2+ centroids should be in [0, π/2]
        for level in 1..4 {
            for &c in &codebooks[level] {
                assert!(c >= 0.0 && c <= PI / 2.0, "Centroid {c} out of range at level {level}");
            }
        }
    }

    // Helper type alias for tests
    #[cfg(feature = "testing")]
    type TestPolar = EncodedVectorsPolar<crate::encoded_storage::TestEncodedStorage>;

    #[test]
    fn test_quantized_vector_size() {
        // dim=128, levels=4 → padded=128, num_radii=8, radii_bytes=32
        // angle bits: 64*4 + 32*2 + 16*2 + 8*2 = 256+64+32+16 = 368 bits = 46 bytes
        // total = 32 + 46 = 78
        let size =
            EncodedVectorsPolar::<crate::encoded_storage::TestEncodedStorage>::get_quantized_vector_size_for_params(
                128,
                4,
                &[4, 2, 2, 2],
            );
        assert_eq!(size, 78);
    }

    #[test]
    fn test_padded_dim() {
        assert_eq!(
            EncodedVectorsPolar::<crate::encoded_storage::TestEncodedStorage>::compute_padded_dim(
                768, 4
            ),
            768
        );
        assert_eq!(
            EncodedVectorsPolar::<crate::encoded_storage::TestEncodedStorage>::compute_padded_dim(
                100, 4
            ),
            112
        );
        assert_eq!(
            EncodedVectorsPolar::<crate::encoded_storage::TestEncodedStorage>::compute_padded_dim(
                128, 4
            ),
            128
        );
    }

    #[cfg(feature = "testing")]
    #[test]
    fn test_encode_and_score() {
        use std::sync::atomic::AtomicBool;

        use crate::encoded_storage::TestEncodedStorageBuilder;
        use crate::encoded_vectors::DistanceType;

        let dim = 16;
        let levels = 4;
        let padded_dim = TestPolar::compute_padded_dim(dim, levels);
        let vec_size =
            TestPolar::get_quantized_vector_size_for_params(padded_dim, levels, &[4, 2, 2, 2]);

        let vectors: Vec<Vec<f32>> = (0..10)
            .map(|i| (0..dim).map(|j| ((i * dim + j) as f32) * 0.01).collect())
            .collect();

        let params = VectorParameters {
            dim,
            distance_type: DistanceType::Dot,
            invert: false,
            deprecated_count: None,
        };

        let stopped = AtomicBool::new(false);
        let storage_builder = TestEncodedStorageBuilder::new(None, vec_size);

        let encoded = TestPolar::encode(
            vectors.iter().map(|v| v.as_slice()),
            storage_builder,
            &params,
            vectors.len(),
            levels,
            None,
            &stopped,
        )
        .unwrap();

        assert_eq!(encoded.vectors_count(), 10);

        // Encode a query and score against the first vector
        let query = encoded.encode_query(&vectors[0]);
        let hw = HardwareCounterCell::disposable();
        let score = encoded.score_point(&query, 0, &hw);

        // The score should be approximately the self-dot-product
        let expected_dot: f32 = vectors[0].iter().map(|x| x * x).sum();
        // Allow generous tolerance due to quantization
        assert!(
            (score - expected_dot).abs() < expected_dot * 0.5,
            "Score {score} too far from expected {expected_dot}"
        );
    }
}