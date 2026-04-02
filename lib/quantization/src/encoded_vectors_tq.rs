use std::alloc::Layout;
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use common::fs::atomic_save_json;
use common::mmap::MmapFlusher;
use common::typelevel::True;
use common::types::PointOffsetType;
use fs_err as fs;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::encoded_storage::{EncodedStorage, EncodedStorageBuilder};
use crate::encoded_vectors::{EncodedVectors, VectorParameters, validate_vector_parameters};
use crate::{DistanceType, EncodingError};

pub const DEFAULT_TURBO_QUANT_BITS: usize = 4;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum TqCorrection {
    NoCorrection,
    Qjl,
    #[default]
    Normalization,
    QjlNormalization,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum TqRotation {
    NoRotation,
    #[default]
    Hadamard,
    Random,
}

/// Default seed for generating the random rotation.
const ROTATION_SEED: u64 = 42;

/// Number of iterations for Lloyd's algorithm (codebook optimization).
const LLOYD_ITERATIONS: usize = 100;

/// Size of the stored norm (f32) in bytes.
const NORM_SIZE: usize = size_of::<f32>();

// ============================================================================
// Math helpers for codebook computation
// ============================================================================

/// Approximate error function (Abramowitz & Stegun 7.1.26, max error 1.5e-7).
fn erf_approx(x: f64) -> f64 {
    let a = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * a);
    let poly = t
        * (0.254829592
            + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))));
    let result = 1.0 - poly * (-a * a).exp();
    if x >= 0.0 { result } else { -result }
}

/// Standard normal PDF: φ(x) = exp(-x²/2) / √(2π)
fn std_normal_pdf(x: f64) -> f64 {
    (-0.5 * x * x).exp() / (2.0 * std::f64::consts::PI).sqrt()
}

/// Standard normal CDF: Φ(x) = (1 + erf(x/√2)) / 2
fn std_normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf_approx(x / std::f64::consts::SQRT_2))
}

/// Approximate inverse of the standard normal CDF (Abramowitz & Stegun 26.2.23).
fn inv_std_normal_cdf(p: f64) -> f64 {
    if p <= 0.0 {
        return f64::NEG_INFINITY;
    }
    if p >= 1.0 {
        return f64::INFINITY;
    }

    let t = if p < 0.5 {
        (-2.0 * p.ln()).sqrt()
    } else {
        (-2.0 * (1.0 - p).ln()).sqrt()
    };

    let result = t
        - (2.515517 + 0.802853 * t + 0.010328 * t * t)
            / (1.0 + 1.432788 * t + 0.189269 * t * t + 0.001308 * t * t * t);

    if p < 0.5 { -result } else { result }
}

/// E[X | a < X < b] where X ~ N(0, σ²).
///
/// Uses: σ · (φ(a/σ) − φ(b/σ)) / (Φ(b/σ) − Φ(a/σ))
fn gaussian_conditional_expectation(sigma: f64, a: f64, b: f64) -> f64 {
    let a_std = if a.is_finite() { a / sigma } else { a };
    let b_std = if b.is_finite() { b / sigma } else { b };

    let prob = if a_std.is_infinite() && a_std < 0.0 {
        std_normal_cdf(b_std)
    } else if b_std.is_infinite() && b_std > 0.0 {
        1.0 - std_normal_cdf(a_std)
    } else {
        std_normal_cdf(b_std) - std_normal_cdf(a_std)
    };

    if prob < 1e-15 {
        if a.is_finite() && b.is_infinite() {
            return a + sigma;
        } else if a.is_infinite() && b.is_finite() {
            return b - sigma;
        } else if a.is_finite() && b.is_finite() {
            return (a + b) / 2.0;
        } else {
            return 0.0;
        }
    }

    let pdf_diff = std_normal_pdf(a_std) - std_normal_pdf(b_std);
    sigma * pdf_diff / prob
}

// ============================================================================
// Codebook construction (Lloyd-Max optimal scalar quantizer for N(0,1/d))
// ============================================================================

/// Compute optimal centroids and decision boundaries for `levels`-bit scalar
/// quantization of coordinates distributed as N(0, 1/padded_dim).
///
/// Returns (centroids, boundaries) where centroids has 2^levels entries and
/// boundaries has 2^levels − 1 entries (midpoints between consecutive centroids).
pub(crate) fn compute_codebook(levels: usize, padded_dim: usize) -> (Vec<f32>, Vec<f32>) {
    let d = padded_dim as f64;

    let centroids_f64: Vec<f64> = match levels {
        1 => {
            // Closed-form: ±√(2/(πd))
            let c = (2.0 / (std::f64::consts::PI * d)).sqrt();
            vec![-c, c]
        }
        2 => {
            // Hardcoded optimal centroids from the TurboQuant paper
            let s = d.sqrt();
            vec![-1.51 / s, -0.453 / s, 0.453 / s, 1.51 / s]
        }
        _ => {
            // Lloyd's algorithm on N(0, 1/d)
            let sigma = 1.0 / d.sqrt();
            lloyds_gaussian(1 << levels, sigma)
        }
    };

    let boundaries: Vec<f32> = centroids_f64
        .windows(2)
        .map(|w| ((w[0] + w[1]) / 2.0) as f32)
        .collect();

    let centroids: Vec<f32> = centroids_f64.iter().map(|&c| c as f32).collect();

    (centroids, boundaries)
}

/// Lloyd's algorithm for optimal scalar quantization of N(0, σ²).
fn lloyds_gaussian(n_centroids: usize, sigma: f64) -> Vec<f64> {
    let mut boundaries: Vec<f64> = (1..n_centroids)
        .map(|i| {
            let p = i as f64 / n_centroids as f64;
            sigma * inv_std_normal_cdf(p)
        })
        .collect();

    let mut centroids = vec![0.0f64; n_centroids];

    for _ in 0..LLOYD_ITERATIONS {
        centroids[0] = gaussian_conditional_expectation(sigma, f64::NEG_INFINITY, boundaries[0]);
        for i in 1..n_centroids - 1 {
            centroids[i] =
                gaussian_conditional_expectation(sigma, boundaries[i - 1], boundaries[i]);
        }
        centroids[n_centroids - 1] =
            gaussian_conditional_expectation(sigma, boundaries[n_centroids - 2], f64::INFINITY);

        for i in 0..n_centroids - 1 {
            boundaries[i] = (centroids[i] + centroids[i + 1]) / 2.0;
        }
    }

    centroids.sort_by(|a, b| a.partial_cmp(b).unwrap());
    centroids
}

// ============================================================================
// Rotation (Walsh-Hadamard Transform + random sign flips)
// ============================================================================

/// Generate two random sign vectors (±1.0) from a deterministic seed.
fn generate_signs(seed: u64, padded_dim: usize) -> (Vec<f32>, Vec<f32>) {
    let mut rng = StdRng::seed_from_u64(seed);
    let signs1: Vec<f32> = (0..padded_dim)
        .map(|_| {
            if rng.random::<bool>() {
                1.0f32
            } else {
                -1.0f32
            }
        })
        .collect();
    let signs2: Vec<f32> = (0..padded_dim)
        .map(|_| {
            if rng.random::<bool>() {
                1.0f32
            } else {
                -1.0f32
            }
        })
        .collect();
    (signs1, signs2)
}

/// In-place normalized Walsh-Hadamard Transform. Self-inverse: WHT(WHT(x)) = x.
/// Input length must be a power of 2.
fn walsh_hadamard_transform(x: &mut [f32]) {
    let n = x.len();
    debug_assert!(n.is_power_of_two(), "WHT requires power-of-2 length");
    let mut h = 1;
    while h < n {
        for i in (0..n).step_by(h * 2) {
            for j in i..i + h {
                let a = x[j];
                let b = x[j + h];
                x[j] = a + b;
                x[j + h] = a - b;
            }
        }
        h *= 2;
    }
    let norm = (n as f32).sqrt();
    for val in x.iter_mut() {
        *val /= norm;
    }
}

/// Forward rotation: y = signs2 · WHT(signs1 · pad(x)).
/// Pads input to `padded_dim`, returns `padded_dim` elements.
fn apply_rotation(x: &[f32], signs1: &[f32], signs2: &[f32], padded_dim: usize) -> Vec<f32> {
    let mut buf = vec![0.0f32; padded_dim];
    buf[..x.len()].copy_from_slice(x);
    for (b, &s) in buf.iter_mut().zip(signs1.iter()) {
        *b *= s;
    }
    walsh_hadamard_transform(&mut buf);
    for (b, &s) in buf.iter_mut().zip(signs2.iter()) {
        *b *= s;
    }
    buf
}

/// Inverse rotation: x = truncate(signs1 · WHT(signs2 · y), dim).
fn apply_inverse_rotation(
    y: &[f32],
    dim: usize,
    signs1: &[f32],
    signs2: &[f32],
    padded_dim: usize,
) -> Vec<f32> {
    let mut buf = vec![0.0f32; padded_dim];
    let len = y.len().min(padded_dim);
    buf[..len].copy_from_slice(&y[..len]);
    for (b, &s) in buf.iter_mut().zip(signs2.iter()) {
        *b *= s;
    }
    walsh_hadamard_transform(&mut buf);
    for (b, &s) in buf.iter_mut().zip(signs1.iter()) {
        *b *= s;
    }
    buf[..dim].to_vec()
}

// ============================================================================
// Bit packing for quantization indices
// ============================================================================

/// Bytes needed to store `num_coords` indices of `levels` bits each.
fn packed_indices_size(num_coords: usize, levels: usize) -> usize {
    (num_coords * levels + 7) / 8
}

/// Pack b-bit indices into a compact byte array (MSB-first bitstream).
fn pack_indices(indices: &[u8], levels: usize) -> Vec<u8> {
    let total_bits = indices.len() * levels;
    let total_bytes = (total_bits + 7) / 8;
    let mut packed = vec![0u8; total_bytes];
    let mut bit_pos = 0usize;
    for &idx in indices {
        for b in (0..levels).rev() {
            let byte_idx = bit_pos / 8;
            let bit_idx = 7 - (bit_pos % 8);
            if (idx >> b) & 1 != 0 {
                packed[byte_idx] |= 1 << bit_idx;
            }
            bit_pos += 1;
        }
    }
    packed
}

/// Unpack the index at `coord_index` from a packed bitstream.
fn unpack_index(packed: &[u8], coord_index: usize, levels: usize) -> u8 {
    let bit_start = coord_index * levels;
    let mut result = 0u8;
    for b in 0..levels {
        let bit_pos = bit_start + b;
        let byte_idx = bit_pos / 8;
        let bit_idx = 7 - (bit_pos % 8);
        result = (result << 1) | ((packed[byte_idx] >> bit_idx) & 1);
    }
    result
}

/// Find the partition index for `value` via binary search on sorted boundaries.
fn find_nearest_centroid(value: f32, boundaries: &[f32]) -> u8 {
    match boundaries
        .binary_search_by(|b| b.partial_cmp(&value).unwrap_or(std::cmp::Ordering::Equal))
    {
        Ok(i) | Err(i) => i as u8,
    }
}

// ============================================================================
// Encode a single vector
// ============================================================================

/// Encode a vector into TurboQuant binary format.
///
/// Layout: `[norm: f32 (4 bytes)] [packed indices: ⌈padded_dim·levels/8⌉ bytes]`
fn encode_vector_data(
    vector_data: &[f32],
    signs1: &[f32],
    signs2: &[f32],
    boundaries: &[f32],
    padded_dim: usize,
    levels: usize,
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

    // 3. Rotate into WHT domain
    let rotated = apply_rotation(&normalized, signs1, signs2, padded_dim);

    // 4. Quantize each coordinate to nearest centroid
    let indices: Vec<u8> = rotated
        .iter()
        .map(|&val| find_nearest_centroid(val, boundaries))
        .collect();

    // 5. Pack: norm bytes + index bits
    let packed = pack_indices(&indices, levels);
    let mut result = Vec::with_capacity(NORM_SIZE + packed.len());
    result.extend_from_slice(&norm.to_ne_bytes());
    result.extend_from_slice(&packed);
    result
}

// ============================================================================
// Main TurboQuant structures
// ============================================================================

pub struct EncodedVectorsTQ<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    metadata_path: Option<PathBuf>,
    /// Random sign vectors for WHT rotation (derived deterministically from seed).
    signs1: Vec<f32>,
    signs2: Vec<f32>,
}

/// Pre-processed query for efficient TurboQuant scoring.
pub struct EncodedQueryTQ {
    /// WHT-rotated query (padded_dim elements). Used for Dot and L2 scoring
    /// in rotated space without needing inverse rotation.
    rotated_query: Vec<f32>,
    /// Original query (dim elements). Needed for L1 scoring which is not
    /// rotation-invariant.
    original_query: Vec<f32>,
    /// ||q||². Used for L2 scoring: ||q−x̃||² = ||q||² + γ² − 2γ⟨Rq, ŷ⟩.
    query_norm_sq: f32,
}

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    pub vector_parameters: VectorParameters,
    /// Bits per coordinate (1–6).
    pub bits: usize,
    /// Seed for deterministic rotation generation.
    pub rotation_seed: u64,
    /// Sorted centroid values (2^bits entries).
    pub codebook: Vec<f32>,
    /// Decision boundaries between centroids (2^bits − 1 entries).
    pub boundaries: Vec<f32>,
    /// WHT-padded dimension (next power of 2 ≥ dim).
    pub padded_dim: usize,
    pub correction: TqCorrection,
    pub rotation: TqRotation,
    pub hadamard_chunk: Option<usize>,
}

impl<TStorage: EncodedStorage> EncodedVectorsTQ<TStorage> {
    pub fn storage(&self) -> &TStorage {
        &self.encoded_vectors
    }

    /// Encode vector data using TurboQuant (PolarQuant MSE).
    ///
    /// Algorithm:
    /// 1. Extract L2 norm and normalize to unit sphere
    /// 2. Apply random rotation (WHT + random sign flips) to decorrelate coordinates
    /// 3. Scalar-quantize each rotated coordinate using Lloyd-Max optimal codebook
    /// 4. Pack norm + quantized indices into compact binary format
    ///
    /// # Arguments
    /// * `data` - iterator over original vector data
    /// * `storage_builder` - encoding result storage builder
    /// * `vector_parameters` - parameters of original vector data (dimension, distance, etc)
    /// * `count` - number of vectors in `data` iterator, used for progress bar
    /// * `bits` - number of bits per coordinate (default: 4, range: 1-6)
    /// * `correction` - correction method
    /// * `rotation` - rotation method
    /// * `hadamard_chunk` - hadamard chunk size (only when rotation is Hadamard)
    /// * `meta_path` - optional path to save metadata, if `None`, metadata will not be saved
    /// * `stopped` - Atomic bool that indicates if encoding should be stopped
    #[allow(clippy::too_many_arguments)]
    pub fn encode<'a>(
        data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
        mut storage_builder: impl EncodedStorageBuilder<Storage = TStorage>,
        vector_parameters: &VectorParameters,
        _count: usize,
        bits: usize,
        correction: TqCorrection,
        rotation: TqRotation,
        hadamard_chunk: Option<usize>,
        meta_path: Option<&Path>,
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        debug_assert!(validate_vector_parameters(data.clone(), vector_parameters).is_ok());

        let padded_dim = vector_parameters.dim.next_power_of_two();
        let rotation_seed = ROTATION_SEED;

        let (signs1, signs2) = generate_signs(rotation_seed, padded_dim);
        let (codebook, boundaries) = compute_codebook(bits, padded_dim);

        // Coordinate analysis (only if QDRANT_ANALYSIS_DIR is set)
        if let Some(analysis_run_dir) = crate::coordinate_analysis::create_run_dir() {
            crate::coordinate_analysis::analyse(
                data.clone(),
                vector_parameters,
                _count,
                &analysis_run_dir,
                "before_rotation",
            );

            let rotated_vectors: Vec<Vec<f32>> = data
                .clone()
                .map(|v| {
                    let v = v.as_ref();
                    let norm_sq: f32 = v.iter().map(|&x| x * x).sum();
                    let norm = norm_sq.sqrt();
                    let normalized: Vec<f32> = if norm > 0.0 {
                        v.iter().map(|&x| x / norm).collect()
                    } else {
                        vec![0.0; v.len()]
                    };
                    apply_rotation(&normalized, &signs1, &signs2, padded_dim)
                })
                .collect();
            let rotated_params = VectorParameters {
                dim: padded_dim,
                ..vector_parameters.clone()
            };
            crate::coordinate_analysis::analyse(
                rotated_vectors.iter(),
                &rotated_params,
                _count,
                &analysis_run_dir,
                "after_rotation",
            );
        }

        for vector in data {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let encoded = encode_vector_data(
                vector.as_ref(),
                &signs1,
                &signs2,
                &boundaries,
                padded_dim,
                bits,
            );

            storage_builder.push_vector_data(&encoded).map_err(|e| {
                EncodingError::EncodingError(format!("Failed to push encoded vector: {e}",))
            })?;
        }

        let encoded_vectors = storage_builder
            .build()
            .map_err(|e| EncodingError::EncodingError(format!("Failed to build storage: {e}",)))?;

        let metadata = Metadata {
            vector_parameters: vector_parameters.clone(),
            bits,
            rotation_seed,
            codebook,
            boundaries,
            padded_dim,
            correction,
            rotation,
            hadamard_chunk,
        };
        if let Some(meta_path) = meta_path {
            meta_path
                .parent()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Path must have a parent directory",
                    )
                })
                .and_then(fs::create_dir_all)
                .map_err(|e| {
                    EncodingError::EncodingError(format!(
                        "Failed to create metadata directory: {e}",
                    ))
                })?;
            atomic_save_json(meta_path, &metadata).map_err(|e| {
                EncodingError::EncodingError(format!("Failed to save metadata: {e}",))
            })?;
        }

        Ok(Self {
            encoded_vectors,
            metadata,
            metadata_path: meta_path.map(PathBuf::from),
            signs1,
            signs2,
        })
    }

    pub fn load(encoded_vectors: TStorage, meta_path: &Path) -> std::io::Result<Self> {
        let contents = fs::read_to_string(meta_path)?;
        let metadata: Metadata = serde_json::from_str(&contents)?;
        let (signs1, signs2) = generate_signs(metadata.rotation_seed, metadata.padded_dim);
        let result = Self {
            encoded_vectors,
            metadata,
            metadata_path: Some(meta_path.to_path_buf()),
            signs1,
            signs2,
        };
        Ok(result)
    }

    /// Get quantized vector size in bytes:
    /// 4 bytes (f32 norm) + ⌈padded_dim × bits / 8⌉ bytes (packed indices).
    pub fn get_quantized_vector_size(vector_parameters: &VectorParameters, bits: usize) -> usize {
        let padded_dim = vector_parameters.dim.next_power_of_two();
        NORM_SIZE + packed_indices_size(padded_dim, bits)
    }

    /// Decode stored bytes into (norm, norm-corrected centroid vector in rotated space).
    /// The returned vector has unit norm after correction.
    fn decode_rotated(
        bytes: &[u8],
        codebook: &[f32],
        padded_dim: usize,
        levels: usize,
    ) -> (f32, Vec<f32>) {
        let norm = f32::from_ne_bytes(bytes[..NORM_SIZE].try_into().unwrap());
        let packed = &bytes[NORM_SIZE..];

        // Look up centroids
        let mut y_tilde = Vec::with_capacity(padded_dim);
        for i in 0..padded_dim {
            let idx = unpack_index(packed, i, levels) as usize;
            y_tilde.push(codebook[idx.min(codebook.len() - 1)]);
        }

        // Norm correction: re-normalize to unit norm (compensates for quantization error)
        let y_norm_sq: f32 = y_tilde.iter().map(|&v| v * v).sum();
        let y_norm = y_norm_sq.sqrt();
        if y_norm > 1e-10 {
            for v in y_tilde.iter_mut() {
                *v /= y_norm;
            }
        }

        (norm, y_tilde)
    }

    /// Dot product score in rotated space: ⟨q, x̃⟩ = γ · ⟨Rq, ŷ⟩
    fn score_dot(
        rotated_query: &[f32],
        bytes: &[u8],
        codebook: &[f32],
        padded_dim: usize,
        levels: usize,
    ) -> f32 {
        let (gamma, y_corrected) = Self::decode_rotated(bytes, codebook, padded_dim, levels);
        let dot: f32 = rotated_query
            .iter()
            .zip(y_corrected.iter())
            .map(|(&q, &v)| q * v)
            .sum();
        gamma * dot
    }

    /// L2 score in rotated space: ||q − x̃||² = ||q||² + γ² − 2γ⟨Rq, ŷ⟩
    fn score_l2(
        rotated_query: &[f32],
        query_norm_sq: f32,
        bytes: &[u8],
        codebook: &[f32],
        padded_dim: usize,
        levels: usize,
    ) -> f32 {
        let (gamma, y_corrected) = Self::decode_rotated(bytes, codebook, padded_dim, levels);
        let dot: f32 = rotated_query
            .iter()
            .zip(y_corrected.iter())
            .map(|(&q, &v)| q * v)
            .sum();
        query_norm_sq + gamma * gamma - 2.0 * gamma * dot
    }

    /// L1 score requires full reconstruction via inverse rotation.
    #[allow(clippy::too_many_arguments)]
    fn score_l1(
        original_query: &[f32],
        bytes: &[u8],
        codebook: &[f32],
        signs1: &[f32],
        signs2: &[f32],
        padded_dim: usize,
        dim: usize,
        levels: usize,
    ) -> f32 {
        let (gamma, y_corrected) = Self::decode_rotated(bytes, codebook, padded_dim, levels);
        let x_hat = apply_inverse_rotation(&y_corrected, dim, signs1, signs2, padded_dim);
        original_query
            .iter()
            .zip(x_hat.iter())
            .map(|(&q, &x)| (q - gamma * x).abs())
            .sum()
    }

    fn score_tq(&self, query: &EncodedQueryTQ, bytes: &[u8]) -> f32 {
        let result = match self.metadata.vector_parameters.distance_type {
            DistanceType::Dot => Self::score_dot(
                &query.rotated_query,
                bytes,
                &self.metadata.codebook,
                self.metadata.padded_dim,
                self.metadata.bits,
            ),
            DistanceType::L2 => Self::score_l2(
                &query.rotated_query,
                query.query_norm_sq,
                bytes,
                &self.metadata.codebook,
                self.metadata.padded_dim,
                self.metadata.bits,
            ),
            DistanceType::L1 => Self::score_l1(
                &query.original_query,
                bytes,
                &self.metadata.codebook,
                &self.signs1,
                &self.signs2,
                self.metadata.padded_dim,
                self.metadata.vector_parameters.dim,
                self.metadata.bits,
            ),
        };

        if self.metadata.vector_parameters.invert {
            -result
        } else {
            result
        }
    }

    pub fn get_quantized_vector(&self, i: PointOffsetType) -> Cow<'_, [u8]> {
        self.encoded_vectors.get_vector_data(i)
    }

    pub fn layout(&self) -> Layout {
        Layout::from_size_align(self.quantized_vector_size(), align_of::<f32>()).unwrap()
    }

    pub fn get_metadata(&self) -> &Metadata {
        &self.metadata
    }
}

impl<TStorage: EncodedStorage> EncodedVectors for EncodedVectorsTQ<TStorage> {
    type EncodedQuery = EncodedQueryTQ;

    fn is_on_disk(&self) -> bool {
        self.encoded_vectors.is_on_disk()
    }

    fn encode_query(&self, query: &[f32]) -> EncodedQueryTQ {
        let rotated_query =
            apply_rotation(query, &self.signs1, &self.signs2, self.metadata.padded_dim);
        let query_norm_sq: f32 = query.iter().map(|&x| x * x).sum();
        EncodedQueryTQ {
            rotated_query,
            original_query: query.to_vec(),
            query_norm_sq,
        }
    }

    fn score_point(
        &self,
        query: &EncodedQueryTQ,
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
        let v1 = self.encoded_vectors.get_vector_data(i);
        let v2 = self.encoded_vectors.get_vector_data(j);

        hw_counter.vector_io_read().incr_delta(v1.len() + v2.len());

        let codebook = &self.metadata.codebook;
        let padded_dim = self.metadata.padded_dim;
        let bits = self.metadata.bits;

        let (gamma1, y1) = Self::decode_rotated(&v1, codebook, padded_dim, bits);
        let (gamma2, y2) = Self::decode_rotated(&v2, codebook, padded_dim, bits);

        let result = match self.metadata.vector_parameters.distance_type {
            DistanceType::Dot => {
                let dot: f32 = y1.iter().zip(y2.iter()).map(|(&a, &b)| a * b).sum();
                gamma1 * gamma2 * dot
            }
            DistanceType::L2 => {
                // ||x̃₁ − x̃₂||² = γ₁² + γ₂² − 2γ₁γ₂⟨ŷ₁,ŷ₂⟩
                let dot: f32 = y1.iter().zip(y2.iter()).map(|(&a, &b)| a * b).sum();
                gamma1 * gamma1 + gamma2 * gamma2 - 2.0 * gamma1 * gamma2 * dot
            }
            DistanceType::L1 => {
                let dim = self.metadata.vector_parameters.dim;
                let x1 = apply_inverse_rotation(&y1, dim, &self.signs1, &self.signs2, padded_dim);
                let x2 = apply_inverse_rotation(&y2, dim, &self.signs1, &self.signs2, padded_dim);
                x1.iter()
                    .zip(x2.iter())
                    .map(|(&a, &b)| (gamma1 * a - gamma2 * b).abs())
                    .sum()
            }
        };

        if self.metadata.vector_parameters.invert {
            -result
        } else {
            result
        }
    }

    fn quantized_vector_size(&self) -> usize {
        NORM_SIZE + packed_indices_size(self.metadata.padded_dim, self.metadata.bits)
    }

    fn encode_internal_vector(&self, id: PointOffsetType) -> Option<EncodedQueryTQ> {
        let bytes = self.encoded_vectors.get_vector_data(id);
        let (gamma, y_corrected) = Self::decode_rotated(
            &bytes,
            &self.metadata.codebook,
            self.metadata.padded_dim,
            self.metadata.bits,
        );

        // Rotated query = gamma * y_corrected (already in rotated space)
        let rotated_query: Vec<f32> = y_corrected.iter().map(|&v| v * gamma).collect();

        // Reconstruct original-space vector for L1
        let dim = self.metadata.vector_parameters.dim;
        let x_hat = apply_inverse_rotation(
            &y_corrected,
            dim,
            &self.signs1,
            &self.signs2,
            self.metadata.padded_dim,
        );
        let original_query: Vec<f32> = x_hat.iter().map(|&v| v * gamma).collect();

        Some(EncodedQueryTQ {
            rotated_query,
            original_query,
            query_norm_sq: gamma * gamma,
        })
    }

    fn upsert_vector(
        &mut self,
        _id: PointOffsetType,
        _vector: &[f32],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        debug_assert!(false, "TurboQuant does not support upsert_vector",);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "TurboQuant does not support upsert_vector",
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
        hw_counter.cpu_counter().incr_delta(bytes.len());
        self.score_tq(query, bytes)
    }
}
