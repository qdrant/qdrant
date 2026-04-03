mod codebook;
pub mod coordinate_analysis;
mod packing;
mod rotation;

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
use serde::{Deserialize, Serialize};

use crate::encoded_storage::{EncodedStorage, EncodedStorageBuilder};
use crate::encoded_vectors::{EncodedVectors, VectorParameters, validate_vector_parameters};
use crate::{DistanceType, EncodingError};

pub(crate) use codebook::Codebook;
use packing::pack_indices;
use rotation::{ROTATION_SEED, RotationImpl};

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
    RotationMatrix,
}

/// Size of the stored norm (f32) in bytes.
const NORM_SIZE: usize = size_of::<f32>();

// ============================================================================
// Encode a single vector
// ============================================================================

/// Encode a vector into TurboQuant binary format.
///
/// Layout: `[norm: f32 (4 bytes)] [packed indices: ⌈padded_dim·levels/8⌉ bytes]`
fn encode_vector_data(
    vector_data: &[f32],
    rotation: &RotationImpl,
    codebook: &Codebook,
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
    let rotated = rotation.apply(&normalized);

    // 4. Quantize each coordinate to nearest centroid
    let indices: Vec<u8> = rotated.iter().map(|&val| codebook.quantize(val)).collect();

    // 5. Pack: norm bytes + index bits
    let packed = pack_indices(&indices, codebook.bits);
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
    rotation_impl: RotationImpl,
}

/// Pre-processed query for efficient TurboQuant scoring.
pub struct EncodedQueryTQ {
    /// Rotated query (padded_dim elements). Used for Dot and L2 scoring
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
    /// Seed for deterministic rotation generation.
    pub rotation_seed: u64,
    /// Optimal scalar quantizer codebook.
    pub codebook: Codebook,
    /// Padded dimension (may differ from original dim depending on rotation).
    pub padded_dim: usize,
    pub correction: TqCorrection,
    pub rotation: TqRotation,
    pub hadamard_chunk: Option<usize>,
    pub plus: bool,
}

impl<TStorage: EncodedStorage> EncodedVectorsTQ<TStorage> {
    pub fn storage(&self) -> &TStorage {
        &self.encoded_vectors
    }

    /// Encode vector data using TurboQuant (PolarQuant MSE).
    ///
    /// Algorithm:
    /// 1. Extract L2 norm and normalize to unit sphere
    /// 2. Apply rotation to decorrelate coordinates
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
    /// * `plus` - whether to use TurboQuant+ mode
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
        plus: bool,
        meta_path: Option<&Path>,
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        debug_assert!(validate_vector_parameters(data.clone(), vector_parameters).is_ok());

        let rotation_seed = ROTATION_SEED;
        let rotation_impl = RotationImpl::new(rotation, rotation_seed, vector_parameters.dim);
        let padded_dim = rotation_impl.padded_dim();
        let codebook = Codebook::new(bits, padded_dim);

        // Coordinate analysis (only if QDRANT_ANALYSIS_DIR is set)
        if let Some(analysis_run_dir) = coordinate_analysis::create_run_dir() {
            coordinate_analysis::analyse(
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
                    rotation_impl.apply(&normalized)
                })
                .collect();
            let rotated_params = VectorParameters {
                dim: padded_dim,
                ..vector_parameters.clone()
            };
            coordinate_analysis::analyse(
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

            let encoded = encode_vector_data(vector.as_ref(), &rotation_impl, &codebook);

            storage_builder.push_vector_data(&encoded).map_err(|e| {
                EncodingError::EncodingError(format!("Failed to push encoded vector: {e}",))
            })?;
        }

        let encoded_vectors = storage_builder
            .build()
            .map_err(|e| EncodingError::EncodingError(format!("Failed to build storage: {e}",)))?;

        let metadata = Metadata {
            vector_parameters: vector_parameters.clone(),
            rotation_seed,
            codebook,
            padded_dim,
            correction,
            rotation,
            hadamard_chunk,
            plus,
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
            rotation_impl,
        })
    }

    pub fn load(encoded_vectors: TStorage, meta_path: &Path) -> std::io::Result<Self> {
        let contents = fs::read_to_string(meta_path)?;
        let metadata: Metadata = serde_json::from_str(&contents)?;
        let rotation_impl = RotationImpl::new(
            metadata.rotation,
            metadata.rotation_seed,
            metadata.vector_parameters.dim,
        );
        let result = Self {
            encoded_vectors,
            metadata,
            metadata_path: Some(meta_path.to_path_buf()),
            rotation_impl,
        };
        Ok(result)
    }

    /// Get quantized vector size in bytes:
    /// 4 bytes (f32 norm) + ⌈padded_dim × bits / 8⌉ bytes (packed indices).
    pub fn get_quantized_vector_size(
        vector_parameters: &VectorParameters,
        bits: usize,
        rotation: TqRotation,
    ) -> usize {
        let padded_dim = RotationImpl::compute_padded_dim(rotation, vector_parameters.dim);
        let codebook = Codebook::new(bits, padded_dim);
        NORM_SIZE + codebook.packed_size(padded_dim)
    }

    /// Decode stored bytes into (norm, norm-corrected centroid vector in rotated space).
    /// The returned vector has unit norm after correction.
    fn decode_rotated(bytes: &[u8], codebook: &Codebook, padded_dim: usize) -> (f32, Vec<f32>) {
        let norm = f32::from_ne_bytes(bytes[..NORM_SIZE].try_into().unwrap());
        let packed = &bytes[NORM_SIZE..];
        let y_tilde = codebook.decode_corrected(packed, padded_dim);
        (norm, y_tilde)
    }

    /// Dot product score in rotated space: ⟨q, x̃⟩ = γ · ⟨Rq, ŷ⟩
    fn score_dot(
        rotated_query: &[f32],
        bytes: &[u8],
        codebook: &Codebook,
        padded_dim: usize,
    ) -> f32 {
        let (gamma, y_corrected) = Self::decode_rotated(bytes, codebook, padded_dim);
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
        codebook: &Codebook,
        padded_dim: usize,
    ) -> f32 {
        let (gamma, y_corrected) = Self::decode_rotated(bytes, codebook, padded_dim);
        let dot: f32 = rotated_query
            .iter()
            .zip(y_corrected.iter())
            .map(|(&q, &v)| q * v)
            .sum();
        query_norm_sq + gamma * gamma - 2.0 * gamma * dot
    }

    /// L1 score requires full reconstruction via inverse rotation.
    fn score_l1(
        original_query: &[f32],
        bytes: &[u8],
        codebook: &Codebook,
        rotation_impl: &RotationImpl,
        dim: usize,
    ) -> f32 {
        let padded_dim = rotation_impl.padded_dim();
        let (gamma, y_corrected) = Self::decode_rotated(bytes, codebook, padded_dim);
        let x_hat = rotation_impl.apply_inverse(&y_corrected, dim);
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
            ),
            DistanceType::L2 => Self::score_l2(
                &query.rotated_query,
                query.query_norm_sq,
                bytes,
                &self.metadata.codebook,
                self.metadata.padded_dim,
            ),
            DistanceType::L1 => Self::score_l1(
                &query.original_query,
                bytes,
                &self.metadata.codebook,
                &self.rotation_impl,
                self.metadata.vector_parameters.dim,
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
        let rotated_query = self.rotation_impl.apply(query);
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

        let (gamma1, y1) = Self::decode_rotated(&v1, codebook, padded_dim);
        let (gamma2, y2) = Self::decode_rotated(&v2, codebook, padded_dim);

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
                let x1 = self.rotation_impl.apply_inverse(&y1, dim);
                let x2 = self.rotation_impl.apply_inverse(&y2, dim);
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
        NORM_SIZE + self.metadata.codebook.packed_size(self.metadata.padded_dim)
    }

    fn encode_internal_vector(&self, _id: PointOffsetType) -> Option<EncodedQueryTQ> {
        None
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
