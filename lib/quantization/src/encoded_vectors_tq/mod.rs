mod codebook;
pub mod coordinate_analysis;
mod encoded_query;
mod encoded_vector;
mod packing;
mod qjl;
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
pub use encoded_query::EncodedQueryTQ;
pub use encoded_vector::EncodedVectorTQ;
use encoded_vector::{NORM_SIZE, encode_vector_data, normalize_and_rotate};
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
    QjlShort,
    QjlShortNormalization,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum TqRotation {
    NoRotation,
    #[default]
    Hadamard,
    RotationMatrix,
}

// ============================================================================
// Encode a single vector
// ============================================================================

/// Compute per-coordinate medians and scales from training data (TQ+ mode).
///
/// For each coordinate `i`:
/// - `median[i]` = median of all rotated values for coordinate `i`
/// - `scale[i]` = expected_sigma / sigma_i (normalizes variance to match codebook)
fn compute_plus_stats(
    data: impl Iterator<Item = impl AsRef<[f32]>> + Clone,
    rotation_impl: &RotationImpl,
    padded_dim: usize,
) -> (Vec<f32>, Vec<f32>) {
    let expected_sigma = 1.0 / (padded_dim as f32).sqrt();

    // Collect per-coordinate values
    let mut coord_values: Vec<Vec<f32>> = vec![Vec::new(); padded_dim];
    for vector in data {
        let rotated = normalize_and_rotate(vector.as_ref(), rotation_impl);
        for (i, &val) in rotated.iter().enumerate() {
            coord_values[i].push(val);
        }
    }

    let mut medians = vec![0.0f32; padded_dim];
    let mut scales = vec![1.0f32; padded_dim];

    for i in 0..padded_dim {
        let values = &mut coord_values[i];
        if values.is_empty() {
            continue;
        }

        // Median
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = values.len();
        let median = if n % 2 == 0 {
            (values[n / 2 - 1] + values[n / 2]) / 2.0
        } else {
            values[n / 2]
        };

        // Standard deviation
        let mean: f32 = values.iter().sum::<f32>() / n as f32;
        let variance: f32 = values.iter().map(|&v| (v - mean) * (v - mean)).sum::<f32>() / n as f32;
        let sigma = variance.sqrt();

        if sigma > 1e-10 {
            medians[i] = median;
            scales[i] = expected_sigma / sigma;
        }
        // else: keep median=0, scale=1 (no correction for near-zero variance)
    }

    (medians, scales)
}

// ============================================================================
// Main TurboQuant structures
// ============================================================================

pub struct EncodedVectorsTQ<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    metadata_path: Option<PathBuf>,
    rotation_impl: RotationImpl,
    qjl: Option<qjl::QjlProjection>,
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
    /// Per-coordinate medians (padded_dim elements). Only present when plus=true.
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub medians: Vec<f32>,
    /// Per-coordinate scales (padded_dim elements). Only present when plus=true.
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub scales: Vec<f32>,
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
    /// 3. (TQ+) Shift and scale each coordinate to match codebook distribution
    /// 4. Scalar-quantize each coordinate using Lloyd-Max optimal codebook
    /// 5. Pack norm + quantized indices into compact binary format
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

        let qjl = match correction {
            TqCorrection::Qjl | TqCorrection::QjlNormalization => {
                Some(qjl::QjlProjection::new(padded_dim, padded_dim))
            }
            TqCorrection::QjlShort | TqCorrection::QjlShortNormalization => {
                Some(qjl::QjlProjection::new(padded_dim, qjl::QJL_SHORT_DIM))
            }
            _ => None,
        };

        // TQ+: compute per-coordinate medians and scales
        let (medians, scales) = if plus {
            compute_plus_stats(data.clone(), &rotation_impl, padded_dim)
        } else {
            (Vec::new(), Vec::new())
        };

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
                .map(|v| normalize_and_rotate(v.as_ref(), &rotation_impl))
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

            // TQ+: also analyze the ready-to-quantize vectors (after shift+scale)
            if plus {
                let plus_vectors: Vec<Vec<f32>> = rotated_vectors
                    .iter()
                    .map(|r| {
                        r.iter()
                            .enumerate()
                            .map(|(i, &v)| (v - medians[i]) * scales[i])
                            .collect()
                    })
                    .collect();
                coordinate_analysis::analyse(
                    plus_vectors.iter(),
                    &rotated_params,
                    _count,
                    &analysis_run_dir,
                    "after_plus",
                );
            }
        }

        for vector in data {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let encoded = encode_vector_data(
                vector.as_ref(),
                &rotation_impl,
                &codebook,
                &medians,
                &scales,
                qjl.as_ref(),
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
            rotation_seed,
            codebook,
            padded_dim,
            correction,
            rotation,
            hadamard_chunk,
            plus,
            medians,
            scales,
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
            qjl,
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
        let qjl = match metadata.correction {
            TqCorrection::Qjl | TqCorrection::QjlNormalization => {
                Some(qjl::QjlProjection::new(metadata.padded_dim, metadata.padded_dim))
            }
            TqCorrection::QjlShort | TqCorrection::QjlShortNormalization => {
                Some(qjl::QjlProjection::new(metadata.padded_dim, qjl::QJL_SHORT_DIM))
            }
            _ => None,
        };
        let result = Self {
            encoded_vectors,
            metadata,
            metadata_path: Some(meta_path.to_path_buf()),
            rotation_impl,
            qjl,
        };
        Ok(result)
    }

    /// Get quantized vector size in bytes:
    /// 4 bytes (f32 norm) + ⌈padded_dim × bits / 8⌉ bytes (packed indices)
    /// + optionally 4 bytes (residual norm) + ⌈padded_dim / 8⌉ bytes (QJL bits).
    pub fn get_quantized_vector_size(
        vector_parameters: &VectorParameters,
        bits: usize,
        rotation: TqRotation,
        correction: TqCorrection,
    ) -> usize {
        let padded_dim = RotationImpl::compute_padded_dim(rotation, vector_parameters.dim);
        let codebook = Codebook::new(bits, padded_dim);
        let base = NORM_SIZE + codebook.packed_size(padded_dim);
        match correction {
            TqCorrection::Qjl | TqCorrection::QjlNormalization => {
                base + NORM_SIZE + (padded_dim + 7) / 8
            }
            TqCorrection::QjlShort | TqCorrection::QjlShortNormalization => {
                base + NORM_SIZE + (qjl::QJL_SHORT_DIM + 7) / 8
            }
            _ => base,
        }
    }

    fn score_tq(&self, query: &EncodedQueryTQ, bytes: &[u8]) -> f32 {
        let medians = &self.metadata.medians;
        let scales = &self.metadata.scales;
        let correction = self.metadata.correction;
        let qjl = self.qjl.as_ref();
        let vec = EncodedVectorTQ::new(bytes);
        let result = match self.metadata.vector_parameters.distance_type {
            DistanceType::Dot => {
                if let (Some(projected), Some(qjl)) = (&query.qjl_projected_query, qjl) {
                    vec.score_dot_fast(
                        &query.rotated_query,
                        &self.metadata.codebook,
                        self.metadata.padded_dim,
                        medians,
                        scales,
                        correction,
                        qjl,
                        projected,
                    )
                } else {
                    vec.score_dot(
                        &query.rotated_query,
                        &self.metadata.codebook,
                        self.metadata.padded_dim,
                        medians,
                        scales,
                        correction,
                        qjl,
                    )
                }
            }
            DistanceType::L2 => {
                if let (Some(projected), Some(qjl)) = (&query.qjl_projected_query, qjl) {
                    vec.score_l2_fast(
                        &query.rotated_query,
                        query.query_norm_sq,
                        &self.metadata.codebook,
                        self.metadata.padded_dim,
                        medians,
                        scales,
                        correction,
                        qjl,
                        projected,
                    )
                } else {
                    vec.score_l2(
                        &query.rotated_query,
                        query.query_norm_sq,
                        &self.metadata.codebook,
                        self.metadata.padded_dim,
                        medians,
                        scales,
                        correction,
                        qjl,
                    )
                }
            }
            DistanceType::L1 => vec.score_l1(
                &query.original_query,
                &self.metadata.codebook,
                &self.rotation_impl,
                self.metadata.vector_parameters.dim,
                medians,
                scales,
                correction,
                qjl,
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

        // Precompute S · weighted_query for O(d) QJL correction in Dot/L2 scoring.
        // Only for Qjl mode (not QjlNormalization — normalization is nonlinear).
        let qjl_projected_query = if matches!(
            self.metadata.correction,
            TqCorrection::Qjl | TqCorrection::QjlShort
        ) {
            self.qjl.as_ref().map(|qjl| {
                if self.metadata.scales.is_empty() {
                    qjl.project_query(&rotated_query)
                } else {
                    let weighted: Vec<f32> = rotated_query
                        .iter()
                        .zip(self.metadata.scales.iter())
                        .map(|(&q, &s)| q / s)
                        .collect();
                    qjl.project_query(&weighted)
                }
            })
        } else {
            None
        };

        EncodedQueryTQ {
            rotated_query,
            original_query: query.to_vec(),
            query_norm_sq,
            qjl_projected_query,
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
        let medians = &self.metadata.medians;
        let scales = &self.metadata.scales;
        let correction = self.metadata.correction;

        let vec1 = EncodedVectorTQ::new(&v1);
        let vec2 = EncodedVectorTQ::new(&v2);
        let (gamma1, y1) = vec1.decode_rotated(
            codebook,
            padded_dim,
            medians,
            scales,
            correction,
            None, // Skip QJL because it's quadratic time
        );
        let (gamma2, y2) = vec2.decode_rotated(
            codebook,
            padded_dim,
            medians,
            scales,
            correction,
            None, // Skip QJL because it's quadratic time
        );

        let result = match self.metadata.vector_parameters.distance_type {
            DistanceType::Dot => {
                let dot: f32 = y1.iter().zip(y2.iter()).map(|(&a, &b)| a * b).sum();
                gamma1 * gamma2 * dot
            }
            DistanceType::L2 => {
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
        let base = NORM_SIZE + self.metadata.codebook.packed_size(self.metadata.padded_dim);
        if let Some(qjl) = &self.qjl {
            base + qjl.extra_bytes()
        } else {
            base
        }
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
