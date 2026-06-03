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

use crate::EncodingError;
use crate::encoded_storage::{EncodedStorage, EncodedStorageBuilder, validate_storage_vector_size};
use crate::encoded_vectors::{EncodedVectors, VectorParameters, validate_vector_parameters};
use crate::quantile::find_quantile_interval_per_coordinate_with_preprocess;
use crate::turboquant::math::std_normal_cdf;
use crate::turboquant::quantization::{ErrorCorrection, TurboQuantizer};
use crate::turboquant::{EncodedQueryTQ, TQBits, TQMode};

pub struct EncodedVectorsTQ<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    metadata_path: Option<PathBuf>,
    quantizer: TurboQuantizer,

    // Buffer used when encoding vectors.
    encoding_buffer: Vec<f64>,
}

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    pub vector_parameters: VectorParameters,
    pub bits: TQBits,
    pub mode: TQMode,
    pub error_correction: Option<ErrorCorrectionMetadata>,
}

/// Initialize a new TurboQuantizer from metadata. Returns `Err` if the
/// persisted `ErrorCorrection` shift/scale lengths don't match the
/// expected `padded_dim`.
pub fn new_turbo_quantizer_from_metadata(metadata: &Metadata) -> std::io::Result<TurboQuantizer> {
    let padded_dim = TurboQuantizer::padded_dim(metadata.vector_parameters.dim, metadata.bits);
    let error_correction = metadata
        .error_correction
        .as_ref()
        .map(|m| new_error_correction_from_metadata(m, padded_dim))
        .transpose()?;
    Ok(TurboQuantizer::new(
        metadata.vector_parameters.dim,
        metadata.bits,
        metadata.mode,
        metadata.vector_parameters.distance_type,
        error_correction,
    ))
}

/// Reconstruct from persisted metadata, validating that `shift` and
/// `scale` have the expected length. Returns an error if the metadata
/// has been truncated or otherwise corrupted.
fn new_error_correction_from_metadata(
    metadata: &ErrorCorrectionMetadata,
    padded_dim: usize,
) -> std::io::Result<ErrorCorrection> {
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
    Ok(ErrorCorrection::new(
        metadata.shift.clone(),
        metadata.scale.clone(),
    ))
}

/// On-disk form of TQ+'s [`ErrorCorrection`]. Stores only `shift` and `scale`
/// — derived caches like `D'_i²` and `⟨M, M⟩` are recomputed at load time so
/// `shift` / `scale` remain the single source of truth.
#[derive(Serialize, Deserialize)]
pub struct ErrorCorrectionMetadata {
    pub shift: Vec<f32>,
    pub scale: Vec<f32>,
}

impl<TStorage: EncodedStorage> EncodedVectorsTQ<TStorage> {
    pub fn storage(&self) -> &TStorage {
        &self.encoded_vectors
    }

    /// Encode vector data
    ///
    /// # Arguments
    /// * `data` - iterator over original vector data
    /// * `storage_builder` - encoding result storage builder
    /// * `vector_parameters` - parameters of original vector data (dimension, distance, etc)
    /// * `count` - number of vectors in `data` iterator
    /// * `bits` - bits for quantization
    /// * `mode` - quantization mode
    /// * `num_threads` - max threads to use for the TQ+ quantile pre-pass
    /// * `meta_path` - optional path to save metadata, if `None`, metadata will not be saved
    /// * `stopped` - Atomic bool that indicates if encoding should be stopped
    #[allow(clippy::too_many_arguments)]
    pub fn encode<'a>(
        data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone + 'a,
        mut storage_builder: impl EncodedStorageBuilder<Storage = TStorage>,
        vector_parameters: &VectorParameters,
        count: usize,
        bits: TQBits,
        mode: TQMode,
        num_threads: usize,
        meta_path: Option<&Path>,
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        debug_assert!(validate_vector_parameters(data.clone(), vector_parameters).is_ok());

        // TQ+: first pass over `data` to fit per-coordinate shift/scale that
        // pulls the rotated, length-rescaled coordinates onto the Lloyd-Max
        // N(0, 1) codebook before quantization.
        //
        // Per-coord shift/scale are derived from the empirical quantiles at
        // probabilities `Phi(±c_outer)`, where `c_outer` is the outermost
        // codebook centroid magnitude. For ideally-N(0, 1) data the empirical
        // quantile at `Phi(c_outer)` equals `c_outer`, so shift/scale collapse
        // to `(0, 1)`. For anisotropic data the quantile-anchored fit avoids
        // the bias of mean/stddev under heavy-tailed or skewed coords.
        let error_correction = match mode {
            TQMode::Normal => None,
            TQMode::Plus => {
                let pre_quantizer = new_turbo_quantizer_from_metadata(&Metadata {
                    vector_parameters: *vector_parameters,
                    bits,
                    mode,
                    error_correction: None,
                })
                .map_err(|e| {
                    EncodingError::EncodingError(format!(
                        "Failed to construct pre-quantizer for TQ+ stats pass: {e}",
                    ))
                })?;
                let padded_dim = pre_quantizer.padded_dim;

                // Use the outermost centroid magnitude as the per-coord
                // anchor: for symmetric N(0, 1) Lloyd-Max codebooks this
                // pins the highest-magnitude bucket onto its design value.
                let centroids = bits.get_centroids();
                let c_outer = centroids
                    .iter()
                    .copied()
                    .fold(0.0_f32, |acc, c| acc.max(c.abs()));
                let p_outer = std_normal_cdf(f64::from(c_outer));
                // `find_interval_per_coordinate` interprets `quantile` as the
                // symmetric interval `[(1-q)/2, 1-(1-q)/2]`, so 2·p_outer − 1
                // gives us the `[1-p_outer, p_outer]` pair we want.
                let quantile_param = ((2.0 * p_outer - 1.0) as f32).clamp(0.0, 0.999_99);

                // Streaming pre-pass: uniformly samples `bits.sample_size()`
                // input vectors, runs Hadamard rotation + length rescale +
                // f64→f32 narrowing on the fly, and pushes each rotated
                // value straight into the per-coord P-square estimators —
                // no reservoir, no intermediate `Vec<Vec<f32>>`, only a
                // small fixed-size batch scratch (~192 KB at padded_dim=1536).
                let pre_quantizer_ref = &pre_quantizer;
                let intervals = find_quantile_interval_per_coordinate_with_preprocess(
                    data.clone(),
                    vector_parameters.dim,
                    padded_dim,
                    count,
                    quantile_param,
                    num_threads,
                    bits.sample_size(),
                    move |raw, scratch| {
                        pre_quantizer_ref.preprocess_into(raw, scratch);
                    },
                    stopped,
                )?;

                // Minimum quantile-interval width below which we skip EC
                // for that coordinate (leave `scale = 1`). `f32::EPSILON`
                // would only catch *exactly* zero variance — a coord with
                // `denom = 1e-5` (real σ ≈ 1e-5 in N(0, 1) units, i.e.
                // effectively constant but with floating-point jitter)
                // would still go through and produce `scale ≈ 2·c_outer / 1e-5`
                // ≈ 1.6e5, amplifying numeric noise enough to dominate the
                // codebook for that axis. `1e-3` corresponds to a per-coord
                // σ < ~3e-4 in N(0, 1) units, which is well below anything
                // the codebook can usefully resolve, and bounds the worst-
                // case `scale` at ~5.5e3 even for Bits4.
                const MIN_QUANTILE_WIDTH: f32 = 1e-3;
                let mut shift = vec![0.0f32; padded_dim];
                let mut scale = vec![1.0f32; padded_dim];
                for (i, &(q_lo, q_hi)) in intervals.iter().enumerate() {
                    // shift recenters the q_lo/q_hi pair around 0, scale
                    // stretches it onto [-c_outer, c_outer]. The width
                    // floor protects near-constant coords from blowing
                    // `scale` up to where quantization noise dominates.
                    shift[i] = -(q_lo + q_hi) / 2.0;
                    let denom = q_hi - q_lo;
                    if denom > MIN_QUANTILE_WIDTH {
                        scale[i] = (2.0 * c_outer) / denom;
                    }
                }
                Some(ErrorCorrection::new(shift, scale))
            }
        };

        let metadata = Metadata {
            vector_parameters: *vector_parameters,
            bits,
            mode,
            error_correction: error_correction.as_ref().map(|ec| ErrorCorrectionMetadata {
                shift: ec.shift.clone(),
                scale: ec.scale.clone(),
            }),
        };

        let quantizer = new_turbo_quantizer_from_metadata(&metadata).map_err(|e| {
            EncodingError::EncodingError(format!(
                "Failed to construct quantizer from metadata: {e}",
            ))
        })?;
        let mut buf = vec![0.0f64; quantizer.padded_dim];

        for vector in data {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let encoded_vector: Vec<u8> =
                Self::encode_vector(vector.as_ref(), &quantizer, &mut buf);

            storage_builder
                .push_vector_data(&encoded_vector)
                .map_err(|e| {
                    EncodingError::EncodingError(format!("Failed to push encoded vector: {e}",))
                })?;
        }

        let encoded_vectors = storage_builder
            .build()
            .map_err(|e| EncodingError::EncodingError(format!("Failed to build storage: {e}",)))?;

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
            encoding_buffer: vec![0.0f64; quantizer.padded_dim],
            quantizer,
        })
    }

    pub fn load(encoded_vectors: TStorage, meta_path: &Path) -> std::io::Result<Self> {
        let contents = fs::read_to_string(meta_path)?;
        let metadata: Metadata = serde_json::from_str(&contents)?;

        let quantizer = new_turbo_quantizer_from_metadata(&metadata)?;

        let result = Self {
            encoded_vectors,
            metadata,
            metadata_path: Some(meta_path.to_path_buf()),
            encoding_buffer: vec![0.0f64; quantizer.padded_dim],
            quantizer,
        };

        // Validate the storage's vector size against the metadata once here, so the size
        // invariant the scoring hot path relies on (it splits each stored vector into packed
        // dimensions plus a fixed-size trailer) also holds in release builds without a per-score
        // check.
        validate_storage_vector_size(&result.encoded_vectors, result.quantized_vector_size())?;

        Ok(result)
    }

    fn encode_vector(
        vector_data: &[f32],
        turbo_quantizer: &TurboQuantizer,
        buf: &mut [f64],
    ) -> Vec<u8> {
        turbo_quantizer.quantize(vector_data, buf)
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

/// Get quantized vector size in bytes
pub fn get_quantized_vector_size(
    vector_parameters: &VectorParameters,
    bits: TQBits,
    mode: TQMode,
) -> usize {
    TurboQuantizer::quantized_size_for(
        vector_parameters.dim,
        bits,
        vector_parameters.distance_type,
        mode,
    )
}

impl<TStorage: EncodedStorage> EncodedVectors for EncodedVectorsTQ<TStorage> {
    type EncodedQuery = EncodedQueryTQ;

    fn is_in_ram_or_mmap() -> bool {
        TStorage::is_in_ram_or_mmap()
    }

    fn is_on_disk(&self) -> bool {
        self.encoded_vectors.is_on_disk()
    }

    fn encode_query(&self, query: &[f32]) -> EncodedQueryTQ {
        self.quantizer.precompute_query(query)
    }

    fn iter_batch(
        &self,
        offsets: &[PointOffsetType],
    ) -> impl Iterator<Item = (usize, Cow<'_, [u8]>)> {
        self.encoded_vectors.iter_batch(offsets)
    }

    fn score(
        &self,
        query: &Self::EncodedQuery,
        encoded_vector: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        self.score_bytes(True, query, encoded_vector, hw_counter)
    }

    fn score_point(
        &self,
        query: &EncodedQueryTQ,
        i: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let encoded_vector = self.encoded_vectors.get_vector_data(i);
        self.score_bytes(True, query, &encoded_vector, hw_counter)
    }

    /// Score two points inside endoded data by their indexes
    fn score_internal(
        &self,
        i: PointOffsetType,
        j: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let v1 = self.encoded_vectors.get_vector_data(i);
        let v2 = self.encoded_vectors.get_vector_data(j);

        hw_counter.vector_io_read().incr_delta(v1.len() + v2.len());

        let score = self.quantizer.score_symmetric(&v1, &v2);
        if self.metadata.vector_parameters.invert {
            -score
        } else {
            score
        }
    }

    fn quantized_vector_size(&self) -> usize {
        get_quantized_vector_size(
            &self.metadata.vector_parameters,
            self.metadata.bits,
            self.metadata.mode,
        )
    }

    fn heap_size_bytes(&self) -> usize {
        let Self {
            encoded_vectors,
            metadata: _,
            metadata_path: _,
            quantizer,
            encoding_buffer,
        } = self;
        // Storage backend (the quantized vectors themselves; non-zero for the
        // RAM-backed variants), plus the always-resident quantizer tables and
        // the per-instance encoding scratch buffer.
        encoded_vectors.heap_size_bytes()
            + quantizer.heap_size_bytes()
            + encoding_buffer.capacity() * size_of::<f64>()
    }

    fn encode_internal_vector(&self, _id: PointOffsetType) -> Option<EncodedQueryTQ> {
        // Turbo quant is asymmetric, so we cannot encode internal vectors, only queries.
        // This method is used for symmetric quantization,
        // where we can encode internal vectors without access to original vector data,
        // which may require disk access.
        None
    }

    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[f32],
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        let encoded_vector =
            Self::encode_vector(vector, &self.quantizer, &mut self.encoding_buffer);
        self.encoded_vectors.upsert_vector(
            id,
            bytemuck::cast_slice(encoded_vector.as_slice()),
            hw_counter,
        )
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
        let score = self.quantizer.score_precomputed(query, bytes);
        if self.metadata.vector_parameters.invert {
            -score
        } else {
            score
        }
    }
}
