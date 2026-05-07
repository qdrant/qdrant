pub(crate) mod encoding;
pub mod lloyd_max;
mod permutation;
pub mod quantization;
pub mod rotation;
pub mod simd;

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
use crate::encoded_storage::{EncodedStorage, EncodedStorageBuilder};
use crate::encoded_vectors::{EncodedVectors, VectorParameters, validate_vector_parameters};
use crate::quantile::find_quantile_interval_per_coordinate_with_preprocess;
use crate::turboquant::quantization::{ErrorCorrection, TurboQuantizer};
use crate::turboquant::simd::{Query1bitSimd, Query2bitSimd, Query4bitSimd};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TQBits {
    Bits4,
    Bits2,
    Bits1_5,
    Bits1,
}

impl TQBits {
    #[inline]
    fn bit_size(&self) -> u8 {
        match self {
            TQBits::Bits4 => 4,
            TQBits::Bits2 => 2,
            // 1.5 bits is implemented as 1 bit with x1.5 dimension padding
            TQBits::Bits1_5 => 1,
            TQBits::Bits1 => 1,
        }
    }

    /// Number of input vectors the TQ+ pre-pass uniformly samples and
    /// streams into the per-coord P-square estimators, scaled to the
    /// extremity of the target probability for this codebook.
    ///
    /// The TQ+ anchor probability is `p_outer = Φ(c_outer)` where
    /// `c_outer` is the outermost centroid magnitude. The variance of an
    /// order-statistic estimator at quantile `p` over a sample of size
    /// `R` is `p(1-p) / (R · f(F⁻¹(p))²)` where `f` is the source PDF.
    /// For N(0, 1)-like post-rotation data this gives:
    ///
    /// | Bits | p_outer | f(F⁻¹) | R    | σ     | rel. error |
    /// |------|---------|--------|------|-------|------------|
    /// | 1    | 0.787   | 0.290  | 2048 | 0.031 | 3.9%       |
    /// | 1.5  | 0.787   | 0.290  | 2048 | 0.031 | 3.9%       |
    /// | 2    | 0.934   | 0.128  | 4096 | 0.030 | 2.0%       |
    /// | 4    | 0.997   | 0.010  | 8192 | 0.063 | 2.3%       |
    ///
    /// We pick `R` per codebook so the absolute σ stays roughly flat
    /// (~0.03–0.06 in N(0, 1) units) instead of forcing the highest-bit
    /// budget on every codebook. Bits1/Bits1_5 sit at a moderate quantile
    /// where `R = 2048` is plenty; Bits4 needs `R = 8192` because its
    /// anchor sits in the deep tail.
    ///
    /// Memory: the pre-pass holds two P-square estimators per coord plus
    /// a `BATCH_SIZE × padded_dim` rotation scratch — total ≈ 400 KB at
    /// `padded_dim = 1536`, **independent of `R`**. Wall-time scales
    /// roughly linearly in `R`.
    #[inline]
    pub(crate) fn sample_size(&self) -> usize {
        match self {
            TQBits::Bits1 | TQBits::Bits1_5 => 2_048,
            TQBits::Bits2 => 4_096,
            TQBits::Bits4 => 8_192,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TQMode {
    Normal,
    Plus,
}

pub struct EncodedVectorsTQ<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    metadata_path: Option<PathBuf>,
    quantizer: TurboQuantizer,

    // Buffer used when encoding vectors.
    encoding_buffer: Vec<f64>,
}

/// Encoded query type for Turbo Quant.
pub struct EncodedQueryTQ {
    /// SIMD-encoded query for the asymmetric scoring path. For TQ+, the
    /// rotated query is pre-multiplied by `D' = 1/scale` per coord so the
    /// SIMD raw_dot computes `⟨Q · D', X+⟩` directly — the asymmetric score
    /// formulas then just add `ec_correction` and apply renorm's
    /// `scaling_factor`, identical to the Normal-mode path.
    data: EncodedQueryTQData,

    // Store the original query's l2 norm for Dot and L2 distances, where we can compute it once and reuse for all distance computations.
    l2_norm: Option<f32>,

    // Store the original query in pre-rotated form for L1 distance, where we need to dequantize vectors and apply inverse rotation to them.
    query: Option<Vec<f32>>,

    /// TQ+ asymmetric-scoring scalar correction `qm = ⟨Q, M⟩ = -⟨rotated_q, shift⟩`.
    /// `0.0` when EC is not configured. Added to the SIMD raw_dot so the
    /// existing score formulas stay unchanged.
    ec_correction: f32,
}

/// SIMD-ready encoded query, one variant per supported bit-width.  Each
/// variant wraps the bit-width's [`simd::Query{N}bitSimd`] precomputation
/// (rotation-applied query, quantized to the SIMD-friendly integer form);
/// on architectures without a matching SIMD instruction set the scalar
/// reference kernel inside each type takes over automatically.
///
/// `Bits1Wide` is the same kernel as `Bits1` but with 16-bit query
/// quantization instead of the default 8-bit (the kernel's max). Used in
/// TQ+ for 1-bit storage: the per-coord `D'` pre-scaling pushes some query
/// coords into the bottom of the 8-bit integer range, where rounding noise
/// is large relative to the signal — 16 bits gives the most headroom the
/// existing kernel supports.
pub enum EncodedQueryTQData {
    Bits1(Query1bitSimd),
    Bits1Wide(Query1bitSimd<16>),
    Bits2(Query2bitSimd),
    Bits4(Query4bitSimd),
}

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    pub vector_parameters: VectorParameters,
    pub bits: TQBits,
    pub mode: TQMode,
    pub error_correction: Option<ErrorCorrectionMetadata>,
}

/// On-disk form of TQ+'s [`ErrorCorrection`]. Stores only `shift` and `scale`
/// — derived caches like `D'_i²` and `⟨M, M⟩` are recomputed at load time so
/// `shift` / `scale` remain the single source of truth.
#[derive(Serialize, Deserialize)]
pub struct ErrorCorrectionMetadata {
    pub shift: Vec<f32>,
    pub scale: Vec<f32>,
}

/// Standard normal CDF Φ(x) = (1 + erf(x/√2)) / 2 via the Abramowitz & Stegun
/// 7.1.26 rational approximation (max error ≈ 1.5e-7). Used once per encode to
/// turn `c_outer` into the symmetric quantile probability for the TQ+ pre-pass.
fn phi(x: f64) -> f64 {
    let z = x / std::f64::consts::SQRT_2;
    let a = z.abs();
    let t = 1.0 / (1.0 + 0.3275911 * a);
    let poly = t
        * (0.254829592
            + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))));
    let erf = 1.0 - poly * (-a * a).exp();
    let erf = if z >= 0.0 { erf } else { -erf };
    0.5 * (1.0 + erf)
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
                let pre_quantizer = TurboQuantizer::new_from_metadata(&Metadata {
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
                let p_outer = phi(f64::from(c_outer));
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

        let quantizer = TurboQuantizer::new_from_metadata(&metadata).map_err(|e| {
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

        let quantizer = TurboQuantizer::new_from_metadata(&metadata)?;

        let result = Self {
            encoded_vectors,
            metadata,
            metadata_path: Some(meta_path.to_path_buf()),
            encoding_buffer: vec![0.0f64; quantizer.padded_dim],
            quantizer,
        };

        Ok(result)
    }

    // Get quantized vector size in bytes
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

impl<TStorage: EncodedStorage> EncodedVectors for EncodedVectorsTQ<TStorage> {
    type EncodedQuery = EncodedQueryTQ;

    fn is_on_disk(&self) -> bool {
        self.encoded_vectors.is_on_disk()
    }

    fn encode_query(&self, query: &[f32]) -> EncodedQueryTQ {
        self.quantizer.precompute_query(query)
    }

    fn for_each_in_batch<F>(&self, offsets: &[PointOffsetType], callback: F)
    where
        F: FnMut(usize, &[u8]),
    {
        self.encoded_vectors.for_each_in_batch(offsets, callback);
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
        Self::get_quantized_vector_size(
            &self.metadata.vector_parameters,
            self.metadata.bits,
            self.metadata.mode,
        )
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
