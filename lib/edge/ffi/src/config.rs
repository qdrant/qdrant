use std::collections::HashMap;

use segment::data_types::modifier::Modifier as SegmentModifier;
use segment::index::sparse_index::sparse_index_config::{
    SparseIndexConfig, SparseIndexType as SegmentSparseIndexType,
};
use segment::types::{
    BinaryQuantization, BinaryQuantizationConfig,
    BinaryQuantizationEncoding as SegmentBinaryQuantizationEncoding,
    BinaryQuantizationQueryEncoding as SegmentBinaryQuantizationQueryEncoding,
    CompressionRatio as SegmentCompressionRatio, Distance as SegmentDistance,
    HnswConfig as SegmentHnswConfig, Indexes, Memory as SegmentMemory,
    MultiVectorComparator as SegmentMultiVectorComparator,
    MultiVectorConfig as SegmentMultiVectorConfig, PayloadStorageType, ProductQuantization,
    ProductQuantizationConfig, QuantizationConfig as SegmentQuantizationConfig, ScalarQuantization,
    ScalarQuantizationConfig, ScalarType as SegmentScalarType, SegmentConfig,
    SparseVectorDataConfig as SegmentSparseVectorDataConfig, SparseVectorStorageType,
    TurboQuantBitSize as SegmentTurboQuantBitSize, TurboQuantQuantizationConfig, TurboQuantization,
    VectorDataConfig as SegmentVectorDataConfig,
    VectorStorageDatatype as SegmentVectorStorageDatatype, VectorStorageType,
};

// ── Distance ────────────────────────────────────────────────────────────────

/// The similarity metric used to compare two vectors.
///
/// The choice of distance must match the metric used to train the embeddings
/// that will be stored in the shard; for example, OpenAI `text-embedding-3`
/// embeddings expect `Cosine`.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum Distance {
    /// Cosine similarity. Vectors are compared by the angle between them;
    /// magnitude is ignored. Suitable for most text-embedding models.
    Cosine,
    /// Euclidean (L2) distance. Smaller values indicate more similar
    /// vectors.
    Euclid,
    /// Dot product. Larger values indicate more similar vectors; assumes
    /// vectors are pre-normalized if magnitude should not dominate.
    Dot,
    /// Manhattan (L1) distance — the sum of absolute coordinate differences.
    Manhattan,
}

impl From<Distance> for SegmentDistance {
    fn from(d: Distance) -> Self {
        match d {
            Distance::Cosine => SegmentDistance::Cosine,
            Distance::Euclid => SegmentDistance::Euclid,
            Distance::Dot => SegmentDistance::Dot,
            Distance::Manhattan => SegmentDistance::Manhattan,
        }
    }
}

impl From<SegmentDistance> for Distance {
    fn from(d: SegmentDistance) -> Self {
        match d {
            SegmentDistance::Cosine => Distance::Cosine,
            SegmentDistance::Euclid => Distance::Euclid,
            SegmentDistance::Dot => Distance::Dot,
            SegmentDistance::Manhattan => Distance::Manhattan,
        }
    }
}

// ── VectorStorageDatatype ───────────────────────────────────────────────────

/// The in-storage numeric datatype for vector components.
///
/// Lowering the datatype reduces memory and disk footprint at the cost of
/// some recall. `Float32` is the safe default; `Float16` halves memory with
/// negligible accuracy impact for most models; `Uint8` requires that
/// embeddings already be quantized into the `[0, 255]` range.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum VectorStorageDatatype {
    /// Full-precision 32-bit IEEE 754 floats.
    Float32,
    /// Half-precision 16-bit floats (IEEE 754 binary16).
    Float16,
    /// Unsigned 8-bit integers. Requires pre-quantized embeddings.
    Uint8,
    /// 4-bit TurboQuant storage datatype. This is the *storage* datatype the
    /// engine reports for a Turbo-quantized field; configure TurboQuant itself
    /// via [`QuantizationConfig::Turbo`], not by setting this datatype directly.
    Turbo4,
}

impl From<VectorStorageDatatype> for SegmentVectorStorageDatatype {
    fn from(d: VectorStorageDatatype) -> Self {
        match d {
            VectorStorageDatatype::Float32 => SegmentVectorStorageDatatype::Float32,
            VectorStorageDatatype::Float16 => SegmentVectorStorageDatatype::Float16,
            VectorStorageDatatype::Uint8 => SegmentVectorStorageDatatype::Uint8,
            VectorStorageDatatype::Turbo4 => SegmentVectorStorageDatatype::Turbo4,
        }
    }
}

impl From<SegmentVectorStorageDatatype> for VectorStorageDatatype {
    fn from(d: SegmentVectorStorageDatatype) -> Self {
        match d {
            SegmentVectorStorageDatatype::Float32 => VectorStorageDatatype::Float32,
            SegmentVectorStorageDatatype::Float16 => VectorStorageDatatype::Float16,
            SegmentVectorStorageDatatype::Uint8 => VectorStorageDatatype::Uint8,
            SegmentVectorStorageDatatype::Turbo4 => VectorStorageDatatype::Turbo4,
        }
    }
}

// ── MultiVectorComparator ───────────────────────────────────────────────────

/// The aggregation strategy used to collapse multi-vector scores into a
/// single point score.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum MultiVectorComparator {
    /// ColBERT-style max-similarity: for each query vector, take its best
    /// match across the point's vectors, then sum those maxima.
    MaxSim,
}

impl From<MultiVectorComparator> for SegmentMultiVectorComparator {
    fn from(c: MultiVectorComparator) -> Self {
        match c {
            MultiVectorComparator::MaxSim => SegmentMultiVectorComparator::MaxSim,
        }
    }
}

impl From<SegmentMultiVectorComparator> for MultiVectorComparator {
    fn from(c: SegmentMultiVectorComparator) -> Self {
        match c {
            SegmentMultiVectorComparator::MaxSim => MultiVectorComparator::MaxSim,
        }
    }
}

// ── MultiVectorConfig ───────────────────────────────────────────────────────

/// Configuration for a multi-vector field, where each point stores an
/// arbitrary number of vectors aggregated at query time.
#[derive(Clone, Debug, uniffi::Record)]
pub struct MultiVectorConfig {
    /// Strategy used to aggregate per-vector similarities into the final
    /// point score.
    pub comparator: MultiVectorComparator,
}

impl From<MultiVectorConfig> for SegmentMultiVectorConfig {
    fn from(c: MultiVectorConfig) -> Self {
        let MultiVectorConfig { comparator } = c;
        SegmentMultiVectorConfig {
            comparator: SegmentMultiVectorComparator::from(comparator),
        }
    }
}

impl From<SegmentMultiVectorConfig> for MultiVectorConfig {
    fn from(c: SegmentMultiVectorConfig) -> Self {
        let SegmentMultiVectorConfig { comparator } = c;
        MultiVectorConfig {
            comparator: MultiVectorComparator::from(comparator),
        }
    }
}

// ── ScalarType ──────────────────────────────────────────────────────────────

/// The target scalar type used by scalar quantization.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum ScalarType {
    /// Signed 8-bit integer scalars.
    Int8,
}

impl From<ScalarType> for SegmentScalarType {
    fn from(s: ScalarType) -> Self {
        match s {
            ScalarType::Int8 => SegmentScalarType::Int8,
        }
    }
}

impl From<SegmentScalarType> for ScalarType {
    fn from(s: SegmentScalarType) -> Self {
        match s {
            SegmentScalarType::Int8 => ScalarType::Int8,
        }
    }
}

// ── CompressionRatio ────────────────────────────────────────────────────────

/// Target compression ratio for product quantization.
///
/// Higher ratios reduce index size and speed up search at the cost of
/// recall. `X4` is conservative; `X64` is aggressive and typically requires
/// a reranking pass over the raw vectors to maintain quality.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum CompressionRatio {
    /// 4× compression.
    X4,
    /// 8× compression.
    X8,
    /// 16× compression.
    X16,
    /// 32× compression.
    X32,
    /// 64× compression.
    X64,
}

impl From<CompressionRatio> for SegmentCompressionRatio {
    fn from(c: CompressionRatio) -> Self {
        match c {
            CompressionRatio::X4 => SegmentCompressionRatio::X4,
            CompressionRatio::X8 => SegmentCompressionRatio::X8,
            CompressionRatio::X16 => SegmentCompressionRatio::X16,
            CompressionRatio::X32 => SegmentCompressionRatio::X32,
            CompressionRatio::X64 => SegmentCompressionRatio::X64,
        }
    }
}

impl From<SegmentCompressionRatio> for CompressionRatio {
    fn from(c: SegmentCompressionRatio) -> Self {
        match c {
            SegmentCompressionRatio::X4 => CompressionRatio::X4,
            SegmentCompressionRatio::X8 => CompressionRatio::X8,
            SegmentCompressionRatio::X16 => CompressionRatio::X16,
            SegmentCompressionRatio::X32 => CompressionRatio::X32,
            SegmentCompressionRatio::X64 => CompressionRatio::X64,
        }
    }
}

// ── BinaryQuantizationEncoding ──────────────────────────────────────────────

/// The number of bits each vector component is encoded to during binary
/// quantization.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum BinaryQuantizationEncoding {
    /// 1-bit-per-component encoding (maximum compression).
    OneBit,
    /// 2-bit-per-component encoding.
    TwoBits,
    /// Hybrid 1.5-bits-per-component encoding.
    OneAndHalfBits,
}

impl From<BinaryQuantizationEncoding> for SegmentBinaryQuantizationEncoding {
    fn from(e: BinaryQuantizationEncoding) -> Self {
        match e {
            BinaryQuantizationEncoding::OneBit => SegmentBinaryQuantizationEncoding::OneBit,
            BinaryQuantizationEncoding::TwoBits => SegmentBinaryQuantizationEncoding::TwoBits,
            BinaryQuantizationEncoding::OneAndHalfBits => {
                SegmentBinaryQuantizationEncoding::OneAndHalfBits
            }
        }
    }
}

impl From<SegmentBinaryQuantizationEncoding> for BinaryQuantizationEncoding {
    fn from(e: SegmentBinaryQuantizationEncoding) -> Self {
        match e {
            SegmentBinaryQuantizationEncoding::OneBit => BinaryQuantizationEncoding::OneBit,
            SegmentBinaryQuantizationEncoding::TwoBits => BinaryQuantizationEncoding::TwoBits,
            SegmentBinaryQuantizationEncoding::OneAndHalfBits => {
                BinaryQuantizationEncoding::OneAndHalfBits
            }
        }
    }
}

// ── BinaryQuantizationQueryEncoding ─────────────────────────────────────────

/// The encoding used for the query vector when searching a binary-quantized
/// field.
///
/// A finer query encoding trades a small amount of query-time memory for
/// better recall against a binary-quantized index.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum BinaryQuantizationQueryEncoding {
    /// Use the server default (currently equivalent to `Binary`).
    Default,
    /// Encode the query with the same binary scheme as the index.
    Binary,
    /// Encode the query as 4-bit scalars (higher recall, higher memory).
    Scalar4Bits,
    /// Encode the query as 8-bit scalars (highest recall, highest memory).
    Scalar8Bits,
}

impl From<BinaryQuantizationQueryEncoding> for SegmentBinaryQuantizationQueryEncoding {
    fn from(e: BinaryQuantizationQueryEncoding) -> Self {
        match e {
            BinaryQuantizationQueryEncoding::Default => {
                SegmentBinaryQuantizationQueryEncoding::Default
            }
            BinaryQuantizationQueryEncoding::Binary => {
                SegmentBinaryQuantizationQueryEncoding::Binary
            }
            BinaryQuantizationQueryEncoding::Scalar4Bits => {
                SegmentBinaryQuantizationQueryEncoding::Scalar4Bits
            }
            BinaryQuantizationQueryEncoding::Scalar8Bits => {
                SegmentBinaryQuantizationQueryEncoding::Scalar8Bits
            }
        }
    }
}

impl From<SegmentBinaryQuantizationQueryEncoding> for BinaryQuantizationQueryEncoding {
    fn from(e: SegmentBinaryQuantizationQueryEncoding) -> Self {
        match e {
            SegmentBinaryQuantizationQueryEncoding::Default => {
                BinaryQuantizationQueryEncoding::Default
            }
            SegmentBinaryQuantizationQueryEncoding::Binary => {
                BinaryQuantizationQueryEncoding::Binary
            }
            SegmentBinaryQuantizationQueryEncoding::Scalar4Bits => {
                BinaryQuantizationQueryEncoding::Scalar4Bits
            }
            SegmentBinaryQuantizationQueryEncoding::Scalar8Bits => {
                BinaryQuantizationQueryEncoding::Scalar8Bits
            }
        }
    }
}

// ── Memory ──────────────────────────────────────────────────────────────────

/// Memory placement of a component (quantized vectors, HNSW graph).
///
/// Data is always persisted on disk regardless of this setting; it only
/// controls how the data is held in RAM.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum Memory {
    /// Data is not pre-loaded from disk to RAM. Preferred for rarely queried
    /// components or components larger than RAM size. First request might be
    /// slow, but data is cached with usage.
    Cold,
    /// Data is pre-loaded into disk-cache RAM on start. First request is
    /// fast, but data may be evicted if there is not enough memory and some
    /// other component's data is used more frequently.
    Cached,
    /// Data is loaded in RAM and never evicted. First request is fast, but
    /// the component must fit in RAM at all times. Recommended for frequently
    /// queried small components like quantized vectors or primary indexes.
    Pinned,
}

impl From<Memory> for SegmentMemory {
    fn from(m: Memory) -> Self {
        match m {
            Memory::Cold => SegmentMemory::Cold,
            Memory::Cached => SegmentMemory::Cached,
            Memory::Pinned => SegmentMemory::Pinned,
        }
    }
}

impl From<SegmentMemory> for Memory {
    fn from(m: SegmentMemory) -> Self {
        match m {
            SegmentMemory::Cold => Memory::Cold,
            SegmentMemory::Cached => Memory::Cached,
            SegmentMemory::Pinned => Memory::Pinned,
        }
    }
}

// ── Quantization configs ────────────────────────────────────────────────────

/// Parameters for scalar quantization.
///
/// Scalar quantization reduces each component to a smaller integer type,
/// compressing the index ~4× while preserving most of the search quality.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ScalarQuantizationParams {
    /// Target scalar datatype (currently only `Int8` is supported).
    pub r#type: ScalarType,
    /// Optional quantile used to clip extreme values during calibration.
    /// `0.99` is a typical choice.
    #[uniffi(default = None)]
    pub quantile: Option<f32>,
    /// Memory placement of the quantized data. `None`/`null` follows the
    /// original vector storage placement.
    #[uniffi(default = None)]
    pub memory: Option<Memory>,
}

/// Parameters for product quantization.
///
/// Product quantization splits the vector into sub-vectors and encodes each
/// with its own codebook, achieving higher compression than scalar
/// quantization at the cost of more complex training.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ProductQuantizationParams {
    /// Target compression ratio.
    pub compression: CompressionRatio,
    /// Memory placement of the quantized data. `None`/`null` follows the
    /// original vector storage placement.
    #[uniffi(default = None)]
    pub memory: Option<Memory>,
}

/// Parameters for binary quantization.
///
/// Binary quantization encodes each vector component with a small number of
/// bits, providing maximum compression for ANN search with optional
/// rescoring.
#[derive(Clone, Debug, uniffi::Record)]
pub struct BinaryQuantizationParams {
    /// Memory placement of the quantized data. `None`/`null` follows the
    /// original vector storage placement.
    #[uniffi(default = None)]
    pub memory: Option<Memory>,
    /// Bits-per-component encoding for the stored index.
    #[uniffi(default = None)]
    pub encoding: Option<BinaryQuantizationEncoding>,
    /// Encoding used for the query vector at search time. Defaults to match
    /// the index encoding when not set.
    #[uniffi(default = None)]
    pub query_encoding: Option<BinaryQuantizationQueryEncoding>,
}

/// Bits-per-component for TurboQuant. Fewer bits = more compression, less recall.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum TurboQuantBitSize {
    /// 1 bit per component (maximum compression).
    Bits1,
    /// 1.5 bits per component. (Named `Bits1Point5`, not `Bits1_5`, so the
    /// generated Swift case reads as `bits1Point5` rather than the ambiguous
    /// `bits15`.)
    Bits1Point5,
    /// 2 bits per component.
    Bits2,
    /// 4 bits per component (default; best recall of the Turbo modes).
    Bits4,
}

impl From<TurboQuantBitSize> for SegmentTurboQuantBitSize {
    fn from(b: TurboQuantBitSize) -> Self {
        match b {
            TurboQuantBitSize::Bits1 => SegmentTurboQuantBitSize::Bits1,
            TurboQuantBitSize::Bits1Point5 => SegmentTurboQuantBitSize::Bits1_5,
            TurboQuantBitSize::Bits2 => SegmentTurboQuantBitSize::Bits2,
            TurboQuantBitSize::Bits4 => SegmentTurboQuantBitSize::Bits4,
        }
    }
}

impl From<SegmentTurboQuantBitSize> for TurboQuantBitSize {
    fn from(b: SegmentTurboQuantBitSize) -> Self {
        match b {
            SegmentTurboQuantBitSize::Bits1 => TurboQuantBitSize::Bits1,
            SegmentTurboQuantBitSize::Bits1_5 => TurboQuantBitSize::Bits1Point5,
            SegmentTurboQuantBitSize::Bits2 => TurboQuantBitSize::Bits2,
            SegmentTurboQuantBitSize::Bits4 => TurboQuantBitSize::Bits4,
        }
    }
}

/// Parameters for TurboQuant quantization.
#[derive(Clone, Debug, uniffi::Record)]
pub struct TurboQuantizationParams {
    /// Memory placement of the quantized data. `None`/`null` follows the
    /// original vector storage placement.
    #[uniffi(default = None)]
    pub memory: Option<Memory>,
    /// Bits-per-component. Defaults to `Bits4` when unset.
    #[uniffi(default = None)]
    pub bits: Option<TurboQuantBitSize>,
}

/// Selects a vector quantization strategy for a vector field.
///
/// Quantization trades a small amount of recall for lower memory and
/// faster queries. See the Qdrant docs for guidance on picking between
/// variants.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum QuantizationConfig {
    /// Scalar (int8) quantization — 4× compression, minor recall loss.
    Scalar { config: ScalarQuantizationParams },
    /// Product quantization — configurable compression, good for
    /// high-dimensional vectors.
    Product { config: ProductQuantizationParams },
    /// Binary quantization — maximum compression, best for very large
    /// collections and often paired with reranking.
    Binary { config: BinaryQuantizationParams },
    /// TurboQuant — bit-packed quantization (1 to 4 bits per component) tuned
    /// for fast on-device search.
    Turbo { config: TurboQuantizationParams },
}

// The FFI surface only carries the `memory` placement; the deprecated
// `always_ram` flags exist solely because the internal structs still declare
// them, and are always written as `None`.
#[allow(deprecated)]
impl From<QuantizationConfig> for SegmentQuantizationConfig {
    fn from(c: QuantizationConfig) -> Self {
        match c {
            QuantizationConfig::Scalar { config } => {
                let ScalarQuantizationParams {
                    r#type,
                    quantile,
                    memory,
                } = config;
                SegmentQuantizationConfig::Scalar(ScalarQuantization {
                    scalar: ScalarQuantizationConfig {
                        r#type: SegmentScalarType::from(r#type),
                        quantile,
                        always_ram: None,
                        memory: memory.map(SegmentMemory::from),
                    },
                })
            }
            QuantizationConfig::Product { config } => {
                let ProductQuantizationParams {
                    compression,
                    memory,
                } = config;
                SegmentQuantizationConfig::Product(ProductQuantization {
                    product: ProductQuantizationConfig {
                        compression: SegmentCompressionRatio::from(compression),
                        always_ram: None,
                        memory: memory.map(SegmentMemory::from),
                    },
                })
            }
            QuantizationConfig::Binary { config } => {
                let BinaryQuantizationParams {
                    memory,
                    encoding,
                    query_encoding,
                } = config;
                SegmentQuantizationConfig::Binary(BinaryQuantization {
                    binary: BinaryQuantizationConfig {
                        always_ram: None,
                        encoding: encoding.map(SegmentBinaryQuantizationEncoding::from),
                        query_encoding: query_encoding
                            .map(SegmentBinaryQuantizationQueryEncoding::from),
                        memory: memory.map(SegmentMemory::from),
                    },
                })
            }
            QuantizationConfig::Turbo { config } => {
                let TurboQuantizationParams { memory, bits } = config;
                SegmentQuantizationConfig::Turbo(TurboQuantization {
                    turbo: TurboQuantQuantizationConfig {
                        always_ram: None,
                        bits: bits.map(SegmentTurboQuantBitSize::from),
                        memory: memory.map(SegmentMemory::from),
                    },
                })
            }
        }
    }
}

// The deprecated `always_ram` flags are consumed only through
// `memory_placement()` (which resolves them for configs written by older
// tooling); the exhaustive destructuring must still name them.
#[allow(deprecated)]
impl TryFrom<SegmentQuantizationConfig> for QuantizationConfig {
    type Error = ();

    fn try_from(c: SegmentQuantizationConfig) -> std::result::Result<Self, Self::Error> {
        match c {
            SegmentQuantizationConfig::Scalar(ScalarQuantization { scalar }) => {
                let memory = scalar.memory_placement().map(Memory::from);
                let ScalarQuantizationConfig {
                    r#type,
                    quantile,
                    always_ram: _,
                    memory: _,
                } = scalar;
                Ok(QuantizationConfig::Scalar {
                    config: ScalarQuantizationParams {
                        r#type: ScalarType::from(r#type),
                        quantile,
                        memory,
                    },
                })
            }
            SegmentQuantizationConfig::Product(ProductQuantization { product }) => {
                let memory = product.memory_placement().map(Memory::from);
                let ProductQuantizationConfig {
                    compression,
                    always_ram: _,
                    memory: _,
                } = product;
                Ok(QuantizationConfig::Product {
                    config: ProductQuantizationParams {
                        compression: CompressionRatio::from(compression),
                        memory,
                    },
                })
            }
            SegmentQuantizationConfig::Binary(BinaryQuantization { binary }) => {
                let memory = binary.memory_placement().map(Memory::from);
                let BinaryQuantizationConfig {
                    always_ram: _,
                    encoding,
                    query_encoding,
                    memory: _,
                } = binary;
                Ok(QuantizationConfig::Binary {
                    config: BinaryQuantizationParams {
                        memory,
                        encoding: encoding.map(BinaryQuantizationEncoding::from),
                        query_encoding: query_encoding.map(BinaryQuantizationQueryEncoding::from),
                    },
                })
            }
            SegmentQuantizationConfig::Turbo(TurboQuantization { turbo }) => {
                let memory = turbo.memory_placement().map(Memory::from);
                let TurboQuantQuantizationConfig {
                    always_ram: _,
                    bits,
                    memory: _,
                } = turbo;
                Ok(QuantizationConfig::Turbo {
                    config: TurboQuantizationParams {
                        memory,
                        bits: bits.map(TurboQuantBitSize::from),
                    },
                })
            }
        }
    }
}

// ── HnswIndexConfig ───────────────────────────────────────────────────────────

/// HNSW index parameters for a dense vector field.
///
/// Set this on a `VectorDataConfig` to control the approximate-nearest-neighbour
/// index. With no HNSW config the field uses a plain (brute-force) index until
/// the optimizer runs; providing one lets you tune the recall/memory/speed
/// trade-off — important on memory-constrained devices.
///
/// The index is built by [`crate::EdgeShard::optimize`]; these parameters take
/// effect when that runs.
#[derive(Clone, Debug, uniffi::Record)]
pub struct HnswIndexConfig {
    /// Edges per node in the graph. Higher = better recall, more memory.
    #[uniffi(default = 16)]
    pub m: u64,
    /// Neighbours considered while building. Higher = better index, slower build.
    #[uniffi(default = 100)]
    pub ef_construct: u64,
    /// Below this size (KB) a full scan is preferred over HNSW traversal.
    #[uniffi(default = 10000)]
    pub full_scan_threshold: u64,
    /// Background index-building threads. `0` = auto-select.
    #[uniffi(default = 0)]
    pub max_indexing_threads: u64,
    /// Memory placement of the HNSW graph. Defaults to `Cached` when unset.
    #[uniffi(default = None)]
    pub memory: Option<Memory>,
    /// Custom `m` for the payload index graph; defaults to `m` when unset.
    #[uniffi(default = None)]
    pub payload_m: Option<u64>,
}

// The FFI surface only carries the `memory` placement; the deprecated
// `on_disk` flag exists solely because the internal struct still declares it,
// and is written as `None` / resolved through `Memory::resolve` on read-back
// for configs written by older tooling.
#[allow(deprecated)]
impl From<HnswIndexConfig> for SegmentHnswConfig {
    fn from(c: HnswIndexConfig) -> Self {
        let HnswIndexConfig {
            m,
            ef_construct,
            full_scan_threshold,
            max_indexing_threads,
            memory,
            payload_m,
        } = c;
        SegmentHnswConfig {
            m: crate::error::clamp_usize(m),
            ef_construct: crate::error::clamp_usize(ef_construct),
            full_scan_threshold: crate::error::clamp_usize(full_scan_threshold),
            max_indexing_threads: crate::error::clamp_usize(max_indexing_threads),
            on_disk: None,
            memory: memory.map(SegmentMemory::from),
            payload_m: payload_m.map(crate::error::clamp_usize),
            inline_storage: None,
        }
    }
}

#[allow(deprecated)]
impl From<SegmentHnswConfig> for HnswIndexConfig {
    fn from(c: SegmentHnswConfig) -> Self {
        let SegmentHnswConfig {
            m,
            ef_construct,
            full_scan_threshold,
            max_indexing_threads,
            on_disk,
            memory,
            payload_m,
            // Not exposed over FFI; requires quantization and is a
            // server-side disk/speed trade-off tuned at optimizer level.
            inline_storage: _,
        } = c;
        HnswIndexConfig {
            m: m as u64,
            ef_construct: ef_construct as u64,
            full_scan_threshold: full_scan_threshold as u64,
            max_indexing_threads: max_indexing_threads as u64,
            // None-preserving legacy resolution: unlike `memory_placement()`,
            // do not substitute the `Cached` default when nothing was set —
            // this is an "as-requested" read-back.
            memory: SegmentMemory::resolve(memory, on_disk.map(SegmentMemory::from_on_disk))
                .map(Memory::from),
            payload_m: payload_m.map(|v| v as u64),
        }
    }
}

// ── VectorDataConfig ────────────────────────────────────────────────────────

/// Configuration for a single dense vector field.
///
/// A named dense vector field is defined by its dimensionality (`size`), its
/// similarity metric (`distance`), and optional quantization and
/// multi-vector settings.
#[derive(Clone, Debug, uniffi::Record)]
pub struct VectorDataConfig {
    /// Number of components per vector. Must match the embedding model
    /// output dimensionality.
    pub size: u64,
    /// Similarity metric used for scoring.
    pub distance: Distance,
    /// Optional quantization strategy. `None` keeps raw vectors.
    #[uniffi(default = None)]
    pub quantization_config: Option<QuantizationConfig>,
    /// Optional multi-vector aggregation config. Set this when each point
    /// stores multiple vectors (e.g. ColBERT-style retrieval).
    #[uniffi(default = None)]
    pub multivector_config: Option<MultiVectorConfig>,
    /// Optional storage datatype; defaults to `Float32` when unset.
    #[uniffi(default = None)]
    pub datatype: Option<VectorStorageDatatype>,
    /// Optional HNSW index parameters. `None` uses a plain index until the
    /// optimizer builds one with default settings; set this to tune the
    /// recall/memory/speed trade-off. Built by [`crate::EdgeShard::optimize`].
    #[uniffi(default = None)]
    pub hnsw_config: Option<HnswIndexConfig>,
}

impl From<VectorDataConfig> for SegmentVectorDataConfig {
    fn from(c: VectorDataConfig) -> Self {
        let VectorDataConfig {
            size,
            distance,
            quantization_config,
            multivector_config,
            datatype,
            hnsw_config,
        } = c;
        SegmentVectorDataConfig {
            size: crate::error::clamp_usize(size),
            distance: SegmentDistance::from(distance),
            storage_type: VectorStorageType::InRamChunkedMmap,
            // The index travels via the `index` field: emit `Hnsw` when the host
            // supplied HNSW params (so `edge::EdgeConfig::from_segment_config`
            // picks them up and the optimizer builds an HNSW index), else `Plain`.
            index: match hnsw_config {
                Some(h) => Indexes::Hnsw(SegmentHnswConfig::from(h)),
                None => Indexes::Plain {},
            },
            quantization_config: quantization_config.map(SegmentQuantizationConfig::from),
            multivector_config: multivector_config.map(SegmentMultiVectorConfig::from),
            datatype: datatype.map(SegmentVectorStorageDatatype::from),
        }
    }
}

impl From<SegmentVectorDataConfig> for VectorDataConfig {
    fn from(c: SegmentVectorDataConfig) -> Self {
        let SegmentVectorDataConfig {
            size,
            distance,
            // The FFI storage type is fixed at `InRamChunkedMmap` on the write
            // path and not surfaced back.
            storage_type: _,
            index,
            quantization_config,
            multivector_config,
            datatype,
        } = c;
        VectorDataConfig {
            size: size as u64,
            distance: Distance::from(distance),
            quantization_config: quantization_config.and_then(|q| q.try_into().ok()),
            multivector_config: multivector_config.map(MultiVectorConfig::from),
            datatype: datatype.map(VectorStorageDatatype::from),
            hnsw_config: match index {
                Indexes::Hnsw(h) => Some(HnswIndexConfig::from(h)),
                Indexes::Plain {} => None,
            },
        }
    }
}

// ── SparseIndexType ─────────────────────────────────────────────────────────

/// Storage mode for a sparse vector index.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum SparseIndexType {
    /// In-RAM, mutable index. Supports updates; uses more memory.
    MutableRam,
    /// In-RAM, immutable index. Rebuilt on update; faster queries.
    ImmutableRam,
    /// Memory-mapped on-disk index. Lowest memory footprint.
    Mmap,
}

impl From<SparseIndexType> for SegmentSparseIndexType {
    fn from(t: SparseIndexType) -> Self {
        match t {
            SparseIndexType::MutableRam => SegmentSparseIndexType::MutableRam,
            SparseIndexType::ImmutableRam => SegmentSparseIndexType::ImmutableRam,
            SparseIndexType::Mmap => SegmentSparseIndexType::Mmap,
        }
    }
}

impl From<SegmentSparseIndexType> for SparseIndexType {
    fn from(t: SegmentSparseIndexType) -> Self {
        match t {
            SegmentSparseIndexType::MutableRam => SparseIndexType::MutableRam,
            SegmentSparseIndexType::ImmutableRam => SparseIndexType::ImmutableRam,
            SegmentSparseIndexType::Mmap => SparseIndexType::Mmap,
        }
    }
}

// ── Modifier ────────────────────────────────────────────────────────────────

/// Optional scoring adjustment applied on top of the raw vector similarity.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum Modifier {
    /// No modification; use the raw similarity score.
    None,
    /// Apply inverse-document-frequency weighting to sparse vector scores.
    /// Recommended for bag-of-words style sparse embeddings like BM42.
    Idf,
}

impl From<Modifier> for SegmentModifier {
    fn from(m: Modifier) -> Self {
        match m {
            Modifier::None => SegmentModifier::None,
            Modifier::Idf => SegmentModifier::Idf,
        }
    }
}

impl From<SegmentModifier> for Modifier {
    fn from(m: SegmentModifier) -> Self {
        match m {
            SegmentModifier::None => Modifier::None,
            SegmentModifier::Idf => Modifier::Idf,
        }
    }
}

// ── SparseVectorDataConfig ──────────────────────────────────────────────────

/// Configuration for a single sparse vector field (e.g. SPLADE, BM42).
#[derive(Clone, Debug, uniffi::Record)]
pub struct SparseVectorDataConfig {
    /// If set, switches to an exact full-scan search when the number of
    /// candidates falls below this threshold. Useful for small shards
    /// where ANN adds overhead without recall benefit.
    #[uniffi(default = None)]
    pub full_scan_threshold: Option<u64>,
    /// Optional storage datatype for sparse values; defaults to `Float32`.
    #[uniffi(default = None)]
    pub datatype: Option<VectorStorageDatatype>,
    /// Optional score modifier (e.g. IDF weighting).
    #[uniffi(default = None)]
    pub modifier: Option<Modifier>,
}

impl From<SparseVectorDataConfig> for SegmentSparseVectorDataConfig {
    fn from(c: SparseVectorDataConfig) -> Self {
        let SparseVectorDataConfig {
            full_scan_threshold,
            datatype,
            modifier,
        } = c;
        SegmentSparseVectorDataConfig {
            index: SparseIndexConfig {
                index_type: SegmentSparseIndexType::MutableRam,
                full_scan_threshold: full_scan_threshold.map(crate::error::clamp_usize),
                datatype: datatype.map(SegmentVectorStorageDatatype::from),
                memory: None,
            },
            storage_type: SparseVectorStorageType::Mmap,
            modifier: modifier.map(SegmentModifier::from),
        }
    }
}

impl From<SegmentSparseVectorDataConfig> for SparseVectorDataConfig {
    fn from(c: SegmentSparseVectorDataConfig) -> Self {
        let SegmentSparseVectorDataConfig {
            index:
                SparseIndexConfig {
                    // Fixed on the write path (`MutableRam`) and not surfaced
                    // back; placement of the sparse index is not an FFI knob.
                    index_type: _,
                    full_scan_threshold,
                    datatype,
                    memory: _,
                },
            storage_type: _,
            modifier,
        } = c;
        SparseVectorDataConfig {
            full_scan_threshold: full_scan_threshold.map(|v| v as u64),
            datatype: datatype.map(VectorStorageDatatype::from),
            modifier: modifier.map(Modifier::from),
        }
    }
}

// ── EdgeConfig (wraps SegmentConfig) ────────────────────────────────────────

/// Top-level configuration passed to [`EdgeShard::load`] when creating a new
/// shard, and returned by [`EdgeShard::config`] for existing shards.
///
/// An `EdgeConfig` defines one or more dense and/or sparse vector fields by
/// name. Each field is independently searchable; points may omit fields
/// they do not need.
///
/// ## Example
///
/// ```swift
/// let config = EdgeConfig(
///     vectorData: ["text": VectorDataConfig(size: 384, distance: .cosine)]
/// )
/// ```
///
/// ```kotlin
/// val config = EdgeConfig(
///     vectorData = mapOf(
///         "text" to VectorDataConfig(size = 384u, distance = Distance.COSINE)
///     ),
/// )
/// ```
///
/// [`EdgeShard::load`]: crate::EdgeShard::load
/// [`EdgeShard::config`]: crate::EdgeShard::config
#[derive(Clone, Debug, uniffi::Record)]
pub struct EdgeConfig {
    /// Named dense vector fields. Each key is a vector name referenced by
    /// `Point`, `Query.nearest(..., using: ...)`, etc.
    pub vector_data: HashMap<String, VectorDataConfig>,
    /// Named sparse vector fields.
    #[uniffi(default)]
    pub sparse_vector_data: HashMap<String, SparseVectorDataConfig>,
}

/// Inclusive bounds for a dense vector's dimensionality, mirroring the server's
/// `#[validate(range(min = 1, max = 65536))]` on `VectorParams.size`
/// (`segment::data_types::vector_name_config`). The FFI `From<VectorDataConfig>`
/// path builds a `SegmentVectorDataConfig` directly and so bypasses that
/// validator — we re-apply the same bound here (and in
/// [`UpdateOperation::create_dense_vector`](crate::update::UpdateOperation::create_dense_vector),
/// which likewise bypasses it).
pub(crate) const MIN_VECTOR_SIZE: u64 = 1;
pub(crate) const MAX_VECTOR_SIZE: u64 = 65_536;

// HNSW parameter bounds. These are validated (rejected with InvalidArgument)
// rather than saturated, because — unlike search-time tuning knobs — `m`,
// `payload_m` and `ef_construct` drive *eager, per-vector* allocations during
// `optimize()` (e.g. `LinksContainer::with_capacity(m * 2)` per point), and
// `max_indexing_threads` spawns OS threads. An unbounded host value would
// request a multi-terabyte allocation or a thread bomb and *abort* the process
// — an abort that `panic = "unwind"` cannot catch. The caps are far above any
// sane on-device value (the engine's own default `m` is 16, `ef_construct` 100).
// `ef_construct` also has an engine-side minimum of 4 that the FFI conversion
// path would otherwise bypass.
const MAX_HNSW_M: u64 = 2_048;
const MIN_HNSW_EF_CONSTRUCT: u64 = 4;
const MAX_HNSW_EF_CONSTRUCT: u64 = 100_000;
const MAX_HNSW_INDEXING_THREADS: u64 = 1_024;

/// Range-check host-supplied HNSW parameters: `m`/`payload_m` ≤ 2048,
/// `ef_construct` in `4..=100000`, `max_indexing_threads` ≤ 1024. These drive
/// eager per-vector allocations and thread spawning at `optimize()` time, so
/// an unbounded host value would abort the process (uncatchably) rather than
/// merely run slow — they must be rejected up front, not saturated.
///
/// Called from [`EdgeConfig::validate`] per vector field and from the
/// `EdgeShard::set_hnsw_config` / `set_vector_hnsw_config` setters, which all
/// bypass the server-side validators. `scope` names the config being checked
/// in the error message.
pub(crate) fn validate_hnsw(scope: &str, hnsw: &HnswIndexConfig) -> crate::error::Result<()> {
    if hnsw.m > MAX_HNSW_M {
        return Err(crate::error::EdgeError::invalid_argument(format!(
            "{scope}: hnsw m ({}) exceeds the maximum of {MAX_HNSW_M}",
            hnsw.m
        )));
    }
    if let Some(payload_m) = hnsw.payload_m
        && payload_m > MAX_HNSW_M
    {
        return Err(crate::error::EdgeError::invalid_argument(format!(
            "{scope}: hnsw payload_m ({payload_m}) exceeds the maximum of {MAX_HNSW_M}"
        )));
    }
    if hnsw.ef_construct < MIN_HNSW_EF_CONSTRUCT || hnsw.ef_construct > MAX_HNSW_EF_CONSTRUCT {
        return Err(crate::error::EdgeError::invalid_argument(format!(
            "{scope}: hnsw ef_construct ({}) is out of range \
             {MIN_HNSW_EF_CONSTRUCT}..={MAX_HNSW_EF_CONSTRUCT}",
            hnsw.ef_construct
        )));
    }
    if hnsw.max_indexing_threads > MAX_HNSW_INDEXING_THREADS {
        return Err(crate::error::EdgeError::invalid_argument(format!(
            "{scope}: hnsw max_indexing_threads ({}) exceeds the maximum of \
             {MAX_HNSW_INDEXING_THREADS}",
            hnsw.max_indexing_threads
        )));
    }
    Ok(())
}

impl EdgeConfig {
    /// Reject host config that would crash the engine, so a host gets a
    /// catchable `InvalidArgument` instead of an uncatchable engine crash.
    /// Called from `load` before any conversion.
    ///
    /// Per dense vector field, **dimensionality** must be in `1..=65536`. A
    /// `size` of `0` would flow into the engine and panic/abort (uncatchable)
    /// rather than surfacing a clean error; a huge `size` would request an
    /// enormous allocation. The server validates this range on its own
    /// deserialization path, but the FFI `From<VectorDataConfig>` path bypasses
    /// that validator.
    ///
    /// **HNSW parameters** (when a field sets `hnsw_config`) are range-checked
    /// via [`validate_hnsw`].
    ///
    /// Quantization is NOT restricted here: the FFI surface exposes the same
    /// four strategies (Scalar/Product/Binary/Turbo) as the Python Edge SDK, for
    /// parity. Note that Edge only builds *appendable* segments, and the engine's
    /// `for_appendable_segment` filter keeps only `Binary`/`Turbo` there —
    /// `Scalar`/`Product` are dropped by the engine (shared behavior across all
    /// Edge SDKs), not by this SDK.
    pub(crate) fn validate(&self) -> crate::error::Result<()> {
        for (name, vd) in &self.vector_data {
            if vd.size < MIN_VECTOR_SIZE || vd.size > MAX_VECTOR_SIZE {
                return Err(crate::error::EdgeError::invalid_argument(format!(
                    "vector field {name:?}: size {} is out of range \
                     {MIN_VECTOR_SIZE}..={MAX_VECTOR_SIZE}",
                    vd.size
                )));
            }
            if let Some(hnsw) = &vd.hnsw_config {
                validate_hnsw(&format!("vector field {name:?}"), hnsw)?;
            }
        }
        Ok(())
    }
}

// ── OptimizersConfig ────────────────────────────────────────────────────────

/// Optimizer tuning applied by [`EdgeShard::set_optimizers_config`], driving
/// what [`EdgeShard::optimize`] does. Unset fields keep engine defaults.
///
/// [`EdgeShard::set_optimizers_config`]: crate::EdgeShard::set_optimizers_config
/// [`EdgeShard::optimize`]: crate::EdgeShard::optimize
#[derive(Clone, Debug, uniffi::Record)]
pub struct OptimizersConfig {
    /// Minimal fraction of deleted vectors in a segment required to run
    /// vacuum.
    #[uniffi(default = None)]
    pub deleted_threshold: Option<f64>,
    /// Minimal number of vectors in a segment required to run vacuum.
    #[uniffi(default = None)]
    pub vacuum_min_vector_number: Option<u64>,
    /// Target number of segments; `0` picks automatically from CPU count.
    #[uniffi(default = None)]
    pub default_segment_number: Option<u64>,
    /// Maximum segment size in KB; unset derives it from CPU count.
    #[uniffi(default = None)]
    pub max_segment_size_kb: Option<u64>,
    /// Segments above this size (KB) get an HNSW index when optimized.
    #[uniffi(default = None)]
    pub indexing_threshold_kb: Option<u64>,
    /// If `true`, points written to unoptimized segments above the indexing
    /// threshold are deferred: persisted but hidden from reads until
    /// [`EdgeShard::optimize`](crate::EdgeShard::optimize) runs.
    #[uniffi(default = None)]
    pub prevent_unoptimized: Option<bool>,
}

impl From<OptimizersConfig> for edge::EdgeOptimizersConfig {
    fn from(c: OptimizersConfig) -> Self {
        let OptimizersConfig {
            deleted_threshold,
            vacuum_min_vector_number,
            default_segment_number,
            max_segment_size_kb,
            indexing_threshold_kb,
            prevent_unoptimized,
        } = c;
        edge::EdgeOptimizersConfig {
            deleted_threshold,
            vacuum_min_vector_number: vacuum_min_vector_number.map(crate::error::clamp_usize),
            default_segment_number: default_segment_number.map(crate::error::clamp_usize),
            max_segment_size: max_segment_size_kb.map(crate::error::clamp_usize),
            indexing_threshold: indexing_threshold_kb.map(crate::error::clamp_usize),
            prevent_unoptimized,
        }
    }
}

impl From<EdgeConfig> for SegmentConfig {
    fn from(c: EdgeConfig) -> Self {
        let EdgeConfig {
            vector_data,
            sparse_vector_data,
        } = c;
        SegmentConfig {
            vector_data: vector_data
                .into_iter()
                .map(|(k, v)| (k, SegmentVectorDataConfig::from(v)))
                .collect(),
            sparse_vector_data: sparse_vector_data
                .into_iter()
                .map(|(k, v)| (k, SegmentSparseVectorDataConfig::from(v)))
                .collect(),
            payload_storage_type: PayloadStorageType::Mmap,
        }
    }
}

impl From<SegmentConfig> for EdgeConfig {
    fn from(c: SegmentConfig) -> Self {
        let SegmentConfig {
            vector_data,
            sparse_vector_data,
            // Fixed on the write path (`Mmap`); not an FFI knob.
            payload_storage_type: _,
        } = c;
        EdgeConfig {
            vector_data: vector_data
                .into_iter()
                .map(|(k, v)| (k, VectorDataConfig::from(v)))
                .collect(),
            sparse_vector_data: sparse_vector_data
                .into_iter()
                .map(|(k, v)| (k, SparseVectorDataConfig::from(v)))
                .collect(),
        }
    }
}

/// Honest read-back from the engine's rich `edge::EdgeConfig`.
///
/// Used by `EdgeShard::config()` instead of the lossy `plain_segment_config()`
/// projection (which hardcodes a plain index and runs quantization through the
/// `for_appendable_segment` filter, so HNSW and the originally-requested
/// quantization would be dropped from the read-back). `edge::EdgeConfig` keeps
/// HNSW and quantization both per-vector and globally; here we resolve each
/// field as `per_vector.or(global)`, mirroring how the engine actually applies
/// them, so `config()` reflects what the host requested.
impl From<&edge::EdgeConfig> for EdgeConfig {
    fn from(c: &edge::EdgeConfig) -> Self {
        let edge::EdgeConfig {
            vectors,
            sparse_vectors,
            // Shard-global quantization is a fallback for per-vector configs
            // (resolved below); the remaining engine-level knobs (payload/WAL
            // placement, optimizers, search threads, and the always-populated
            // global HNSW — see the per-vector comment below) have no FFI
            // surface.
            quantization_config: global_quantization_config,
            hnsw_config: _,
            on_disk_payload: _,
            optimizers: _,
            wal_options: _,
            max_search_threads: _,
        } = c;
        let vector_data = vectors
            .iter()
            .map(|(name, p)| {
                let edge::EdgeVectorParams {
                    size,
                    distance,
                    // Storage placement is fixed by the FFI write path and not
                    // surfaced back.
                    on_disk: _,
                    multivector_config,
                    datatype,
                    quantization_config,
                    hnsw_config,
                } = p;
                // This is an "as-requested" read-back: each field reflects what
                // the host configured, NOT what the engine will effectively
                // apply. So we read the per-vector value (falling back to the
                // shard-global one Edge stores), and crucially do NOT substitute
                // the engine's default when the host set nothing — a field
                // configured without HNSW reads back as `None`, matching how the
                // quantization field behaves. (The engine's *apply* path uses
                // `unwrap_or(default)`; that's a different contract.)
                let quant = quantization_config
                    .clone()
                    .or_else(|| global_quantization_config.clone())
                    // `.ok()` cannot drop a real config: `TryFrom` is total over
                    // the closed `SegmentQuantizationConfig` enum (every variant
                    // maps to `Ok`). It only guards a hypothetical future engine
                    // variant with no FFI equivalent.
                    .and_then(|q| QuantizationConfig::try_from(q).ok());
                // Read the per-vector HNSW only. `edge::EdgeConfig.hnsw_config`
                // is a non-optional global that's always populated (the engine
                // default when unset), so falling back to it would report HNSW
                // for a field the host left as a plain index — the asymmetry we
                // are fixing. The per-vector field is `None` exactly when the
                // host requested no HNSW.
                let hnsw = hnsw_config.map(HnswIndexConfig::from);
                (
                    name.clone(),
                    VectorDataConfig {
                        size: *size as u64,
                        distance: Distance::from(*distance),
                        quantization_config: quant,
                        multivector_config: multivector_config.map(MultiVectorConfig::from),
                        datatype: datatype.map(VectorStorageDatatype::from),
                        hnsw_config: hnsw,
                    },
                )
            })
            .collect();
        let sparse_vector_data = sparse_vectors
            .iter()
            .map(|(name, p)| {
                let edge::EdgeSparseVectorParams {
                    full_scan_threshold,
                    // Storage placement is fixed by the FFI write path and not
                    // surfaced back.
                    on_disk: _,
                    modifier,
                    datatype,
                } = p;
                (
                    name.clone(),
                    SparseVectorDataConfig {
                        full_scan_threshold: full_scan_threshold.map(|v| v as u64),
                        datatype: datatype.map(VectorStorageDatatype::from),
                        modifier: modifier.map(Modifier::from),
                    },
                )
            })
            .collect();
        EdgeConfig {
            vector_data,
            sparse_vector_data,
        }
    }
}
