use std::collections::HashMap;

use segment::data_types::modifier::Modifier as SegmentModifier;
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType as SegmentSparseIndexType};
use segment::types::{
    BinaryQuantization, BinaryQuantizationConfig, BinaryQuantizationEncoding as SegmentBinaryQuantizationEncoding,
    BinaryQuantizationQueryEncoding as SegmentBinaryQuantizationQueryEncoding,
    CompressionRatio as SegmentCompressionRatio, Distance as SegmentDistance,
    HnswConfig as SegmentHnswConfig,
    Indexes, MultiVectorComparator as SegmentMultiVectorComparator,
    MultiVectorConfig as SegmentMultiVectorConfig, PayloadStorageType,
    ProductQuantization, ProductQuantizationConfig,
    QuantizationConfig as SegmentQuantizationConfig,
    ScalarQuantization, ScalarQuantizationConfig, ScalarType as SegmentScalarType,
    SegmentConfig, SparseVectorDataConfig as SegmentSparseVectorDataConfig,
    SparseVectorStorageType, TurboQuantBitSize as SegmentTurboQuantBitSize,
    TurboQuantQuantizationConfig, TurboQuantization,
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
        SegmentMultiVectorConfig {
            comparator: SegmentMultiVectorComparator::from(c.comparator),
        }
    }
}

impl From<SegmentMultiVectorConfig> for MultiVectorConfig {
    fn from(c: SegmentMultiVectorConfig) -> Self {
        MultiVectorConfig {
            comparator: MultiVectorComparator::from(c.comparator),
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
            BinaryQuantizationQueryEncoding::Default => SegmentBinaryQuantizationQueryEncoding::Default,
            BinaryQuantizationQueryEncoding::Binary => SegmentBinaryQuantizationQueryEncoding::Binary,
            BinaryQuantizationQueryEncoding::Scalar4Bits => SegmentBinaryQuantizationQueryEncoding::Scalar4Bits,
            BinaryQuantizationQueryEncoding::Scalar8Bits => SegmentBinaryQuantizationQueryEncoding::Scalar8Bits,
        }
    }
}

impl From<SegmentBinaryQuantizationQueryEncoding> for BinaryQuantizationQueryEncoding {
    fn from(e: SegmentBinaryQuantizationQueryEncoding) -> Self {
        match e {
            SegmentBinaryQuantizationQueryEncoding::Default => BinaryQuantizationQueryEncoding::Default,
            SegmentBinaryQuantizationQueryEncoding::Binary => BinaryQuantizationQueryEncoding::Binary,
            SegmentBinaryQuantizationQueryEncoding::Scalar4Bits => BinaryQuantizationQueryEncoding::Scalar4Bits,
            SegmentBinaryQuantizationQueryEncoding::Scalar8Bits => BinaryQuantizationQueryEncoding::Scalar8Bits,
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
    pub quantile: Option<f32>,
    /// If `true`, keep the quantized data in RAM; otherwise allow it to be
    /// memory-mapped.
    pub always_ram: Option<bool>,
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
    /// If `true`, keep the quantized data in RAM; otherwise allow it to be
    /// memory-mapped.
    pub always_ram: Option<bool>,
}

/// Parameters for binary quantization.
///
/// Binary quantization encodes each vector component with a small number of
/// bits, providing maximum compression for ANN search with optional
/// rescoring.
#[derive(Clone, Debug, uniffi::Record)]
pub struct BinaryQuantizationParams {
    /// If `true`, keep the quantized data in RAM; otherwise allow it to be
    /// memory-mapped.
    pub always_ram: Option<bool>,
    /// Bits-per-component encoding for the stored index.
    pub encoding: Option<BinaryQuantizationEncoding>,
    /// Encoding used for the query vector at search time. Defaults to match
    /// the index encoding when not set.
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
    /// If `true`, keep the quantized data in RAM; otherwise allow it to be
    /// memory-mapped.
    pub always_ram: Option<bool>,
    /// Bits-per-component. Defaults to `Bits4` when unset.
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

impl From<QuantizationConfig> for SegmentQuantizationConfig {
    fn from(c: QuantizationConfig) -> Self {
        match c {
            QuantizationConfig::Scalar { config } => {
                SegmentQuantizationConfig::Scalar(ScalarQuantization {
                    scalar: ScalarQuantizationConfig {
                        r#type: SegmentScalarType::from(config.r#type),
                        quantile: config.quantile,
                        always_ram: config.always_ram,
                    },
                })
            }
            QuantizationConfig::Product { config } => {
                SegmentQuantizationConfig::Product(ProductQuantization {
                    product: ProductQuantizationConfig {
                        compression: SegmentCompressionRatio::from(config.compression),
                        always_ram: config.always_ram,
                    },
                })
            }
            QuantizationConfig::Binary { config } => {
                SegmentQuantizationConfig::Binary(BinaryQuantization {
                    binary: BinaryQuantizationConfig {
                        always_ram: config.always_ram,
                        encoding: config.encoding.map(SegmentBinaryQuantizationEncoding::from),
                        query_encoding: config
                            .query_encoding
                            .map(SegmentBinaryQuantizationQueryEncoding::from),
                    },
                })
            }
            QuantizationConfig::Turbo { config } => {
                SegmentQuantizationConfig::Turbo(TurboQuantization {
                    turbo: TurboQuantQuantizationConfig {
                        always_ram: config.always_ram,
                        bits: config.bits.map(SegmentTurboQuantBitSize::from),
                    },
                })
            }
        }
    }
}

impl TryFrom<SegmentQuantizationConfig> for QuantizationConfig {
    type Error = ();

    fn try_from(c: SegmentQuantizationConfig) -> std::result::Result<Self, Self::Error> {
        match c {
            SegmentQuantizationConfig::Scalar(ScalarQuantization { scalar }) => {
                Ok(QuantizationConfig::Scalar {
                    config: ScalarQuantizationParams {
                        r#type: ScalarType::from(scalar.r#type),
                        quantile: scalar.quantile,
                        always_ram: scalar.always_ram,
                    },
                })
            }
            SegmentQuantizationConfig::Product(ProductQuantization { product }) => {
                Ok(QuantizationConfig::Product {
                    config: ProductQuantizationParams {
                        compression: CompressionRatio::from(product.compression),
                        always_ram: product.always_ram,
                    },
                })
            }
            SegmentQuantizationConfig::Binary(BinaryQuantization { binary }) => {
                Ok(QuantizationConfig::Binary {
                    config: BinaryQuantizationParams {
                        always_ram: binary.always_ram,
                        encoding: binary.encoding.map(BinaryQuantizationEncoding::from),
                        query_encoding: binary
                            .query_encoding
                            .map(BinaryQuantizationQueryEncoding::from),
                    },
                })
            }
            SegmentQuantizationConfig::Turbo(TurboQuantization { turbo }) => {
                Ok(QuantizationConfig::Turbo {
                    config: TurboQuantizationParams {
                        always_ram: turbo.always_ram,
                        bits: turbo.bits.map(TurboQuantBitSize::from),
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
    pub m: u64,
    /// Neighbours considered while building. Higher = better index, slower build.
    pub ef_construct: u64,
    /// Below this size (KB) a full scan is preferred over HNSW traversal.
    pub full_scan_threshold: u64,
    /// Background index-building threads. `0` = auto-select.
    pub max_indexing_threads: u64,
    /// Store the HNSW index on disk instead of RAM. Defaults to `false`.
    pub on_disk: Option<bool>,
    /// Custom `m` for the payload index graph; defaults to `m` when unset.
    pub payload_m: Option<u64>,
}

impl From<HnswIndexConfig> for SegmentHnswConfig {
    fn from(c: HnswIndexConfig) -> Self {
        SegmentHnswConfig {
            m: crate::error::clamp_usize(c.m),
            ef_construct: crate::error::clamp_usize(c.ef_construct),
            full_scan_threshold: crate::error::clamp_usize(c.full_scan_threshold),
            max_indexing_threads: crate::error::clamp_usize(c.max_indexing_threads),
            on_disk: c.on_disk,
            payload_m: c.payload_m.map(crate::error::clamp_usize),
            inline_storage: None,
        }
    }
}

impl From<SegmentHnswConfig> for HnswIndexConfig {
    fn from(c: SegmentHnswConfig) -> Self {
        HnswIndexConfig {
            m: c.m as u64,
            ef_construct: c.ef_construct as u64,
            full_scan_threshold: c.full_scan_threshold as u64,
            max_indexing_threads: c.max_indexing_threads as u64,
            on_disk: c.on_disk,
            payload_m: c.payload_m.map(|v| v as u64),
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
    pub quantization_config: Option<QuantizationConfig>,
    /// Optional multi-vector aggregation config. Set this when each point
    /// stores multiple vectors (e.g. ColBERT-style retrieval).
    pub multivector_config: Option<MultiVectorConfig>,
    /// Optional storage datatype; defaults to `Float32` when unset.
    pub datatype: Option<VectorStorageDatatype>,
    /// Optional HNSW index parameters. `None` uses a plain index until the
    /// optimizer builds one with default settings; set this to tune the
    /// recall/memory/speed trade-off. Built by [`crate::EdgeShard::optimize`].
    pub hnsw_config: Option<HnswIndexConfig>,
}

impl From<VectorDataConfig> for SegmentVectorDataConfig {
    fn from(c: VectorDataConfig) -> Self {
        SegmentVectorDataConfig {
            size: crate::error::clamp_usize(c.size),
            distance: SegmentDistance::from(c.distance),
            storage_type: VectorStorageType::InRamChunkedMmap,
            // The index travels via the `index` field: emit `Hnsw` when the host
            // supplied HNSW params (so `edge::EdgeConfig::from_segment_config`
            // picks them up and the optimizer builds an HNSW index), else `Plain`.
            index: match c.hnsw_config {
                Some(h) => Indexes::Hnsw(SegmentHnswConfig::from(h)),
                None => Indexes::Plain {},
            },
            quantization_config: c.quantization_config.map(SegmentQuantizationConfig::from),
            multivector_config: c.multivector_config.map(SegmentMultiVectorConfig::from),
            datatype: c.datatype.map(SegmentVectorStorageDatatype::from),
        }
    }
}

impl From<SegmentVectorDataConfig> for VectorDataConfig {
    fn from(c: SegmentVectorDataConfig) -> Self {
        let hnsw_config = match c.index {
            Indexes::Hnsw(h) => Some(HnswIndexConfig::from(h)),
            Indexes::Plain {} => None,
        };
        VectorDataConfig {
            size: c.size as u64,
            distance: Distance::from(c.distance),
            quantization_config: c.quantization_config.and_then(|q| q.try_into().ok()),
            multivector_config: c.multivector_config.map(MultiVectorConfig::from),
            datatype: c.datatype.map(VectorStorageDatatype::from),
            hnsw_config,
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
    pub full_scan_threshold: Option<u64>,
    /// Optional storage datatype for sparse values; defaults to `Float32`.
    pub datatype: Option<VectorStorageDatatype>,
    /// Optional score modifier (e.g. IDF weighting).
    pub modifier: Option<Modifier>,
}

impl From<SparseVectorDataConfig> for SegmentSparseVectorDataConfig {
    fn from(c: SparseVectorDataConfig) -> Self {
        SegmentSparseVectorDataConfig {
            index: SparseIndexConfig {
                index_type: SegmentSparseIndexType::MutableRam,
                full_scan_threshold: c.full_scan_threshold.map(crate::error::clamp_usize),
                datatype: c.datatype.map(SegmentVectorStorageDatatype::from),
            },
            storage_type: SparseVectorStorageType::Mmap,
            modifier: c.modifier.map(SegmentModifier::from),
        }
    }
}

impl From<SegmentSparseVectorDataConfig> for SparseVectorDataConfig {
    fn from(c: SegmentSparseVectorDataConfig) -> Self {
        SparseVectorDataConfig {
            full_scan_threshold: c.index.full_scan_threshold.map(|v| v as u64),
            datatype: c.index.datatype.map(VectorStorageDatatype::from),
            modifier: c.modifier.map(Modifier::from),
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
///     vectorData: ["text": VectorDataConfig(size: 384, distance: .cosine,
///                                           quantizationConfig: nil,
///                                           multivectorConfig: nil,
///                                           datatype: nil)],
///     sparseVectorData: [:]
/// )
/// ```
///
/// ```kotlin
/// val config = EdgeConfig(
///     vectorData = mapOf(
///         "text" to VectorDataConfig(
///             size = 384u,
///             distance = Distance.COSINE,
///             quantizationConfig = null,
///             multivectorConfig = null,
///             datatype = null,
///         )
///     ),
///     sparseVectorData = emptyMap(),
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
    pub sparse_vector_data: HashMap<String, SparseVectorDataConfig>,
}

/// Inclusive bounds for a dense vector's dimensionality, mirroring the server's
/// `#[validate(range(min = 1, max = 65536))]` on `VectorParams.size`
/// (`segment::data_types::vector_name_config`). The FFI `From<VectorDataConfig>`
/// path builds a `SegmentVectorDataConfig` directly and so bypasses that
/// validator — we re-apply the same bound here.
const MIN_VECTOR_SIZE: u64 = 1;
const MAX_VECTOR_SIZE: u64 = 65_536;

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
    /// **HNSW parameters** (when a field sets `hnsw_config`) are range-checked:
    /// `m`/`payload_m` ≤ 2048, `ef_construct` in `4..=100000`,
    /// `max_indexing_threads` ≤ 1024. These drive eager per-vector allocations
    /// and thread spawning at `optimize()` time, so an unbounded host value
    /// would abort the process (uncatchably) rather than merely run slow — they
    /// must be rejected up front, not saturated.
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
                if hnsw.m > MAX_HNSW_M {
                    return Err(crate::error::EdgeError::invalid_argument(format!(
                        "vector field {name:?}: hnsw m ({}) exceeds the maximum of {MAX_HNSW_M}",
                        hnsw.m
                    )));
                }
                if let Some(payload_m) = hnsw.payload_m
                    && payload_m > MAX_HNSW_M
                {
                    return Err(crate::error::EdgeError::invalid_argument(format!(
                        "vector field {name:?}: hnsw payload_m ({payload_m}) exceeds the maximum of {MAX_HNSW_M}"
                    )));
                }
                if hnsw.ef_construct < MIN_HNSW_EF_CONSTRUCT || hnsw.ef_construct > MAX_HNSW_EF_CONSTRUCT {
                    return Err(crate::error::EdgeError::invalid_argument(format!(
                        "vector field {name:?}: hnsw ef_construct ({}) is out of range \
                         {MIN_HNSW_EF_CONSTRUCT}..={MAX_HNSW_EF_CONSTRUCT}",
                        hnsw.ef_construct
                    )));
                }
                if hnsw.max_indexing_threads > MAX_HNSW_INDEXING_THREADS {
                    return Err(crate::error::EdgeError::invalid_argument(format!(
                        "vector field {name:?}: hnsw max_indexing_threads ({}) exceeds the maximum of {MAX_HNSW_INDEXING_THREADS}",
                        hnsw.max_indexing_threads
                    )));
                }
            }
        }
        Ok(())
    }
}

impl From<EdgeConfig> for SegmentConfig {
    fn from(c: EdgeConfig) -> Self {
        SegmentConfig {
            vector_data: c
                .vector_data
                .into_iter()
                .map(|(k, v)| (k, SegmentVectorDataConfig::from(v)))
                .collect(),
            sparse_vector_data: c
                .sparse_vector_data
                .into_iter()
                .map(|(k, v)| (k, SegmentSparseVectorDataConfig::from(v)))
                .collect(),
            payload_storage_type: PayloadStorageType::Mmap,
        }
    }
}

impl From<SegmentConfig> for EdgeConfig {
    fn from(c: SegmentConfig) -> Self {
        EdgeConfig {
            vector_data: c
                .vector_data
                .into_iter()
                .map(|(k, v)| (k, VectorDataConfig::from(v)))
                .collect(),
            sparse_vector_data: c
                .sparse_vector_data
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
        let vector_data = c
            .vectors
            .iter()
            .map(|(name, p)| {
                // This is an "as-requested" read-back: each field reflects what
                // the host configured, NOT what the engine will effectively
                // apply. So we read the per-vector value (falling back to the
                // shard-global one Edge stores), and crucially do NOT substitute
                // the engine's default when the host set nothing — a field
                // configured without HNSW reads back as `None`, matching how the
                // quantization field behaves. (The engine's *apply* path uses
                // `unwrap_or(default)`; that's a different contract.)
                let quant = p
                    .quantization_config
                    .clone()
                    .or_else(|| c.quantization_config.clone())
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
                let hnsw = p.hnsw_config.map(HnswIndexConfig::from);
                (
                    name.clone(),
                    VectorDataConfig {
                        size: p.size as u64,
                        distance: Distance::from(p.distance),
                        quantization_config: quant,
                        multivector_config: p.multivector_config.map(MultiVectorConfig::from),
                        datatype: p.datatype.map(VectorStorageDatatype::from),
                        hnsw_config: hnsw,
                    },
                )
            })
            .collect();
        let sparse_vector_data = c
            .sparse_vectors
            .iter()
            .map(|(name, p)| {
                (
                    name.clone(),
                    SparseVectorDataConfig {
                        full_scan_threshold: p.full_scan_threshold.map(|v| v as u64),
                        datatype: p.datatype.map(VectorStorageDatatype::from),
                        modifier: p.modifier.map(Modifier::from),
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
