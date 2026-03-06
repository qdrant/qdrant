use std::collections::HashMap;

use segment::data_types::modifier::Modifier;
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::types::{
    Distance, HnswConfig, Indexes, MultiVectorConfig, PayloadStorageType, QuantizationConfig,
    SegmentConfig, SparseVectorDataConfig, SparseVectorStorageType, VectorDataConfig,
    VectorNameBuf, VectorStorageDatatype, VectorStorageType,
};

pub const TEMP_SEGMENTS_PATH: &str = "temp_segments";
pub const DEFAULT_MAX_SEGMENT_PER_CPU_KB: usize = 256_000;
pub const DEFAULT_INDEXING_THRESHOLD_KB: usize = 10_000;
pub const DEFAULT_DELETED_THRESHOLD: f64 = 0.2;
pub const DEFAULT_VACUUM_MIN_VECTOR_NUMBER: usize = 1000;

#[derive(Debug, Clone, PartialEq)]
pub struct DenseVectorOptimizerConfig {
    pub on_disk: Option<bool>,
    pub hnsw_config: HnswConfig,
    pub quantization_config: Option<QuantizationConfig>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SparseVectorOptimizerConfig {
    pub on_disk: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct SegmentOptimizerConfig {
    pub payload_storage_type: PayloadStorageType,
    pub base_vector_data: HashMap<VectorNameBuf, VectorDataConfig>,
    pub base_sparse_vector_data: HashMap<VectorNameBuf, SparseVectorDataConfig>,
    pub dense_vector: HashMap<VectorNameBuf, DenseVectorOptimizerConfig>,
    pub sparse_vector: HashMap<VectorNameBuf, SparseVectorOptimizerConfig>,
}

impl SegmentOptimizerConfig {
    pub fn base_segment_config(&self) -> SegmentConfig {
        SegmentConfig {
            vector_data: self.base_vector_data.clone(),
            sparse_vector_data: self.base_sparse_vector_data.clone(),
            payload_storage_type: self.payload_storage_type,
        }
    }
}

/// Per-dense-vector input for the optimizer builder.
#[derive(Debug, Clone)]
pub struct DenseVectorOptimizerInput {
    pub size: usize,
    pub distance: Distance,
    pub on_disk: Option<bool>,
    pub hnsw_config: HnswConfig,
    pub quantization_config: Option<QuantizationConfig>,
    pub multivector_config: Option<MultiVectorConfig>,
    pub datatype: Option<VectorStorageDatatype>,
}

/// Per-sparse-vector input for the optimizer builder.
#[derive(Debug, Clone)]
pub struct SparseVectorOptimizerInput {
    pub on_disk: Option<bool>,
    pub full_scan_threshold: Option<usize>,
    pub index_datatype: Option<VectorStorageDatatype>,
    pub storage_type: SparseVectorStorageType,
    pub modifier: Option<Modifier>,
}

/// Minimal input for building [`SegmentOptimizerConfig`].
///
/// Both the collection and edge/embedded paths construct this struct from their
/// own config types, then call [`OptimizerSourceConfig::build`] to produce
/// the unified [`SegmentOptimizerConfig`].
#[derive(Debug, Clone)]
pub struct OptimizerSourceConfig {
    pub payload_storage_type: PayloadStorageType,
    pub dense_vectors: HashMap<VectorNameBuf, DenseVectorOptimizerInput>,
    pub sparse_vectors: HashMap<VectorNameBuf, SparseVectorOptimizerInput>,
}

impl OptimizerSourceConfig {
    /// Construct from a [`SegmentConfig`] (edge/embedded path).
    ///
    /// `fallback_hnsw` is used for vectors that have `Indexes::Plain` (no HNSW
    /// config stored yet). Typically this is inferred from the first HNSW-indexed
    /// vector in the shard, or `HnswConfig::default()`.
    pub fn from_segment_config(segment_config: &SegmentConfig, fallback_hnsw: HnswConfig) -> Self {
        let dense_vectors = segment_config
            .vector_data
            .iter()
            .map(|(name, config)| {
                let VectorDataConfig {
                    size,
                    distance,
                    storage_type,
                    index,
                    quantization_config,
                    multivector_config,
                    datatype,
                } = config;

                let hnsw_config = match index {
                    Indexes::Plain {} => fallback_hnsw,
                    Indexes::Hnsw(hnsw) => *hnsw,
                };

                (
                    name.clone(),
                    DenseVectorOptimizerInput {
                        size: *size,
                        distance: *distance,
                        on_disk: Some(storage_type.is_on_disk()),
                        hnsw_config,
                        quantization_config: quantization_config.clone(),
                        multivector_config: *multivector_config,
                        datatype: *datatype,
                    },
                )
            })
            .collect();

        let sparse_vectors = segment_config
            .sparse_vector_data
            .iter()
            .map(|(name, config)| {
                let SparseVectorDataConfig {
                    index,
                    storage_type,
                    modifier,
                } = config;

                (
                    name.clone(),
                    SparseVectorOptimizerInput {
                        on_disk: Some(index.index_type.is_on_disk()),
                        full_scan_threshold: index.full_scan_threshold,
                        index_datatype: index.datatype,
                        storage_type: *storage_type,
                        modifier: *modifier,
                    },
                )
            })
            .collect();

        Self {
            payload_storage_type: segment_config.payload_storage_type,
            dense_vectors,
            sparse_vectors,
        }
    }

    /// Build the unified [`SegmentOptimizerConfig`].
    pub fn build(self) -> SegmentOptimizerConfig {
        let appendable_quantization = common::flags::feature_flags().appendable_quantization;

        let base_vector_data = self
            .dense_vectors
            .iter()
            .map(|(name, input)| {
                (
                    name.clone(),
                    VectorDataConfig {
                        size: input.size,
                        distance: input.distance,
                        index: Indexes::Plain {},
                        storage_type: if input.on_disk.unwrap_or_default() {
                            VectorStorageType::ChunkedMmap
                        } else {
                            VectorStorageType::InRamChunkedMmap
                        },
                        quantization_config: input
                            .quantization_config
                            .as_ref()
                            .filter(|q| appendable_quantization && q.supports_appendable())
                            .cloned(),
                        multivector_config: input.multivector_config,
                        datatype: input.datatype,
                    },
                )
            })
            .collect();

        let base_sparse_vector_data = self
            .sparse_vectors
            .iter()
            .map(|(name, input)| {
                (
                    name.clone(),
                    SparseVectorDataConfig {
                        index: SparseIndexConfig {
                            full_scan_threshold: input.full_scan_threshold,
                            index_type: SparseIndexType::MutableRam,
                            datatype: input.index_datatype,
                        },
                        storage_type: input.storage_type,
                        modifier: input.modifier,
                    },
                )
            })
            .collect();

        let dense_vector = self
            .dense_vectors
            .into_iter()
            .map(|(name, input)| {
                (
                    name,
                    DenseVectorOptimizerConfig {
                        on_disk: input.on_disk,
                        hnsw_config: input.hnsw_config,
                        quantization_config: input.quantization_config,
                    },
                )
            })
            .collect();

        let sparse_vector = self
            .sparse_vectors
            .into_iter()
            .map(|(name, input)| {
                (
                    name,
                    SparseVectorOptimizerConfig {
                        on_disk: input.on_disk,
                    },
                )
            })
            .collect();

        SegmentOptimizerConfig {
            payload_storage_type: self.payload_storage_type,
            base_vector_data,
            base_sparse_vector_data,
            dense_vector,
            sparse_vector,
        }
    }
}

/// Target segment count for the merge optimizer.
pub fn default_segment_number() -> usize {
    // Configure 1 segment per 2 CPUs, as a middle ground between
    // latency and RPS.
    let expected_segments = common::cpu::get_num_cpus() / 2;
    // Do not configure less than 2 and more than 8 segments
    // until it is not explicitly requested
    expected_segments.clamp(2, 8)
}
