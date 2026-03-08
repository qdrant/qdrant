use std::collections::HashMap;
use std::num::NonZeroUsize;

use segment::common::BYTES_IN_KB;
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

/// Extra configuration for dense vectors, applied on top of the plain config during optimization.
#[derive(Debug, Clone, PartialEq)]
pub struct DenseVectorOptimizerConfig {
    pub on_disk: Option<bool>,
    pub hnsw_config: HnswConfig,
    pub quantization_config: Option<QuantizationConfig>,
}

/// Extra configuration for sparse vectors, applied on top of the plain config during optimization.
#[derive(Debug, Clone, PartialEq)]
pub struct SparseVectorOptimizerConfig {
    pub on_disk: Option<bool>,
}

/// This configuration contains all necessary information to build an optimized segment.
#[derive(Debug, Clone)]
pub struct SegmentOptimizerConfig {
    pub payload_storage_type: PayloadStorageType,
    /// Configuration of dense vectors, as it should be for a plain segment (without any optimization).
    pub plain_dense_vector_config: HashMap<VectorNameBuf, VectorDataConfig>,
    /// Configuration of sparse vectors, as it should be for a plain segment (without any optimization).
    pub plain_sparse_vector_config: HashMap<VectorNameBuf, SparseVectorDataConfig>,
    /// Extra configuration for dense vectors, which _might_ be applied during optimization,
    /// depending on the segment state.
    pub dense_vector: HashMap<VectorNameBuf, DenseVectorOptimizerConfig>,
    /// Extra configuration for sparse vectors, which _might_ be applied during optimization,
    /// depending on the segment state.
    pub sparse_vector: HashMap<VectorNameBuf, SparseVectorOptimizerConfig>,
}

impl SegmentOptimizerConfig {
    pub fn plain_segment_config(&self) -> SegmentConfig {
        SegmentConfig {
            vector_data: self.plain_dense_vector_config.clone(),
            sparse_vector_data: self.plain_sparse_vector_config.clone(),
            payload_storage_type: self.payload_storage_type,
        }
    }

    pub fn new(
        payload_storage_type: PayloadStorageType,
        dense_vectors: HashMap<VectorNameBuf, DenseVectorOptimizerInput>,
        sparse_vectors: HashMap<VectorNameBuf, SparseVectorOptimizerInput>,
    ) -> SegmentOptimizerConfig {
        let (mut plain_dense_vector_config, mut dense_vector) = (HashMap::new(), HashMap::new());
        for (name, input) in dense_vectors {
            let DenseVectorOptimizerInput {
                size,
                distance,
                on_disk,
                hnsw_config,
                quantization_config,
                multivector_config,
                datatype,
            } = input;
            plain_dense_vector_config.insert(
                name.clone(),
                VectorDataConfig {
                    size,
                    distance,
                    index: Indexes::Plain {},
                    storage_type: VectorStorageType::from_on_disk(on_disk.unwrap_or_default()),
                    quantization_config: QuantizationConfig::for_appendable_segment(
                        quantization_config.as_ref(),
                    ),
                    multivector_config,
                    datatype,
                },
            );
            dense_vector.insert(
                name,
                DenseVectorOptimizerConfig {
                    on_disk,
                    hnsw_config,
                    quantization_config,
                },
            );
        }

        let (mut plain_sparse_vector_config, mut sparse_vector) = (HashMap::new(), HashMap::new());
        for (name, input) in sparse_vectors {
            let SparseVectorOptimizerInput {
                on_disk,
                full_scan_threshold,
                index_datatype,
                storage_type,
                modifier,
            } = input;
            plain_sparse_vector_config.insert(
                name.clone(),
                SparseVectorDataConfig {
                    index: SparseIndexConfig {
                        full_scan_threshold,
                        index_type: SparseIndexType::MutableRam,
                        datatype: index_datatype,
                    },
                    storage_type,
                    modifier,
                },
            );
            sparse_vector.insert(name, SparseVectorOptimizerConfig { on_disk });
        }

        SegmentOptimizerConfig {
            payload_storage_type,
            plain_dense_vector_config,
            plain_sparse_vector_config,
            dense_vector,
            sparse_vector,
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

/// Target segment count for the merge optimizer.
pub fn default_segment_number() -> usize {
    // Configure 1 segment per 2 CPUs, as a middle ground between
    // latency and RPS.
    let expected_segments = common::cpu::get_num_cpus() / 2;
    // Do not configure less than 2 and more than 8 segments
    // until it is not explicitly requested
    expected_segments.clamp(2, 8)
}

// --- Shared optimizer threshold helpers (used by collection and edge) ---

/// Resolve number of segments: if `default_segment_number` is 0, use CPU-based default.
pub fn get_number_segments(requested_segment_number: usize) -> usize {
    if requested_segment_number == 0 {
        default_segment_number()
    } else {
        requested_segment_number
    }
}

/// Resolve indexing threshold in KB: `None` => default, `Some(0)` => disable (usize::MAX).
pub fn get_indexing_threshold_kb(indexing_threshold: Option<usize>) -> usize {
    match indexing_threshold {
        None => DEFAULT_INDEXING_THRESHOLD_KB,
        Some(0) => usize::MAX,
        Some(custom) => custom,
    }
}

/// Resolve max segment size in KB: custom value or per-thread default.
pub fn get_max_segment_size_kb(
    max_segment_size: Option<usize>,
    num_indexing_threads: usize,
) -> usize {
    if let Some(max) = max_segment_size {
        max
    } else {
        num_indexing_threads.saturating_mul(DEFAULT_MAX_SEGMENT_PER_CPU_KB)
    }
}

/// Build deferred points threshold in bytes when `prevent_unoptimized` is true.
pub fn get_deferred_points_threshold_bytes(
    prevent_unoptimized: Option<bool>,
    indexing_threshold_kb: usize,
) -> Option<NonZeroUsize> {
    (prevent_unoptimized == Some(true))
        .then(|| indexing_threshold_kb.saturating_mul(BYTES_IN_KB))
        .and_then(NonZeroUsize::new)
}
