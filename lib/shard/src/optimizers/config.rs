use std::collections::HashMap;

use segment::types::{
    HnswConfig, PayloadStorageType, QuantizationConfig, SegmentConfig, SparseVectorDataConfig,
    VectorDataConfig, VectorNameBuf,
};

pub const TEMP_SEGMENTS_PATH: &str = "temp_segments";
pub const DEFAULT_MAX_SEGMENT_PER_CPU_KB: usize = 256_000;
pub const DEFAULT_INDEXING_THRESHOLD_KB: usize = 10_000;
pub const DEFAULT_DELETED_THRESHOLD: f64 = 0.2;
pub const DEFAULT_VACUUM_MIN_VECTOR_NUMBER: usize = 1000;

#[derive(Debug, Clone)]
pub struct DenseVectorOptimizerConfig {
    pub on_disk: Option<bool>,
    pub hnsw_config: HnswConfig,
    pub quantization_config: Option<QuantizationConfig>,
}

#[derive(Debug, Clone)]
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

/// Target segment count for the merge optimizer.
pub fn default_segment_number() -> usize {
    // Configure 1 segment per 2 CPUs, as a middle ground between
    // latency and RPS.
    let expected_segments = common::cpu::get_num_cpus() / 2;
    // Do not configure less than 2 and more than 8 segments
    // until it is not explicitly requested
    expected_segments.clamp(2, 8)
}
