use std::collections::HashMap;

use segment::types::{
    HnswConfig, PayloadStorageType, QuantizationConfig, SegmentConfig, SparseVectorDataConfig,
    VectorDataConfig, VectorNameBuf,
};

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
