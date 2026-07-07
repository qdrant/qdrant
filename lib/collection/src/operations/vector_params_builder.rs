// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::num::NonZeroU64;

use segment::types::{Distance, QuantizationConfig};

use crate::operations::config_diff::HnswConfigDiff;
use crate::operations::types::VectorParams;

pub struct VectorParamsBuilder {
    vector_params: VectorParams,
}

impl VectorParamsBuilder {
    pub fn new(size: u64, distance: Distance) -> Self {
        VectorParamsBuilder {
            vector_params: VectorParams {
                size: NonZeroU64::new(size).unwrap(),
                distance,
                hnsw_config: None,
                quantization_config: None,
                on_disk: None,
                memory: None,
                datatype: None,
                multivector_config: None,
            },
        }
    }

    pub fn with_on_disk(mut self, on_disk: bool) -> Self {
        self.vector_params.on_disk = Some(on_disk);
        self
    }

    pub fn with_memory(mut self, memory: segment::types::Memory) -> Self {
        self.vector_params.memory = Some(memory);
        self
    }

    pub fn with_hnsw_config(mut self, hnsw_config: HnswConfigDiff) -> Self {
        self.vector_params.hnsw_config = Some(hnsw_config);
        self
    }

    pub fn with_quantization_config(mut self, quantization_config: QuantizationConfig) -> Self {
        self.vector_params.quantization_config = Some(quantization_config);
        self
    }

    pub fn build(self) -> VectorParams {
        self.vector_params
    }
}
