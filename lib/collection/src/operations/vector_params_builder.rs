use std::num::NonZeroU64;

use segment::types::{Distance, MultiVectorConfig, QuantizationConfig};

use crate::operations::config_diff::HnswConfigDiff;
use crate::operations::types::{Datatype, VectorParams};

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
                datatype: None,
                multivector_config: None,
            },
        }
    }

    pub fn with_hnsw_config(mut self, hnsw_config: HnswConfigDiff) -> Self {
        self.vector_params.hnsw_config = Some(hnsw_config);
        self
    }

    pub fn with_quantization_config(mut self, quantization_config: QuantizationConfig) -> Self {
        self.vector_params.quantization_config = Some(quantization_config);
        self
    }

    pub fn with_on_disk(mut self, on_disk: bool) -> Self {
        self.vector_params.on_disk = Some(on_disk);
        self
    }

    pub fn with_datatype(mut self, datatype: Datatype) -> Self {
        self.vector_params.datatype = Some(datatype);
        self
    }

    pub fn with_multivector_config(mut self, multivector_config: MultiVectorConfig) -> Self {
        self.vector_params.multivector_config = Some(multivector_config);
        self
    }

    pub fn build(self) -> VectorParams {
        self.vector_params
    }
}
