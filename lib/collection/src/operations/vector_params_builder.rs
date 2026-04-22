use std::num::NonZeroU64;

use segment::types::Distance;

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
                datatype: None,
                multivector_config: None,
            },
        }
    }

    pub fn with_on_disk(mut self, on_disk: bool) -> Self {
        self.vector_params.on_disk = Some(on_disk);
        self
    }

    pub fn build(self) -> VectorParams {
        self.vector_params
    }
}
