//! Fluent builder for [`EdgeVectorParams`].
//!
//! Builder fields mirror [`EdgeVectorParams`] explicitly so adding a field
//! to the target struct forces a compile error here.

use segment::types::{
    Distance, HnswConfig, MultiVectorConfig, QuantizationConfig, VectorStorageDatatype,
};

use crate::config::vectors::EdgeVectorParams;

/// Fluent builder for [`EdgeVectorParams`].
///
/// `size` and `distance` are required and passed through [`Self::new`]; every
/// other field is optional and falls back to `None`.
#[derive(Debug, Clone)]
pub struct EdgeVectorParamsBuilder {
    size: usize,
    distance: Distance,
    on_disk: Option<bool>,
    multivector_config: Option<MultiVectorConfig>,
    datatype: Option<VectorStorageDatatype>,
    quantization_config: Option<QuantizationConfig>,
    hnsw_config: Option<HnswConfig>,
    data_integrity_check: Option<bool>,
    magnitude_bound: Option<f32>,
}

impl EdgeVectorParamsBuilder {
    pub fn new(size: usize, distance: Distance) -> Self {
        Self {
            size,
            distance,
            on_disk: None,
            multivector_config: None,
            datatype: None,
            quantization_config: None,
            hnsw_config: None,
            data_integrity_check: None,
            magnitude_bound: None,
        }
    }

    /// If `true`, vector storage is on disk (mmap); otherwise in RAM.
    pub fn on_disk(mut self, on_disk: bool) -> Self {
        self.on_disk = Some(on_disk);
        self
    }

    pub fn multivector_config(mut self, multivector_config: MultiVectorConfig) -> Self {
        self.multivector_config = Some(multivector_config);
        self
    }

    pub fn datatype(mut self, datatype: VectorStorageDatatype) -> Self {
        self.datatype = Some(datatype);
        self
    }

    /// Per-vector quantization. Overrides the global
    /// [`EdgeConfig::quantization_config`](crate::EdgeConfig::quantization_config)
    /// when set.
    pub fn quantization_config(mut self, quantization_config: QuantizationConfig) -> Self {
        self.quantization_config = Some(quantization_config);
        self
    }

    /// Per-vector HNSW config. Overrides the global
    /// [`EdgeConfig::hnsw_config`](crate::EdgeConfig::hnsw_config) when set.
    pub fn hnsw_config(mut self, hnsw_config: HnswConfig) -> Self {
        self.hnsw_config = Some(hnsw_config);
        self
    }

    pub fn data_integrity_check(mut self, data_integrity_check: bool) -> Self {
        self.data_integrity_check = Some(data_integrity_check);
        self
    }

    pub fn magnitude_bound(mut self, magnitude_bound: f32) -> Self {
        self.magnitude_bound = Some(magnitude_bound);
        self
    }

    pub fn build(self) -> EdgeVectorParams {
        // Exhaustively destructure Self and construct EdgeVectorParams:
        // adding a field to either type forces a compile error here.
        let Self {
            size,
            distance,
            on_disk,
            multivector_config,
            datatype,
            quantization_config,
            hnsw_config,
            data_integrity_check,
            magnitude_bound,
        } = self;
        EdgeVectorParams {
            size,
            distance,
            on_disk,
            multivector_config,
            datatype,
            quantization_config,
            hnsw_config,
            data_integrity_check,
            magnitude_bound,
        }
    }
}
