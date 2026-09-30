//! Fluent builder for [`EdgeVectorParams`].
//!
//! Builder fields mirror [`EdgeVectorParams`] explicitly so adding a field
//! to the target struct forces a compile error here.

use segment::types::{
    Distance, HnswConfig, Memory, MultiVectorConfig, QuantizationConfig, VectorStorageDatatype,
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
    memory: Option<Memory>,
    multivector_config: Option<MultiVectorConfig>,
    datatype: Option<VectorStorageDatatype>,
    quantization_config: Option<QuantizationConfig>,
    hnsw_config: Option<HnswConfig>,
}

impl EdgeVectorParamsBuilder {
    pub fn new(size: usize, distance: Distance) -> Self {
        Self {
            size,
            distance,
            on_disk: None,
            memory: None,
            multivector_config: None,
            datatype: None,
            quantization_config: None,
            hnsw_config: None,
        }
    }

    /// Deprecated: use [`Self::memory`] instead.
    /// If `true`, vector storage is on disk (mmap); otherwise in RAM.
    #[deprecated(since = "1.19.0", note = "Use `memory` instead")]
    pub fn on_disk(mut self, on_disk: bool) -> Self {
        self.on_disk = Some(on_disk);
        self
    }

    /// Memory placement of the original vector storage. Overrides the deprecated
    /// `on_disk` flag if both are set.
    pub fn memory(mut self, memory: Memory) -> Self {
        self.memory = Some(memory);
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

    #[allow(deprecated)]
    pub fn build(self) -> EdgeVectorParams {
        // Exhaustively destructure Self and construct EdgeVectorParams:
        // adding a field to either type forces a compile error here.
        let Self {
            size,
            distance,
            on_disk,
            memory,
            multivector_config,
            datatype,
            quantization_config,
            hnsw_config,
        } = self;
        EdgeVectorParams {
            size,
            distance,
            on_disk,
            memory,
            multivector_config,
            datatype,
            quantization_config,
            hnsw_config,
        }
    }
}
