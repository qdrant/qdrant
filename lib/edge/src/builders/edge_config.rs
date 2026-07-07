//! Fluent builder for [`EdgeConfig`].
//!
//! Builder fields mirror [`EdgeConfig`] explicitly (no `inner: EdgeConfig`)
//! so adding a field to [`EdgeConfig`] forces a compile error here — both
//! in the struct definition and in the exhaustive `build` step.

use std::collections::HashMap;

use segment::types::{HnswConfig, QuantizationConfig, VectorNameBuf};
use wal::WalOptions;

use crate::config::optimizers::EdgeOptimizersConfig;
use crate::config::shard::EdgeConfig;
use crate::config::vectors::{EdgeSparseVectorParams, EdgeVectorParams};

/// Fluent builder for [`EdgeConfig`].
///
/// All fields are optional; at minimum supply at least one dense or sparse
/// vector via [`Self::vector`] / [`Self::sparse_vector`]. Fields left unset
/// stay unspecified (`None`) in the built config: loading an existing shard
/// keeps their persisted values, otherwise defaults apply.
#[derive(Debug, Default)]
pub struct EdgeConfigBuilder {
    on_disk_payload: Option<bool>,
    vectors: HashMap<VectorNameBuf, EdgeVectorParams>,
    sparse_vectors: HashMap<VectorNameBuf, EdgeSparseVectorParams>,
    hnsw_config: Option<HnswConfig>,
    quantization_config: Option<QuantizationConfig>,
    optimizers: Option<EdgeOptimizersConfig>,
    wal_options: Option<WalOptions>,
    max_search_threads: Option<usize>,
}

impl EdgeConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a single dense vector by name. Overwrites any prior entry with
    /// the same name.
    pub fn vector(mut self, name: impl Into<VectorNameBuf>, params: EdgeVectorParams) -> Self {
        self.vectors.insert(name.into(), params);
        self
    }

    /// Replace the full dense vectors map.
    pub fn vectors(mut self, vectors: HashMap<VectorNameBuf, EdgeVectorParams>) -> Self {
        self.vectors = vectors;
        self
    }

    /// Insert a single sparse vector by name. Overwrites any prior entry with
    /// the same name.
    pub fn sparse_vector(
        mut self,
        name: impl Into<VectorNameBuf>,
        params: EdgeSparseVectorParams,
    ) -> Self {
        self.sparse_vectors.insert(name.into(), params);
        self
    }

    /// Replace the full sparse vectors map.
    pub fn sparse_vectors(
        mut self,
        sparse_vectors: HashMap<VectorNameBuf, EdgeSparseVectorParams>,
    ) -> Self {
        self.sparse_vectors = sparse_vectors;
        self
    }

    pub fn on_disk_payload(mut self, on_disk_payload: bool) -> Self {
        self.on_disk_payload = Some(on_disk_payload);
        self
    }

    pub fn hnsw_config(mut self, hnsw_config: HnswConfig) -> Self {
        self.hnsw_config = Some(hnsw_config);
        self
    }

    pub fn quantization_config(mut self, quantization_config: QuantizationConfig) -> Self {
        self.quantization_config = Some(quantization_config);
        self
    }

    pub fn optimizers(mut self, optimizers: EdgeOptimizersConfig) -> Self {
        self.optimizers = Some(optimizers);
        self
    }

    pub fn wal_options(mut self, wal_options: WalOptions) -> Self {
        self.wal_options = Some(wal_options);
        self
    }

    /// Number of threads in the shard's search thread pool. `0` derives the count from the number
    /// of CPUs (matching the core search runtime). See [`EdgeConfig::max_search_threads`].
    pub fn max_search_threads(mut self, max_search_threads: usize) -> Self {
        self.max_search_threads = Some(max_search_threads);
        self
    }

    pub fn build(self) -> EdgeConfig {
        // Exhaustively destructure Self and construct EdgeConfig: adding a
        // field to either type forces a compile error here.
        let Self {
            on_disk_payload,
            vectors,
            sparse_vectors,
            hnsw_config,
            quantization_config,
            optimizers,
            wal_options,
            max_search_threads,
        } = self;
        EdgeConfig {
            on_disk_payload,
            vectors,
            sparse_vectors,
            hnsw_config,
            quantization_config,
            optimizers,
            wal_options,
            max_search_threads,
        }
    }
}
