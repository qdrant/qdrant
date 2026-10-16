//! User-facing vector and sparse-vector parameters for the edge shard.
//!
//! Uses `memory` (preferred) or the deprecated `on_disk` bool instead of internal
//! `storage_type`. Per-vector quantization is supported via
//! `EdgeVectorParams::quantization_config`; when set it overrides the global
//! `EdgeShardConfig::quantization_config` for that vector.

use segment::data_types::modifier::Modifier;
use segment::index::sparse_index::sparse_index_config::SparseIndexConfig;
use segment::types::{
    Distance, HnswConfig, Indexes, Memory, MultiVectorConfig, QuantizationConfig,
    SparseVectorDataConfig, SparseVectorStorageType, VectorDataConfig, VectorStorageDatatype,
};
use serde::{Deserialize, Serialize};
use shard::optimizers::config::{DenseVectorOptimizerConfig, SparseVectorOptimizerConfig};

/// User-facing dense vector parameters.
///
/// Prefer [`Self::memory`] over the deprecated [`Self::on_disk`] flag. Per-vector
/// quantization is supported via `quantization_config` and overrides the global
/// config when set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EdgeVectorParams {
    pub size: usize,
    pub distance: Distance,
    /// Deprecated: use `memory` instead.
    /// If true, vector storage is on disk (mmap); otherwise in RAM.
    /// Default is false (RAM) when neither `memory` nor `on_disk` is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deprecated(since = "1.19.0", note = "Use `memory` instead")]
    pub on_disk: Option<bool>,
    /// Memory placement of the original vector storage. Overrides the deprecated
    /// `on_disk` flag if both are set. `pinned` is not supported for dense vector
    /// storage (defensively mapped to `cached`). Default: `cached` (`cold` if
    /// `on_disk` is set to true).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<Memory>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multivector_config: Option<MultiVectorConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datatype: Option<VectorStorageDatatype>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantization_config: Option<QuantizationConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hnsw_config: Option<HnswConfig>,
}

impl EdgeVectorParams {
    /// Start building [`EdgeVectorParams`] with a fluent API. The two
    /// required fields (`size`, `distance`) are supplied here.
    pub fn builder(size: usize, distance: Distance) -> crate::builders::EdgeVectorParamsBuilder {
        crate::builders::EdgeVectorParamsBuilder::new(size, distance)
    }

    /// Requested memory placement of the original vector storage, resolving `memory` against
    /// the deprecated `on_disk` flag. `None` if neither is set.
    #[allow(deprecated)]
    pub fn memory_placement(&self) -> Option<Memory> {
        Memory::resolve(self.memory, self.on_disk.map(Memory::from_on_disk))
    }

    #[allow(deprecated)]
    pub fn to_dense_vector_optimizer_config(
        &self,
        global_hnsw_config: &HnswConfig,
        global_quantization_config: Option<&QuantizationConfig>,
    ) -> DenseVectorOptimizerConfig {
        let EdgeVectorParams {
            size,
            distance,
            on_disk,
            memory,
            multivector_config,
            datatype,
            quantization_config,
            hnsw_config,
        } = self;
        DenseVectorOptimizerConfig {
            size: *size,
            distance: *distance,
            on_disk: *on_disk,
            memory: *memory,
            hnsw_config: hnsw_config.unwrap_or(*global_hnsw_config),
            quantization_config: quantization_config
                .clone()
                .or_else(|| global_quantization_config.cloned()),
            multivector_config: *multivector_config,
            datatype: *datatype,
        }
    }

    #[allow(deprecated)]
    pub fn from_vector_data_config(v: &VectorDataConfig) -> Self {
        let VectorDataConfig {
            size,
            distance,
            storage_type: _,
            index,
            quantization_config, // edge uses global only
            multivector_config,
            datatype,
        } = v;
        Self {
            size: *size,
            distance: *distance,
            // Report both: `memory` is the preferred knob; `on_disk` keeps
            // legacy readers working when deriving from a segment.
            on_disk: Some(v.is_cold()),
            memory: v.storage_type.memory(),
            multivector_config: *multivector_config,
            datatype: *datatype,
            quantization_config: quantization_config.clone(),
            hnsw_config: match index {
                Indexes::Plain {} => None,
                Indexes::Hnsw(hnsw_config) => Some(*hnsw_config),
            },
        }
    }
}

/// User-facing sparse vector parameters.
///
/// Prefer [`Self::memory`] over the deprecated [`Self::on_disk`] flag.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EdgeSparseVectorParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_scan_threshold: Option<usize>,
    /// Deprecated: use `memory` instead.
    /// If true, sparse index is on disk (mmap); otherwise in RAM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deprecated(since = "1.19.0", note = "Use `memory` instead")]
    pub on_disk: Option<bool>,
    /// Memory placement of the sparse index. Overrides the deprecated `on_disk`
    /// flag if both are set. Default: `pinned` (`cold` if `on_disk` is set to
    /// true). Use `cached` for an mmap index that primes the page cache on open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<Memory>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modifier: Option<Modifier>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datatype: Option<VectorStorageDatatype>,
}

impl EdgeSparseVectorParams {
    /// Start building [`EdgeSparseVectorParams`] with a fluent API.
    pub fn builder() -> crate::builders::EdgeSparseVectorParamsBuilder {
        crate::builders::EdgeSparseVectorParamsBuilder::new()
    }

    /// Requested memory placement of the sparse index, resolving `memory` against the
    /// deprecated `on_disk` flag. `None` if neither is set.
    #[allow(deprecated)]
    pub fn memory_placement(&self) -> Option<Memory> {
        Memory::resolve(self.memory, self.on_disk.map(Memory::from_on_disk_heap))
    }

    #[allow(deprecated)]
    pub fn to_sparse_vector_optimizer_config(&self) -> SparseVectorOptimizerConfig {
        let EdgeSparseVectorParams {
            full_scan_threshold,
            on_disk,
            memory,
            modifier,
            datatype,
        } = self;
        SparseVectorOptimizerConfig {
            on_disk: *on_disk,
            // Persist only the explicitly requested `memory` parameter (same
            // rule as the server): structural placement stays on `index_type`,
            // so legacy-only configs keep byte-identical index files.
            memory: *memory,
            full_scan_threshold: *full_scan_threshold,
            index_datatype: *datatype,
            storage_type: SparseVectorStorageType::Mmap,
            modifier: *modifier,
        }
    }

    #[allow(deprecated)]
    pub fn from_sparse_vector_data_config(s: &SparseVectorDataConfig) -> Self {
        let SparseVectorDataConfig {
            index,
            storage_type: _, // edge uses on_disk from index_type
            modifier,
        } = s;
        let SparseIndexConfig {
            full_scan_threshold,
            index_type,
            datatype,
            memory,
        } = index;
        Self {
            full_scan_threshold: *full_scan_threshold,
            on_disk: Some(index_type.is_mmap()),
            // Only the explicitly stored `memory` field — not the resolved
            // placement — so re-persisting does not invent a `memory` key for
            // legacy on_disk-only configs.
            memory: *memory,
            modifier: *modifier,
            datatype: *datatype,
        }
    }
}
