//! User-facing vector and sparse-vector parameters for the edge shard.
//!
//! Uses `on_disk` (bool) instead of internal `storage_type`. Per-vector quantization
//! is supported via `EdgeVectorParams::quantization_config`; when set it overrides the
//! global `EdgeShardConfig::quantization_config` for that vector.

use segment::data_types::modifier::Modifier;
use segment::index::sparse_index::sparse_index_config::SparseIndexConfig;
use segment::types::{
    Distance, HnswConfig, Indexes, MultiVectorConfig, QuantizationConfig, SparseVectorDataConfig,
    SparseVectorStorageType, VectorDataConfig, VectorStorageDatatype,
};
use serde::{Deserialize, Serialize};
use shard::optimizers::config::{DenseVectorOptimizerConfig, SparseVectorOptimizerConfig};

/// User-facing dense vector parameters.
///
/// Uses `on_disk: bool` instead of `storage_type`. Per-vector quantization is
/// supported via `quantization_config` and overrides the global config when set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EdgeVectorParams {
    pub size: usize,
    pub distance: Distance,
    /// If true, vector storage is on disk (mmap); otherwise in RAM.
    /// Default is false (RAM).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
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

    pub fn to_dense_vector_optimizer_config(
        &self,
        global_hnsw_config: &HnswConfig,
        global_quantization_config: Option<&QuantizationConfig>,
    ) -> DenseVectorOptimizerConfig {
        let EdgeVectorParams {
            size,
            distance,
            on_disk,
            multivector_config,
            datatype,
            quantization_config,
            hnsw_config,
        } = self;
        DenseVectorOptimizerConfig {
            size: *size,
            distance: *distance,
            on_disk: *on_disk,
            memory: None,
            hnsw_config: hnsw_config.unwrap_or(*global_hnsw_config),
            quantization_config: quantization_config
                .clone()
                .or_else(|| global_quantization_config.cloned()),
            multivector_config: *multivector_config,
            datatype: *datatype,
        }
    }

    pub fn from_vector_data_config(v: &VectorDataConfig) -> Self {
        let VectorDataConfig {
            size,
            distance,
            storage_type,
            index,
            quantization_config, // edge uses global only
            multivector_config,
            datatype,
        } = v;
        Self {
            size: *size,
            distance: *distance,
            on_disk: Some(storage_type.is_on_disk()),
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
/// Uses `on_disk: bool` instead of internal storage/index type enums.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EdgeSparseVectorParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_scan_threshold: Option<usize>,
    /// If true, sparse index is on disk (mmap); otherwise in RAM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
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

    pub fn to_sparse_vector_optimizer_config(&self) -> SparseVectorOptimizerConfig {
        let EdgeSparseVectorParams {
            full_scan_threshold,
            on_disk,
            modifier,
            datatype,
        } = self;
        SparseVectorOptimizerConfig {
            on_disk: *on_disk,
            memory: None,
            full_scan_threshold: *full_scan_threshold,
            index_datatype: *datatype,
            storage_type: SparseVectorStorageType::Mmap,
            modifier: *modifier,
        }
    }

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
            memory: _,
        } = index;
        Self {
            full_scan_threshold: *full_scan_threshold,
            on_disk: Some(index_type.is_on_disk()),
            modifier: *modifier,
            datatype: *datatype,
        }
    }
}
