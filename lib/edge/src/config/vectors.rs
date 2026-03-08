//! User-facing vector and sparse-vector parameters for the edge shard.
//!
//! Uses `on_disk` (bool) instead of internal `storage_type`. Per-vector quantization
//! is supported via `EdgeVectorParams::quantization_config`; when set it overrides the
//! global `EdgeShardConfig::quantization_config` for that vector.

use segment::data_types::modifier::Modifier;
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::types::{
    Distance, HnswConfig, Indexes, MultiVectorConfig, QuantizationConfig, SparseVectorDataConfig,
    SparseVectorStorageType, VectorDataConfig, VectorStorageDatatype, VectorStorageType,
};
use serde::{Deserialize, Serialize};
use shard::optimizers::config::DenseVectorOptimizerConfig;

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
    /// Build `VectorDataConfig` for segment/optimizer use, using global quantization.
    pub fn to_plain_vector_data_config(
        &self,
        global_quantization: Option<&QuantizationConfig>,
    ) -> VectorDataConfig {
        let EdgeVectorParams {
            size,
            distance,
            on_disk,
            multivector_config,
            datatype,
            quantization_config,
            hnsw_config: _, // edge does not use per-vector HNSW config
        } = self;

        let resolved_quantization_config = quantization_config.as_ref().or(global_quantization);
        let quantization_config =
            QuantizationConfig::for_appendable_segment(resolved_quantization_config);
        VectorDataConfig {
            size: *size,
            distance: *distance,
            storage_type: VectorStorageType::from_on_disk(on_disk.unwrap_or_default()),
            index: Indexes::Plain {},
            quantization_config,
            multivector_config: *multivector_config,
            datatype: *datatype,
        }
    }

    pub fn to_dense_vector_optimizer_config(
        &self,
        global_hnsw_config: &HnswConfig,
        global_quantization_config: Option<&QuantizationConfig>,
    ) -> DenseVectorOptimizerConfig {
        let EdgeVectorParams {
            size: _,
            distance: _,
            on_disk,
            multivector_config: _,
            datatype: _,
            quantization_config,
            hnsw_config,
        } = self;
        DenseVectorOptimizerConfig {
            on_disk: *on_disk,
            hnsw_config: hnsw_config.unwrap_or(*global_hnsw_config),
            quantization_config: quantization_config
                .clone()
                .or_else(|| global_quantization_config.cloned()),
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    pub fn to_plain_sparse_vector_data_config(&self) -> SparseVectorDataConfig {
        let EdgeSparseVectorParams {
            full_scan_threshold,
            on_disk: _,
            modifier,
            datatype,
        } = self;
        SparseVectorDataConfig {
            index: SparseIndexConfig {
                full_scan_threshold: *full_scan_threshold,
                index_type: SparseIndexType::default(),
                datatype: *datatype,
            },
            storage_type: SparseVectorStorageType::Mmap,
            modifier: *modifier,
        }
    }

    pub fn to_sparse_vector_optimizer_config(
        &self,
    ) -> shard::optimizers::config::SparseVectorOptimizerConfig {
        let EdgeSparseVectorParams {
            full_scan_threshold: _,
            on_disk,
            modifier: _,
            datatype: _,
        } = self;
        shard::optimizers::config::SparseVectorOptimizerConfig { on_disk: *on_disk }
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
        } = index;
        Self {
            full_scan_threshold: *full_scan_threshold,
            on_disk: Some(index_type.is_on_disk()),
            modifier: *modifier,
            datatype: *datatype,
        }
    }
}
