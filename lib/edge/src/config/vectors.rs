//! User-facing vector and sparse-vector parameters for the edge shard.
//!
//! Uses `on_disk` (bool) instead of internal `storage_type`; no per-vector
//! quantization (global only in `EdgeShardConfig`).

use segment::data_types::modifier::Modifier;
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::types::{
    Distance, Indexes, MultiVectorConfig, SparseVectorDataConfig, SparseVectorStorageType,
    VectorDataConfig, VectorStorageDatatype, VectorStorageType,
};
use serde::{Deserialize, Serialize};

/// User-facing dense vector parameters.
///
/// Uses `on_disk: bool` instead of `storage_type`; quantization is global in
/// `EdgeShardConfig`, not per-vector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EdgeVectorParams {
    pub size: usize,
    pub distance: Distance,
    /// If true, vector storage is on disk (mmap); otherwise in RAM.
    #[serde(default)]
    pub on_disk: bool,
    /// Index type: plain (no HNSW) or HNSW with config. Per-vector override of global `hnsw_config`.
    #[serde(default)]
    pub index: Indexes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multivector_config: Option<MultiVectorConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datatype: Option<VectorStorageDatatype>,
}

impl EdgeVectorParams {
    /// Build `VectorDataConfig` for segment/optimizer use, using global quantization.
    pub fn to_vector_data_config(
        &self,
        global_quantization: Option<&segment::types::QuantizationConfig>,
    ) -> VectorDataConfig {
        let appendable_quantization = common::flags::feature_flags().appendable_quantization;
        let quantization_config = global_quantization
            .filter(|q| appendable_quantization && q.supports_appendable())
            .cloned();
        VectorDataConfig {
            size: self.size,
            distance: self.distance,
            storage_type: if self.on_disk {
                VectorStorageType::ChunkedMmap
            } else {
                VectorStorageType::InRamChunkedMmap
            },
            index: self.index.clone(),
            quantization_config,
            multivector_config: self.multivector_config,
            datatype: self.datatype,
        }
    }

    pub fn from_vector_data_config(v: &VectorDataConfig) -> Self {
        Self {
            size: v.size,
            distance: v.distance,
            on_disk: v.storage_type.is_on_disk(),
            index: v.index.clone(),
            multivector_config: v.multivector_config,
            datatype: v.datatype,
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
    #[serde(default)]
    pub on_disk: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modifier: Option<Modifier>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datatype: Option<VectorStorageDatatype>,
}

impl EdgeSparseVectorParams {
    pub fn to_sparse_vector_data_config(&self) -> SparseVectorDataConfig {
        SparseVectorDataConfig {
            index: SparseIndexConfig {
                full_scan_threshold: self.full_scan_threshold,
                index_type: if self.on_disk {
                    SparseIndexType::Mmap
                } else {
                    SparseIndexType::MutableRam
                },
                datatype: self.datatype,
            },
            storage_type: SparseVectorStorageType::Mmap,
            modifier: self.modifier,
        }
    }

    pub fn from_sparse_vector_data_config(s: &SparseVectorDataConfig) -> Self {
        Self {
            full_scan_threshold: s.index.full_scan_threshold,
            on_disk: s.index.index_type.is_on_disk(),
            modifier: s.modifier,
            datatype: s.index.datatype,
        }
    }
}
