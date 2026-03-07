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
        let EdgeVectorParams {
            size,
            distance,
            on_disk,
            index,
            multivector_config,
            datatype,
        } = self;
        let appendable_quantization = common::flags::feature_flags().appendable_quantization;
        let quantization_config = global_quantization
            .filter(|q| appendable_quantization && q.supports_appendable())
            .cloned();
        VectorDataConfig {
            size: *size,
            distance: *distance,
            storage_type: if *on_disk {
                VectorStorageType::ChunkedMmap
            } else {
                VectorStorageType::InRamChunkedMmap
            },
            index: index.clone(),
            quantization_config,
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
            quantization_config: _, // edge uses global only
            multivector_config,
            datatype,
        } = v;
        Self {
            size: *size,
            distance: *distance,
            on_disk: storage_type.is_on_disk(),
            index: index.clone(),
            multivector_config: *multivector_config,
            datatype: *datatype,
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
        let EdgeSparseVectorParams {
            full_scan_threshold,
            on_disk,
            modifier,
            datatype,
        } = self;
        SparseVectorDataConfig {
            index: SparseIndexConfig {
                full_scan_threshold: *full_scan_threshold,
                index_type: if *on_disk {
                    SparseIndexType::Mmap
                } else {
                    SparseIndexType::MutableRam
                },
                datatype: *datatype,
            },
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
        } = index;
        Self {
            full_scan_threshold: *full_scan_threshold,
            on_disk: index_type.is_on_disk(),
            modifier: *modifier,
            datatype: *datatype,
        }
    }
}
