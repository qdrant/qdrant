//! Edge shard configuration: user-facing params and conversion to/from SegmentConfig.

use std::collections::HashMap;
use std::path::Path;

use common::fs::{atomic_save_json, read_json};
use segment::common::operation_error::{OperationError, OperationResult};
use segment::types::{
    HnswConfig, PayloadStorageType, QuantizationConfig, SegmentConfig, VectorNameBuf,
};
use serde::{Deserialize, Serialize};
use shard::operations::optimization::OptimizerThresholds;

use super::optimizers::EdgeOptimizersConfig;
use super::vectors::{EdgeSparseVectorParams, EdgeVectorParams};

/// File name for the persisted edge shard config.
pub(crate) const EDGE_CONFIG_FILE: &str = "edge_config.json";

/// Full configuration for an edge shard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EdgeConfig {
    /// If true, payload is stored on disk (mmap); otherwise in RAM. Same as `CollectionParams::on_disk_payload`.
    #[serde(default = "default_on_disk_payload")]
    pub on_disk_payload: bool,
    /// Dense vector params per vector name.
    #[serde(default)]
    pub vectors: HashMap<VectorNameBuf, EdgeVectorParams>,
    /// Sparse vector params per vector name.
    #[serde(default)]
    pub sparse_vectors: HashMap<VectorNameBuf, EdgeSparseVectorParams>,
    /// Global HNSW config; per-vector override is in `vectors[].hnsw_config`
    #[serde(default)]
    pub hnsw_config: HnswConfig,
    /// Global quantization config for all vectors
    /// Per-vector override in in `vectors[].quantization_config`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantization_config: Option<QuantizationConfig>,
    #[serde(default)]
    pub optimizers: EdgeOptimizersConfig,
}

fn default_on_disk_payload() -> bool {
    true
}

impl Default for EdgeConfig {
    fn default() -> Self {
        Self {
            on_disk_payload: default_on_disk_payload(),
            vectors: HashMap::new(),
            sparse_vectors: HashMap::new(),
            hnsw_config: HnswConfig::default(),
            quantization_config: None,
            optimizers: EdgeOptimizersConfig::default(),
        }
    }
}

impl EdgeConfig {
    /// Build from existing segment config. Fills all parameters that can be inferred.
    pub fn from_segment_config(segment: &SegmentConfig) -> Self {
        let SegmentConfig {
            vector_data,
            sparse_vector_data,
            payload_storage_type,
        } = segment;

        let vectors = vector_data
            .iter()
            .map(|(name, v)| (name.clone(), EdgeVectorParams::from_vector_data_config(v)))
            .collect();

        let sparse_vectors = sparse_vector_data
            .iter()
            .map(|(name, s)| {
                (
                    name.clone(),
                    EdgeSparseVectorParams::from_sparse_vector_data_config(s),
                )
            })
            .collect();

        let on_disk_payload = payload_storage_type.is_on_disk();

        // Infer global hnsw_config from per-vector HNSW configs when all agree
        let hnsw_configs: Vec<HnswConfig> = vector_data
            .values()
            .filter_map(|v| match &v.index {
                segment::types::Indexes::Plain {} => None,
                segment::types::Indexes::Hnsw(h) => Some(*h),
            })
            .collect();
        let hnsw_config = hnsw_configs
            .first()
            .and_then(|first| {
                if hnsw_configs.iter().all(|h| h == first) {
                    Some(*first)
                } else {
                    None
                }
            })
            .unwrap_or_default();

        Self {
            on_disk_payload,
            vectors,
            sparse_vectors,
            hnsw_config,
            quantization_config: None,
            optimizers: EdgeOptimizersConfig::default(),
        }
    }

    /// Check compatibility with a segment config (e.g. loaded segment).
    pub fn check_compatible_with_segment_config(
        &self,
        other: &SegmentConfig,
    ) -> Result<(), String> {
        self.plain_segment_config().check_compatible(other)
    }

    /// Segment config for creating appendable segments only.
    /// Does not contain any HNSW configuration (plain index only).
    pub fn plain_segment_config(&self) -> SegmentConfig {
        let payload_storage_type = PayloadStorageType::from_on_disk_payload(self.on_disk_payload);
        let vector_data = self
            .vectors
            .iter()
            .map(|(name, p)| {
                (
                    name.clone(),
                    p.to_plain_vector_data_config(self.quantization_config.as_ref()),
                )
            })
            .collect();

        let sparse_vector_data = self
            .sparse_vectors
            .iter()
            .map(|(name, p)| (name.clone(), p.to_plain_sparse_vector_data_config()))
            .collect();

        SegmentConfig {
            vector_data,
            sparse_vector_data,
            payload_storage_type,
        }
    }

    /// Build segment optimizer config from this config (for blocking optimizers).
    /// Use this instead of converting to SegmentConfig first.
    pub fn segment_optimizer_config(&self) -> shard::optimizers::config::SegmentOptimizerConfig {
        use shard::optimizers::config::SegmentOptimizerConfig;

        let SegmentConfig {
            vector_data: plain_dense_vector_config,
            sparse_vector_data: plain_sparse_vector_config,
            payload_storage_type,
        } = self.plain_segment_config();

        let dense_vector = self
            .vectors
            .iter()
            .map(|(name, p)| {
                (
                    name.clone(),
                    p.to_dense_vector_optimizer_config(
                        &self.hnsw_config,
                        self.quantization_config.as_ref(),
                    ),
                )
            })
            .collect();

        let sparse_vector = self
            .sparse_vectors
            .iter()
            .map(|(name, p)| (name.clone(), p.to_sparse_vector_optimizer_config()))
            .collect();

        SegmentOptimizerConfig {
            payload_storage_type,
            plain_dense_vector_config,
            plain_sparse_vector_config,
            dense_vector,
            sparse_vector,
        }
    }

    /// Return vector data config for a named vector (for read-only use, e.g. query).
    /// Uses plain index; for optimizer/segment creation use segment_optimizer_config or plain_segment_config.
    pub fn vector_data_config(
        &self,
        name: &VectorNameBuf,
    ) -> Option<segment::types::VectorDataConfig> {
        self.vectors
            .get(name)
            .map(|p| p.to_plain_vector_data_config(self.quantization_config.as_ref()))
    }

    pub fn optimizer_thresholds(&self, num_indexing_threads: usize) -> OptimizerThresholds {
        let indexing_threshold_kb = self.optimizers.get_indexing_threshold_kb();
        OptimizerThresholds {
            memmap_threshold_kb: usize::MAX,
            indexing_threshold_kb,
            max_segment_size_kb: self
                .optimizers
                .get_max_segment_size_kb(num_indexing_threads),
            deferred_internal_id: None,
        }
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        let config_path = path.join(EDGE_CONFIG_FILE);
        atomic_save_json(&config_path, self).map_err(|e| {
            OperationError::service_error(format!(
                "failed to write {}: {}",
                config_path.display(),
                e
            ))
        })
    }

    pub fn load(path: &Path) -> Option<OperationResult<Self>> {
        let config_path = path.join(EDGE_CONFIG_FILE);
        match fs_err::exists(&config_path) {
            Ok(false) => return None,
            Err(e) => return Some(Err(OperationError::from(e))),
            Ok(true) => {}
        }
        Some(read_json(&config_path).map_err(OperationError::from))
    }

    pub fn set_hnsw_config(&mut self, hnsw_config: HnswConfig) {
        self.hnsw_config = hnsw_config;
    }

    pub fn set_vector_hnsw_config(
        &mut self,
        vector_name: &str,
        hnsw_config: HnswConfig,
    ) -> OperationResult<()> {
        let name = VectorNameBuf::from(vector_name);
        let params = self
            .vectors
            .get_mut(&name)
            .ok_or_else(|| OperationError::vector_name_not_exists(vector_name))?;
        params.hnsw_config = Some(hnsw_config);
        Ok(())
    }

    pub fn set_optimizers_config(&mut self, optimizers: EdgeOptimizersConfig) {
        self.optimizers = optimizers;
    }

    pub fn optimizers_mut(&mut self) -> &mut EdgeOptimizersConfig {
        &mut self.optimizers
    }
}
