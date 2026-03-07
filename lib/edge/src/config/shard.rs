//! Edge shard configuration: user-facing params and conversion to/from SegmentConfig.

use std::collections::HashMap;
use std::path::Path;

use fs_err as fs;
use segment::types::{
    HnswConfig, Indexes, PayloadStorageType, QuantizationConfig, SegmentConfig, VectorNameBuf,
};
use serde::{Deserialize, Serialize};

use super::optimizers::EdgeOptimizersConfig;
use super::vectors::{EdgeSparseVectorParams, EdgeVectorParams};

/// File name for the persisted edge shard config.
pub const EDGE_CONFIG_FILE: &str = "edge_config.json";

/// Full configuration for an edge shard.
///
/// User-facing only: `on_disk_payload`, `on_disk` per vector, global
/// `hnsw_config` and `quantization_config`. No `SegmentConfig`, no
/// `payload_storage_type`, no per-vector quantization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EdgeShardConfig {
    /// If true, payload is stored on disk (mmap); otherwise in RAM. Same as `CollectionParams::on_disk_payload`.
    #[serde(default = "default_on_disk_payload")]
    pub on_disk_payload: bool,
    /// Dense vector params per vector name.
    #[serde(default)]
    pub vectors: HashMap<VectorNameBuf, EdgeVectorParams>,
    /// Sparse vector params per vector name.
    #[serde(default)]
    pub sparse_vectors: HashMap<VectorNameBuf, EdgeSparseVectorParams>,
    /// Global HNSW config; per-vector override is in `vectors[].index` when `Indexes::Hnsw`.
    #[serde(default)]
    pub hnsw_config: HnswConfig,
    /// Global quantization config for all vectors (no per-vector quantization).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantization_config: Option<QuantizationConfig>,
    #[serde(default)]
    pub optimizers: EdgeOptimizersConfig,
}

fn default_on_disk_payload() -> bool {
    true
}

impl Default for EdgeShardConfig {
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

impl EdgeShardConfig {
    /// Build from existing segment config. Fills all parameters that can be inferred.
    pub fn from_segment_config(segment: &SegmentConfig) -> Self {
        let SegmentConfig {
            vector_data,
            sparse_vector_data,
            payload_storage_type,
        } = segment;
        let on_disk_payload = payload_storage_type.is_on_disk();
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
        let hnsw_config = vector_data
            .values()
            .find_map(|v| match &v.index {
                Indexes::Hnsw(h) => Some(*h),
                Indexes::Plain { .. } => None,
            })
            .unwrap_or_default();
        let quantization_config = vector_data
            .values()
            .find_map(|v| v.quantization_config.as_ref())
            .cloned();
        Self {
            on_disk_payload,
            vectors,
            sparse_vectors,
            hnsw_config,
            quantization_config,
            optimizers: EdgeOptimizersConfig::default(),
        }
    }

    /// Build `SegmentConfig` for creating/checking segments and for optimizers.
    pub fn to_segment_config(&self) -> SegmentConfig {
        let EdgeShardConfig {
            on_disk_payload,
            vectors,
            sparse_vectors,
            quantization_config,
            hnsw_config: _,
            optimizers: _,
        } = self;
        let payload_storage_type = PayloadStorageType::from_on_disk_payload(*on_disk_payload);
        let vector_data = vectors
            .iter()
            .map(|(name, p)| {
                (
                    name.clone(),
                    p.to_vector_data_config(quantization_config.as_ref()),
                )
            })
            .collect();
        let sparse_vector_data = sparse_vectors
            .iter()
            .map(|(name, p)| (name.clone(), p.to_sparse_vector_data_config()))
            .collect();
        SegmentConfig {
            vector_data,
            sparse_vector_data,
            payload_storage_type,
        }
    }

    /// Check compatibility with a segment config (e.g. loaded segment).
    pub fn is_compatible_with_segment_config(&self, other: &SegmentConfig) -> bool {
        self.to_segment_config().is_compatible(other)
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
            .map(|(name, p)| (name.clone(), p.to_sparse_vector_data_config()))
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
        use shard::optimizers::config::{
            DenseVectorOptimizerConfig, SegmentOptimizerConfig, SparseVectorOptimizerConfig,
        };

        let payload_storage_type = PayloadStorageType::from_on_disk_payload(self.on_disk_payload);
        let base_vector_data = self
            .vectors
            .iter()
            .map(|(name, p)| {
                (
                    name.clone(),
                    p.to_plain_vector_data_config(self.quantization_config.as_ref()),
                )
            })
            .collect();
        let base_sparse_vector_data = self
            .sparse_vectors
            .iter()
            .map(|(name, p)| (name.clone(), p.to_sparse_vector_data_config()))
            .collect();
        let dense_vector = self
            .vectors
            .iter()
            .map(|(name, p)| {
                let hnsw_config = match &p.index {
                    Indexes::Plain { .. } => self.hnsw_config,
                    Indexes::Hnsw(h) => *h,
                };
                (
                    name.clone(),
                    DenseVectorOptimizerConfig {
                        on_disk: Some(p.on_disk),
                        hnsw_config,
                        quantization_config:
                            segment::types::QuantizationConfig::for_appendable_segment(
                                self.quantization_config.as_ref(),
                            ),
                    },
                )
            })
            .collect();
        let sparse_vector = self
            .sparse_vectors
            .iter()
            .map(|(name, p)| {
                (
                    name.clone(),
                    SparseVectorOptimizerConfig {
                        on_disk: Some(p.on_disk),
                    },
                )
            })
            .collect();

        SegmentOptimizerConfig {
            payload_storage_type,
            base_vector_data,
            base_sparse_vector_data,
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

    pub fn save(&self, path: &Path) -> segment::common::operation_error::OperationResult<()> {
        let config_path = path.join(EDGE_CONFIG_FILE);
        let contents = serde_json::to_string_pretty(self).map_err(|e| {
            segment::common::operation_error::OperationError::service_error(e.to_string())
        })?;
        fs::write(&config_path, contents).map_err(|e| {
            segment::common::operation_error::OperationError::service_error(format!(
                "failed to write {}: {}",
                config_path.display(),
                e
            ))
        })?;
        Ok(())
    }

    pub fn load(path: &Path) -> Option<segment::common::operation_error::OperationResult<Self>> {
        let config_path = path.join(EDGE_CONFIG_FILE);
        if !config_path.exists() {
            return None;
        }
        let contents = match fs::read_to_string(&config_path) {
            Ok(c) => c,
            Err(e) => {
                return Some(Err(
                    segment::common::operation_error::OperationError::service_error(format!(
                        "failed to read {}: {}",
                        config_path.display(),
                        e
                    )),
                ));
            }
        };
        Some(serde_json::from_str(&contents).map_err(|e| {
            segment::common::operation_error::OperationError::service_error(format!(
                "failed to parse {}: {}",
                config_path.display(),
                e
            ))
        }))
    }

    pub fn set_hnsw_config(&mut self, hnsw_config: HnswConfig) {
        self.hnsw_config = hnsw_config;
    }

    pub fn set_vector_hnsw_config(
        &mut self,
        vector_name: &str,
        hnsw_config: HnswConfig,
    ) -> segment::common::operation_error::OperationResult<()> {
        let name = VectorNameBuf::from(vector_name);
        let params = self.vectors.get_mut(&name).ok_or_else(|| {
            segment::common::operation_error::OperationError::service_error(format!(
                "vector '{vector_name}' not found in config"
            ))
        })?;
        params.index = Indexes::Hnsw(hnsw_config);
        Ok(())
    }

    pub fn set_optimizers_config(&mut self, optimizers: EdgeOptimizersConfig) {
        self.optimizers = optimizers;
    }

    pub fn optimizers_mut(&mut self) -> &mut EdgeOptimizersConfig {
        &mut self.optimizers
    }
}
