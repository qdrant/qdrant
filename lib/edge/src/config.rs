//! Configuration for [`crate::EdgeShard`].
//!
//! [`EdgeShardConfig`] holds all parameters required by the edge shard: segment-level
//! config (vector data, sparse vectors, payload storage), global and per-vector HNSW
//! settings, and optimizer parameters. It is persisted as JSON when provided on
//! initialization or when updated via the public API.

use std::path::Path;

use fs_err as fs;
use segment::types::{HnswConfig, SegmentConfig};
use serde::{Deserialize, Serialize};
use shard::optimizers::config::{
    DEFAULT_DELETED_THRESHOLD, DEFAULT_INDEXING_THRESHOLD_KB, DEFAULT_MAX_SEGMENT_PER_CPU_KB,
    DEFAULT_VACUUM_MIN_VECTOR_NUMBER, default_segment_number,
};

/// File name for the persisted edge shard config.
pub const EDGE_CONFIG_FILE: &str = "edge_config.json";

/// Optimizer-related parameters for the edge shard.
///
/// Subset of collection-level [`OptimizersConfig`](https://docs.rs/collection/latest/collection/optimizers_builder/struct.OptimizersConfig.html):
/// excludes `memmap_threshold` (deprecated), `flush_interval_sec` (edge does not flush
/// on a timer), and `max_optimization_threads` (optimizations are manual).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default)]
pub struct EdgeOptimizersConfig {
    /// Minimal fraction of deleted vectors in a segment required to run vacuum.
    #[serde(default = "default_deleted_threshold")]
    pub deleted_threshold: f64,
    /// Minimal number of vectors in a segment required to run vacuum.
    #[serde(default = "default_vacuum_min_vector_number")]
    pub vacuum_min_vector_number: usize,
    /// Target number of segments. If 0, chosen automatically from CPU count.
    pub default_segment_number: usize,
    /// Max segment size in KB. If not set, derived from CPU count.
    #[serde(alias = "max_segment_size_kb")]
    pub max_segment_size: Option<usize>,
    /// Indexing threshold in KB; segments above this get HNSW index. If not set, default is used.
    #[serde(alias = "indexing_threshold_kb")]
    pub indexing_threshold: Option<usize>,
    /// If true, block updates when unoptimized segments exceed indexing threshold.
    #[serde(default)]
    pub prevent_unoptimized: Option<bool>,
}

fn default_deleted_threshold() -> f64 {
    DEFAULT_DELETED_THRESHOLD
}

fn default_vacuum_min_vector_number() -> usize {
    DEFAULT_VACUUM_MIN_VECTOR_NUMBER
}

impl Default for EdgeOptimizersConfig {
    fn default() -> Self {
        Self {
            deleted_threshold: default_deleted_threshold(),
            vacuum_min_vector_number: default_vacuum_min_vector_number(),
            default_segment_number: 0,
            max_segment_size: None,
            indexing_threshold: None,
            prevent_unoptimized: None,
        }
    }
}

impl EdgeOptimizersConfig {
    pub fn get_number_segments(&self) -> usize {
        if self.default_segment_number == 0 {
            default_segment_number()
        } else {
            self.default_segment_number
        }
    }

    pub fn get_indexing_threshold_kb(&self) -> usize {
        match self.indexing_threshold {
            None => DEFAULT_INDEXING_THRESHOLD_KB,
            Some(0) => usize::MAX,
            Some(custom) => custom,
        }
    }

    pub fn get_max_segment_size_kb(&self, num_indexing_threads: usize) -> usize {
        if let Some(max_segment_size) = self.max_segment_size {
            max_segment_size
        } else {
            num_indexing_threads.saturating_mul(DEFAULT_MAX_SEGMENT_PER_CPU_KB)
        }
    }
}

/// Full configuration for an edge shard.
///
/// Combines segment-level config (vector data, sparse vectors, payload storage),
/// global HNSW defaults, per-vector HNSW (inside [`SegmentConfig`]), and
/// optimizer parameters. Persisted as `edge_config.json` in the shard directory
/// when provided on load or when updated via [`crate::EdgeShard`] methods.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EdgeShardConfig {
    /// Segment-level config: vector data, sparse vector data, payload storage type.
    pub segment: SegmentConfig,
    /// Global HNSW config used when a vector has plain index or as base for per-vector overrides.
    #[serde(default)]
    pub hnsw_config: HnswConfig,
    /// Optimizer parameters (no flush interval, no max_optimization_threads).
    #[serde(default)]
    pub optimizers: EdgeOptimizersConfig,
}

impl EdgeShardConfig {
    /// Build config from existing segment config and defaults for HNSW and optimizers.
    pub fn from_segment_config(segment: SegmentConfig) -> Self {
        Self {
            segment,
            hnsw_config: HnswConfig::default(),
            optimizers: EdgeOptimizersConfig::default(),
        }
    }

    /// Segment config for creating/checking segments.
    pub fn segment_config(&self) -> &SegmentConfig {
        &self.segment
    }

    /// Check compatibility with another config (same vector names and compatible vector/sparse configs).
    pub fn is_compatible_with_segment_config(&self, other: &SegmentConfig) -> bool {
        self.segment.is_compatible(other)
    }

    /// Persist config to shard path.
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

    /// Load config from shard path if file exists.
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

    /// Update global HNSW config. Does not change per-vector HNSW stored in `segment.vector_data`.
    pub fn set_hnsw_config(&mut self, hnsw_config: HnswConfig) {
        self.hnsw_config = hnsw_config;
    }

    /// Update HNSW config for a specific vector by name.
    /// Fails if the vector does not exist (immutable: dimensionality and distance cannot be changed here).
    pub fn set_vector_hnsw_config(
        &mut self,
        vector_name: &str,
        hnsw_config: HnswConfig,
    ) -> segment::common::operation_error::OperationResult<()> {
        use segment::types::{Indexes, VectorNameBuf};
        let name = VectorNameBuf::from(vector_name);
        let config = self.segment.vector_data.get_mut(&name).ok_or_else(|| {
            segment::common::operation_error::OperationError::service_error(format!(
                "vector '{vector_name}' not found in config"
            ))
        })?;
        config.index = Indexes::Hnsw(hnsw_config);
        Ok(())
    }

    /// Update optimizer parameters.
    pub fn set_optimizers_config(&mut self, optimizers: EdgeOptimizersConfig) {
        self.optimizers = optimizers;
    }

    /// Get mutable reference to optimizer config for fine-grained updates.
    pub fn optimizers_mut(&mut self) -> &mut EdgeOptimizersConfig {
        &mut self.optimizers
    }
}
