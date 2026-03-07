//! Optimizer-related parameters for the edge shard.

use serde::{Deserialize, Serialize};
use shard::optimizers::config::{
    DEFAULT_DELETED_THRESHOLD, DEFAULT_INDEXING_THRESHOLD_KB, DEFAULT_MAX_SEGMENT_PER_CPU_KB,
    DEFAULT_VACUUM_MIN_VECTOR_NUMBER, default_segment_number,
};

/// Optimizer-related parameters for the edge shard.
///
/// Subset of collection-level `OptimizersConfig`: excludes `memmap_threshold`
/// (deprecated), `flush_interval_sec` (edge does not flush on a timer), and
/// `max_optimization_threads` (optimizations are manual).
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
