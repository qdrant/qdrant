//! Optimizer-related parameters for the edge shard.

use serde::{Deserialize, Serialize};
use shard::optimizers::config::{
    get_indexing_threshold_kb, get_max_segment_size_kb, get_number_segments,
};

/// Optimizer-related parameters for the edge shard.
///
/// Subset of collection-level `OptimizersConfig`: excludes `memmap_threshold`
/// (deprecated), `flush_interval_sec` (edge does not flush on a timer), and
/// `max_optimization_threads` (optimizations are manual).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default)]
pub struct EdgeOptimizersConfig {
    /// Minimal fraction of deleted vectors in a segment required to run vacuum.
    #[serde(default)]
    pub deleted_threshold: Option<f64>,
    /// Minimal number of vectors in a segment required to run vacuum.
    #[serde(default)]
    pub vacuum_min_vector_number: Option<usize>,
    /// Target number of segments. If 0, chosen automatically from CPU count.
    pub default_segment_number: Option<usize>,
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

impl EdgeOptimizersConfig {
    pub fn get_number_segments(&self) -> usize {
        get_number_segments(self.default_segment_number.unwrap_or_default())
    }

    pub fn get_indexing_threshold_kb(&self) -> usize {
        get_indexing_threshold_kb(self.indexing_threshold)
    }

    pub fn get_max_segment_size_kb(&self, num_indexing_threads: usize) -> usize {
        get_max_segment_size_kb(self.max_segment_size, num_indexing_threads)
    }
}
