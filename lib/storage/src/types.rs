use serde::{Deserialize, Serialize};


#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PerformanceConfig {
    pub max_search_threads: usize,
    pub max_optimize_threads: usize,
}


#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OptimizersConfig {
    pub deleted_threshold: f64,
    pub vacuum_min_vector_number: usize,
    pub max_segment_number: usize,
    pub memmap_threshold: usize,
    pub indexing_threshold: usize
}


#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WalConfig {
    pub wal_capacity_mb: usize,
    pub wal_segments_ahead: usize,
}


#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StorageConfig {
    pub storage_path: String,
    pub optimizers: OptimizersConfig,
    pub wal: WalConfig,
    pub performance: PerformanceConfig,
}

