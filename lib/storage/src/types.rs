use serde::{Deserialize, Serialize};
use collection::collection_builder::optimizers_builder::OptimizersConfig;


#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PerformanceConfig {
    pub max_search_threads: usize,
    pub max_optimize_threads: usize,
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

