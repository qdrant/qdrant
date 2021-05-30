use serde::{Deserialize, Serialize};
use schemars::{JsonSchema};
use collection::collection_builder::optimizers_builder::OptimizersConfig;
use collection::config::WalConfig;
use segment::types::HnswConfig;


#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct PerformanceConfig {
    pub max_search_threads: usize,
}


#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct StorageConfig {
    pub storage_path: String,
    pub optimizers: OptimizersConfig,
    pub wal: WalConfig,
    pub performance: PerformanceConfig,
    pub hnsw_index: HnswConfig
}

