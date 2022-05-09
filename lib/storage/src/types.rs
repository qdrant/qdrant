use std::collections::HashMap;

use collection::config::WalConfig;
use collection::optimizers_builder::OptimizersConfig;
use schemars::JsonSchema;
use segment::types::HnswConfig;
use serde::{Deserialize, Serialize};
use tonic::transport::Uri;

pub type PeerAddressById = HashMap<u64, Uri>;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct PerformanceConfig {
    pub max_search_threads: usize,
}

/// Global configuration of the storage, loaded on the service launch, default stored in ./config
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct StorageConfig {
    pub storage_path: String,
    pub optimizers: OptimizersConfig,
    pub wal: WalConfig,
    pub performance: PerformanceConfig,
    pub hnsw_index: HnswConfig,
}
