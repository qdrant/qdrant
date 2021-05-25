use serde::{Deserialize, Serialize};
use schemars::{JsonSchema};
use segment::types::{HnswConfig, Distance};
use crate::collection_builder::optimizers_builder::OptimizersConfig;
use wal::WalOptions;


#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct WalConfig {
    /// Size of a single WAL segment in MB
    pub wal_capacity_mb: usize,
    /// Number of WAL segments to create ahead of actually used ones
    pub wal_segments_ahead: usize,
}

impl From<&WalConfig> for WalOptions {
    fn from(config: &WalConfig) -> Self {
        WalOptions {
            segment_capacity: config.wal_capacity_mb * 1024 * 1024,
            segment_queue_len: config.wal_segments_ahead
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self { WalConfig { wal_capacity_mb: 32, wal_segments_ahead: 0 } }
}


#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct CollectionParams {
    /// Size of a vectors used
    pub vector_size: usize,
    /// Type of distance function used for measuring distance between vectors
    pub distance: Distance
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct CollectionConfig {
    pub params: CollectionParams,
    pub hnsw_config: HnswConfig,
    pub optimizer_config: OptimizersConfig,
    pub wal_config: WalConfig,
}

