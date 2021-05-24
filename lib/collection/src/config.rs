use serde::{Deserialize, Serialize};
use schemars::{JsonSchema};
use segment::types::HnswConfig;
use crate::collection_builder::optimizers_builder::OptimizersConfig;


#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct WalConfig {
    pub wal_capacity_mb: usize,
    pub wal_segments_ahead: usize,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct CollectionConfig {
    pub hnsw_config: HnswConfig,
    pub optimizer_config: OptimizersConfig,
    pub wal_config: WalConfig,
}

