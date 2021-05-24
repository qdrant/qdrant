use serde::{Deserialize, Serialize};
use schemars::{JsonSchema};
use segment::types::{HnswConfig, Distance};
use crate::collection_builder::optimizers_builder::OptimizersConfig;


#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct WalConfig {
    pub wal_capacity_mb: usize,
    pub wal_segments_ahead: usize,
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

