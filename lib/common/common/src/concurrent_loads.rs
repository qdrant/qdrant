//! Configuration for concurrent load limits at collection, shard, and segment levels.

use serde::{Deserialize, Serialize};

const DEFAULT_MAX_CONCURRENT_COLLECTION_LOADS: usize = 1;
const DEFAULT_MAX_CONCURRENT_SHARD_LOADS: usize = 1;
const DEFAULT_MAX_CONCURRENT_SEGMENT_LOADS: usize = 8;

/// Configuration for concurrent load limits.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct ConcurrentLoadConfig {
    /// Maximum number of collections to load concurrently.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent_collection_loads: Option<usize>,
    /// Maximum number of shards to load concurrently when loading a collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent_shard_loads: Option<usize>,
    /// Maximum number of segments to load concurrently when loading a shard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent_segment_loads: Option<usize>,
}

impl ConcurrentLoadConfig {
    pub fn get_concurrent_collections(&self) -> usize {
        self.max_concurrent_collection_loads
            .unwrap_or(DEFAULT_MAX_CONCURRENT_COLLECTION_LOADS)
    }

    pub fn get_concurrent_shards(&self) -> usize {
        self.max_concurrent_shard_loads
            .unwrap_or(DEFAULT_MAX_CONCURRENT_SHARD_LOADS)
    }

    pub fn get_concurrent_segments(&self) -> usize {
        self.max_concurrent_segment_loads
            .unwrap_or(DEFAULT_MAX_CONCURRENT_SEGMENT_LOADS)
    }
}
