//! Configuration for concurrent load limits at collection, shard, and segment levels.

use std::num::NonZeroUsize;

use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};

const DEFAULT_MAX_CONCURRENT_COLLECTION_LOADS: usize = 1;
const DEFAULT_MAX_CONCURRENT_SHARD_LOADS: usize = 1;
const DEFAULT_MAX_CONCURRENT_SEGMENT_LOADS: usize = 8;

/// Configuration for concurrent load limits.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct LoadConcurrencyConfig {
    /// Maximum number of collections to load concurrently.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "non_zero_num_or_string"
    )]
    pub max_concurrent_collection_loads: Option<NonZeroUsize>,
    /// Maximum number of shards to load concurrently when loading a collection.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "non_zero_num_or_string"
    )]
    pub max_concurrent_shard_loads: Option<NonZeroUsize>,
    /// Maximum number of segments to load concurrently when loading a shard.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "non_zero_num_or_string"
    )]
    pub max_concurrent_segment_loads: Option<NonZeroUsize>,
}

impl LoadConcurrencyConfig {
    pub fn get_concurrent_collections(&self) -> NonZeroUsize {
        self.max_concurrent_collection_loads
            .unwrap_or(NonZeroUsize::new(DEFAULT_MAX_CONCURRENT_COLLECTION_LOADS).unwrap())
    }

    pub fn get_concurrent_shards(&self) -> NonZeroUsize {
        self.max_concurrent_shard_loads
            .unwrap_or(NonZeroUsize::new(DEFAULT_MAX_CONCURRENT_SHARD_LOADS).unwrap())
    }

    pub fn get_concurrent_segments(&self) -> NonZeroUsize {
        self.max_concurrent_segment_loads
            .unwrap_or(NonZeroUsize::new(DEFAULT_MAX_CONCURRENT_SEGMENT_LOADS).unwrap())
    }
}

/// Helper to accept string inputs from environment variables
fn non_zero_num_or_string<'de, D>(deserializer: D) -> Result<Option<NonZeroUsize>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Helper {
        Number(Option<NonZeroUsize>),
        String(Option<String>),
    }

    match Helper::deserialize(deserializer)? {
        Helper::Number(n) => Ok(n),
        Helper::String(Some(n)) => Ok(Some(n.parse::<NonZeroUsize>().map_err(D::Error::custom)?)),
        Helper::String(None) => Ok(None),
    }
}
