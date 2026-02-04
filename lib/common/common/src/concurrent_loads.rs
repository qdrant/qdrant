//! Configuration for concurrent load limits at collection, shard, and segment levels.

use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

const DEFAULT_MAX_CONCURRENT_COLLECTION_LOADS: usize = 1;
const DEFAULT_MAX_CONCURRENT_SHARD_LOADS: usize = 1;
const DEFAULT_MAX_CONCURRENT_SEGMENT_LOADS: usize = 8;

static MAX_CONCURRENT_COLLECTION_LOADS: AtomicUsize =
    AtomicUsize::new(DEFAULT_MAX_CONCURRENT_COLLECTION_LOADS);
static MAX_CONCURRENT_SHARD_LOADS: AtomicUsize =
    AtomicUsize::new(DEFAULT_MAX_CONCURRENT_SHARD_LOADS);
static MAX_CONCURRENT_SEGMENT_LOADS: AtomicUsize =
    AtomicUsize::new(DEFAULT_MAX_CONCURRENT_SEGMENT_LOADS);

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

/// Sets the global concurrent loads configuration.
///
/// Values of `Some(0)` are treated as invalid and replaced with defaults.
pub fn set_concurrent_loads_config(config: &ConcurrentLoadConfig) {
    MAX_CONCURRENT_COLLECTION_LOADS.store(
        resolve_value(
            config.max_concurrent_collection_loads,
            DEFAULT_MAX_CONCURRENT_COLLECTION_LOADS,
            "collection",
        ),
        Ordering::Relaxed,
    );
    MAX_CONCURRENT_SHARD_LOADS.store(
        resolve_value(
            config.max_concurrent_shard_loads,
            DEFAULT_MAX_CONCURRENT_SHARD_LOADS,
            "shard",
        ),
        Ordering::Relaxed,
    );
    MAX_CONCURRENT_SEGMENT_LOADS.store(
        resolve_value(
            config.max_concurrent_segment_loads,
            DEFAULT_MAX_CONCURRENT_SEGMENT_LOADS,
            "segment",
        ),
        Ordering::Relaxed,
    );
}

fn resolve_value(value: Option<usize>, default: usize, name: &str) -> usize {
    match value {
        None => default,
        Some(0) => {
            log::warn!(
                "Received 0 for max concurrent {name} loads config, using default {default}"
            );
            default
        }
        Some(v) => v,
    }
}

pub fn max_concurrent_collection_loads() -> usize {
    MAX_CONCURRENT_COLLECTION_LOADS.load(Ordering::Relaxed)
}

pub fn max_concurrent_shard_loads() -> usize {
    MAX_CONCURRENT_SHARD_LOADS.load(Ordering::Relaxed)
}

pub fn max_concurrent_segment_loads() -> usize {
    MAX_CONCURRENT_SEGMENT_LOADS.load(Ordering::Relaxed)
}
