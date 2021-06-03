use serde::{Deserialize, Serialize};
use merge::Merge;
use serde::de::DeserializeOwned;
use schemars::{JsonSchema};
use crate::operations::types::CollectionResult;
use segment::types::HnswConfig;
use crate::config::WalConfig;
use crate::collection_builder::optimizers_builder::OptimizersConfig;

// Structures for partial update of collection params
// ToDo: Make auto-generated somehow...

pub trait DiffConfig<T: DeserializeOwned + Serialize> {
    fn update(self, config: &T) -> CollectionResult<T>
        where Self: Sized,
              Self: Serialize,
              Self: DeserializeOwned,
              Self: Merge { update_config(config, self) }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq, Merge)]
#[serde(rename_all = "snake_case")]
pub struct HnswConfigDiff {
    /// Number of edges per node in the index graph. Larger the value - more accurate the search, more space required.
    pub m: Option<usize>,
    /// Number of neighbours to consider during the index building. Larger the value - more accurate the search, more time required to build index.
    pub ef_construct: Option<usize>,
    /// Minimal amount of points for additional payload-based indexing.
    /// If payload chunk is smaller than `full_scan_threshold` additional indexing won't be used -
    /// in this case full-scan search should be preferred by query planner and additional indexing is not required.
    pub full_scan_threshold: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Merge)]
pub struct WalConfigDiff {
    /// Size of a single WAL segment in MB
    pub wal_capacity_mb: Option<usize>,
    /// Number of WAL segments to create ahead of actually used ones
    pub wal_segments_ahead: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Merge)]
pub struct OptimizersConfigDiff {
    /// The minimal fraction of deleted vectors in a segment, required to perform segment optimization
    pub deleted_threshold: Option<f64>,
    /// The minimal number of vectors in a segment, required to perform segment optimization
    pub vacuum_min_vector_number: Option<usize>,
    /// If the number of segments exceeds this value, the optimizer will merge the smallest segments.
    pub max_segment_number: Option<usize>,
    /// Maximum number of vectors to store in-memory per segment.
    /// Segments larger than this threshold will be stored as read-only memmaped file.
    pub memmap_threshold: Option<usize>,
    /// Maximum number of vectors allowed for plain index.
    /// Default value based on https://github.com/google-research/google-research/blob/master/scann/docs/algorithms.md
    pub indexing_threshold: Option<usize>,
    /// Starting from this amount of vectors per-segment the engine will start building index for payload.
    pub payload_indexing_threshold: Option<usize>,
    /// Minimum interval between forced flushes.
    pub flush_interval_sec: Option<u64>,
}

impl DiffConfig<HnswConfig> for HnswConfigDiff {}

impl DiffConfig<OptimizersConfig> for OptimizersConfigDiff {}

impl DiffConfig<WalConfig> for WalConfigDiff {}

/// Hacky way to update configuration structures with diff-updates.
/// Intended to only be used in non critical for speed places.
/// ToDo: Replace with proc macro
pub fn update_config<T: DeserializeOwned + Serialize, Y: DeserializeOwned + Serialize + Merge>(config: &T, mut update: Y) -> CollectionResult<T> {
    let serialized = serde_json::to_vec(config)?;
    let config_as_diff: Y = serde_json::from_slice(serialized.as_slice())?;
    update.merge(config_as_diff);
    let serialized = serde_json::to_vec(&update)?;
    let res = serde_json::from_slice(serialized.as_slice())?;
    Ok(res)
}


#[cfg(test)]
mod tests {
    use super::*;
    use segment::types::HnswConfig;
    use crate::collection_builder::optimizers_builder::OptimizersConfig;

    #[test]
    fn test_hnsw_update() {
        let base_config = HnswConfig::default();
        let update: HnswConfigDiff = serde_json::from_str(&r#"{ "m": 32 }"#).unwrap();
        let new_config = update.update(&base_config).unwrap();
        assert_eq!(new_config.m, 32)
    }

    #[test]
    fn test_optimizer_update() {
        let base_config = OptimizersConfig {
            deleted_threshold: 0.9,
            vacuum_min_vector_number: 1000,
            max_segment_number: 10,
            memmap_threshold: 100_000,
            indexing_threshold: 50_000,
            payload_indexing_threshold: 20_000,
            flush_interval_sec: 30,
        };
        let update: OptimizersConfigDiff = serde_json::from_str(&r#"{ "indexing_threshold": 10000 }"#).unwrap();
        let new_config = update.update(&base_config).unwrap();
        assert_eq!(new_config.indexing_threshold, 10000)
    }

    #[test]
    fn test_wal_config() {
        let base_config = WalConfig::default();
        let update: WalConfigDiff = serde_json::from_str(&r#"{ "wal_segments_ahead": 2 }"#).unwrap();
        let new_config = update.update(&base_config).unwrap();
        assert_eq!(new_config.wal_segments_ahead, 2)
    }
}
