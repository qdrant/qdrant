use std::num::NonZeroU32;

use merge::Merge;
use schemars::JsonSchema;
use segment::types::HnswConfig;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{CollectionParams, WalConfig};
use crate::operations::types::CollectionResult;
use crate::optimizers_builder::OptimizersConfig;

// Structures for partial update of collection params
// ToDo: Make auto-generated somehow...

pub trait DiffConfig<T: DeserializeOwned + Serialize> {
    fn update(self, config: &T) -> CollectionResult<T>
    where
        Self: Sized,
        Self: Serialize,
        Self: DeserializeOwned,
        Self: Merge,
    {
        update_config(config, self)
    }

    fn from_full(full: &T) -> CollectionResult<Self>
    where
        Self: Sized,
        Self: Serialize,
        Self: DeserializeOwned,
    {
        from_full(full)
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq, Merge, Hash)]
#[serde(rename_all = "snake_case")]
pub struct HnswConfigDiff {
    /// Number of edges per node in the index graph. Larger the value - more accurate the search, more space required.
    pub m: Option<usize>,
    /// Number of neighbours to consider during the index building. Larger the value - more accurate the search, more time required to build index.
    pub ef_construct: Option<usize>,
    /// Minimal size (in KiloBytes) of vectors for additional payload-based indexing.
    /// If payload chunk is smaller than `full_scan_threshold_kb` additional indexing won't be used -
    /// in this case full-scan search should be preferred by query planner and additional indexing is not required.
    /// Note: 1Kb = 1 vector of size 256
    #[serde(alias = "full_scan_threshold_kb")]
    pub full_scan_threshold: Option<usize>,
    /// Number of parallel threads used for background index building. If 0 - auto selection.
    #[serde(default)]
    pub max_indexing_threads: Option<usize>,
    /// Store HNSW index on disk. If set to false, index will be stored in RAM. Default: false
    #[serde(default)]
    pub on_disk: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Merge, PartialEq, Eq, Hash)]
pub struct WalConfigDiff {
    /// Size of a single WAL segment in MB
    pub wal_capacity_mb: Option<usize>,
    /// Number of WAL segments to create ahead of actually used ones
    pub wal_segments_ahead: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Merge, PartialEq, Eq, Hash)]
pub struct CollectionParamsDiff {
    /// Number of replicas for each shard
    pub replication_factor: Option<NonZeroU32>,
    /// Minimal number successful responses from replicas to consider operation successful
    pub write_consistency_factor: Option<NonZeroU32>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Merge)]
pub struct OptimizersConfigDiff {
    /// The minimal fraction of deleted vectors in a segment, required to perform segment optimization
    pub deleted_threshold: Option<f64>,
    /// The minimal number of vectors in a segment, required to perform segment optimization
    pub vacuum_min_vector_number: Option<usize>,
    /// Target amount of segments optimizer will try to keep.
    /// Real amount of segments may vary depending on multiple parameters:
    ///  - Amount of stored points
    ///  - Current write RPS
    ///
    /// It is recommended to select default number of segments as a factor of the number of search threads,
    /// so that each segment would be handled evenly by one of the threads
    /// If `default_segment_number = 0`, will be automatically selected by the number of available CPUs
    pub default_segment_number: Option<usize>,
    /// Do not create segments larger this size (in KiloBytes).
    /// Large segments might require disproportionately long indexation times,
    /// therefore it makes sense to limit the size of segments.
    ///
    /// If indexation speed have more priority for your - make this parameter lower.
    /// If search speed is more important - make this parameter higher.
    /// Note: 1Kb = 1 vector of size 256
    #[serde(alias = "max_segment_size_kb")]
    pub max_segment_size: Option<usize>,
    /// Maximum size (in KiloBytes) of vectors to store in-memory per segment.
    /// Segments larger than this threshold will be stored as read-only memmaped file.
    /// To enable memmap storage, lower the threshold
    /// Note: 1Kb = 1 vector of size 256
    #[serde(alias = "memmap_threshold_kb")]
    pub memmap_threshold: Option<usize>,
    /// Maximum size (in KiloBytes) of vectors allowed for plain index.
    /// Default value based on <https://github.com/google-research/google-research/blob/master/scann/docs/algorithms.md>
    /// Note: 1Kb = 1 vector of size 256
    #[serde(alias = "indexing_threshold_kb")]
    pub indexing_threshold: Option<usize>,
    /// Minimum interval between forced flushes.
    pub flush_interval_sec: Option<u64>,
    /// Maximum available threads for optimization workers
    pub max_optimization_threads: Option<usize>,
}

impl std::hash::Hash for OptimizersConfigDiff {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.deleted_threshold.map(f64::to_le_bytes).hash(state);
        self.vacuum_min_vector_number.hash(state);
        self.default_segment_number.hash(state);
        self.max_segment_size.hash(state);
        self.memmap_threshold.hash(state);
        self.indexing_threshold.hash(state);
        self.flush_interval_sec.hash(state);
        self.max_optimization_threads.hash(state);
    }
}

impl PartialEq for OptimizersConfigDiff {
    fn eq(&self, other: &Self) -> bool {
        self.deleted_threshold.map(f64::to_le_bytes)
            == other.deleted_threshold.map(f64::to_le_bytes)
            && self.vacuum_min_vector_number == other.vacuum_min_vector_number
            && self.default_segment_number == other.default_segment_number
            && self.max_segment_size == other.max_segment_size
            && self.memmap_threshold == other.memmap_threshold
            && self.indexing_threshold == other.indexing_threshold
            && self.flush_interval_sec == other.flush_interval_sec
            && self.max_optimization_threads == other.max_optimization_threads
    }
}

impl Eq for OptimizersConfigDiff {}

impl DiffConfig<HnswConfig> for HnswConfigDiff {}

impl DiffConfig<OptimizersConfig> for OptimizersConfigDiff {}

impl DiffConfig<WalConfig> for WalConfigDiff {}

impl DiffConfig<CollectionParams> for CollectionParamsDiff {}

impl From<HnswConfig> for HnswConfigDiff {
    fn from(config: HnswConfig) -> Self {
        HnswConfigDiff::from_full(&config).unwrap()
    }
}

impl From<OptimizersConfig> for OptimizersConfigDiff {
    fn from(config: OptimizersConfig) -> Self {
        OptimizersConfigDiff::from_full(&config).unwrap()
    }
}

impl From<WalConfig> for WalConfigDiff {
    fn from(config: WalConfig) -> Self {
        WalConfigDiff::from_full(&config).unwrap()
    }
}

impl From<CollectionParams> for CollectionParamsDiff {
    fn from(config: CollectionParams) -> Self {
        CollectionParamsDiff::from_full(&config).unwrap()
    }
}

pub fn from_full<T: DeserializeOwned + Serialize, Y: DeserializeOwned + Serialize>(
    full_config: &T,
) -> CollectionResult<Y> {
    let json = serde_json::to_value(full_config)?;
    let res = serde_json::from_value(json)?;
    Ok(res)
}

/// Merge first level of json values, if diff values present explicitly
///
/// Example:
///
/// base: {"a": 1, "b": 2}
/// diff: {"a": 3}
/// result: {"a": 3, "b": 2}
///
/// base: {"a": 1, "b": 2}
/// diff: {"a": null}
/// result: {"a": 1, "b": 2}
fn merge_level_0(base: &mut Value, diff: Value) {
    match (base, diff) {
        (base @ &mut Value::Object(_), Value::Object(diff)) => {
            let base = base.as_object_mut().unwrap();
            for (k, v) in diff {
                if !v.is_null() {
                    base.insert(k, v);
                }
            }
        }
        (_base, _diff) => {}
    }
}

/// Hacky way to update configuration structures with diff-updates.
/// Intended to only be used in non critical for speed places.
/// ToDo: Replace with proc macro
pub fn update_config<T: DeserializeOwned + Serialize, Y: DeserializeOwned + Serialize + Merge>(
    config: &T,
    update: Y,
) -> CollectionResult<T> {
    let mut config_values = serde_json::to_value(config)?;
    let diff_values = serde_json::to_value(&update)?;
    merge_level_0(&mut config_values, diff_values);
    let res = serde_json::from_value(config_values)?;
    Ok(res)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use segment::types::{Distance, HnswConfig};

    use super::*;
    use crate::operations::types::VectorParams;
    use crate::optimizers_builder::OptimizersConfig;

    #[test]
    fn test_update_collection_params() {
        let params = CollectionParams {
            vectors: VectorParams {
                size: NonZeroU64::new(128).unwrap(),
                distance: Distance::Cosine,
            }
            .into(),
            shard_number: NonZeroU32::new(1).unwrap(),
            replication_factor: NonZeroU32::new(1).unwrap(),
            write_consistency_factor: NonZeroU32::new(1).unwrap(),
            on_disk_payload: false,
        };

        let diff = CollectionParamsDiff {
            replication_factor: None,
            write_consistency_factor: Some(NonZeroU32::new(2).unwrap()),
        };

        let new_params = diff.update(&params).unwrap();

        assert_eq!(new_params.replication_factor.get(), 1);
        assert_eq!(new_params.write_consistency_factor.get(), 2);
    }

    #[test]
    fn test_hnsw_update() {
        let base_config = HnswConfig::default();
        let update: HnswConfigDiff = serde_json::from_str(r#"{ "m": 32 }"#).unwrap();
        let new_config = update.update(&base_config).unwrap();
        assert_eq!(new_config.m, 32)
    }

    #[test]
    fn test_optimizer_update() {
        let base_config = OptimizersConfig {
            deleted_threshold: 0.9,
            vacuum_min_vector_number: 1000,
            default_segment_number: 10,
            max_segment_size: None,
            memmap_threshold: None,
            indexing_threshold: 50_000,
            flush_interval_sec: 30,
            max_optimization_threads: 1,
        };
        let update: OptimizersConfigDiff =
            serde_json::from_str(r#"{ "indexing_threshold": 10000 }"#).unwrap();
        let new_config = update.update(&base_config).unwrap();
        assert_eq!(new_config.indexing_threshold, 10000)
    }

    #[test]
    fn test_wal_config() {
        let base_config = WalConfig::default();
        let update: WalConfigDiff = serde_json::from_str(r#"{ "wal_segments_ahead": 2 }"#).unwrap();
        let new_config = update.update(&base_config).unwrap();
        assert_eq!(new_config.wal_segments_ahead, 2)
    }
}
