//! Structures for partial update of collection params

#![allow(deprecated)] // hack to remove warning for memmap_threshold deprecation below

use std::num::NonZeroU32;

use api::rest::MaxOptimizationThreads;
use schemars::JsonSchema;
use segment::types::{
    BinaryQuantization, HnswConfig, ProductQuantization, ScalarQuantization, StrictModeConfig,
};
use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationErrors};

use crate::config::{CollectionParams, WalConfig};
use crate::optimizers_builder::OptimizersConfig;

pub trait DiffConfig<Diff>: Clone {
    /// Update this config with field from `diff`
    ///
    /// The `diff` has higher priority, meaning that fields specified in
    /// the `diff` will always be in the returned object.
    fn update(&self, diff: &Diff) -> Self;

    fn update_opt(&self, diff: Option<&Diff>) -> Self {
        match diff {
            Some(diff) => self.update(diff),
            None => self.clone(),
        }
    }
}

#[derive(
    Debug, Default, Deserialize, Serialize, JsonSchema, Validate, Copy, Clone, PartialEq, Eq, Hash,
)]
#[serde(rename_all = "snake_case")]
pub struct HnswConfigDiff {
    /// Number of edges per node in the index graph. Larger the value - more accurate the search, more space required.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub m: Option<usize>,
    /// Number of neighbours to consider during the index building. Larger the value - more accurate the search, more time required to build the index.
    #[validate(range(min = 4))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_construct: Option<usize>,
    /// Minimal size threshold (in KiloBytes) below which full-scan is preferred over HNSW search.
    /// This measures the total size of vectors being queried against.
    /// When the maximum estimated amount of points that a condition satisfies is smaller than
    /// `full_scan_threshold_kb`, the query planner will use full-scan search instead of HNSW index
    /// traversal for better performance.
    /// Note: 1Kb = 1 vector of size 256
    #[serde(
        alias = "full_scan_threshold_kb",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    #[validate(range(min = 10))]
    pub full_scan_threshold: Option<usize>,
    /// Number of parallel threads used for background index building.
    /// If 0 - automatically select from 8 to 16.
    /// Best to keep between 8 and 16 to prevent likelihood of building broken/inefficient HNSW graphs.
    /// On small CPUs, less threads are used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_indexing_threads: Option<usize>,
    /// Store HNSW index on disk. If set to false, the index will be stored in RAM. Default: false
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
    /// Custom M param for additional payload-aware HNSW links. If not set, default M will be used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_m: Option<usize>,
    /// Store copies of original and quantized vectors within the HNSW index file. Default: false.
    /// Enabling this option will trade the search speed for disk usage by reducing amount of
    /// random seeks during the search.
    /// Requires quantized vectors to be enabled. Multi-vectors are not supported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline_storage: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Eq, Hash)]
pub struct WalConfigDiff {
    /// Size of a single WAL segment in MB
    #[validate(range(min = 1))]
    pub wal_capacity_mb: Option<usize>,
    /// Number of WAL segments to create ahead of actually used ones
    pub wal_segments_ahead: Option<usize>,
    /// Number of closed WAL segments to retain
    pub wal_retain_closed: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
pub struct CollectionParamsDiff {
    /// Number of replicas for each shard
    pub replication_factor: Option<NonZeroU32>,
    /// Minimal number successful responses from replicas to consider operation successful
    pub write_consistency_factor: Option<NonZeroU32>,
    /// Fan-out every read request to these many additional remote nodes (and return first available response)
    pub read_fan_out_factor: Option<u32>,
    /// If true - point's payload will not be stored in memory.
    /// It will be read from the disk every time it is requested.
    /// This setting saves RAM by (slightly) increasing the response time.
    /// Note: those payload values that are involved in filtering and are indexed - remain in RAM.
    #[serde(default)]
    pub on_disk_payload: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
pub struct OptimizersConfigDiff {
    /// The minimal fraction of deleted vectors in a segment, required to perform segment optimization
    #[validate(range(min = 0.0, max = 1.0))]
    pub deleted_threshold: Option<f64>,
    /// The minimal number of vectors in a segment, required to perform segment optimization
    #[validate(range(min = 100))]
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
    /// Do not create segments larger this size (in kilobytes).
    /// Large segments might require disproportionately long indexation times,
    /// therefore it makes sense to limit the size of segments.
    ///
    /// If indexation speed have more priority for your - make this parameter lower.
    /// If search speed is more important - make this parameter higher.
    /// Note: 1Kb = 1 vector of size 256
    #[serde(alias = "max_segment_size_kb")]
    #[validate(range(min = 1))]
    pub max_segment_size: Option<usize>,
    /// Maximum size (in kilobytes) of vectors to store in-memory per segment.
    /// Segments larger than this threshold will be stored as read-only memmapped file.
    ///
    /// Memmap storage is disabled by default, to enable it, set this threshold to a reasonable value.
    ///
    /// To disable memmap storage, set this to `0`.
    ///
    /// Note: 1Kb = 1 vector of size 256
    ///
    /// Deprecated since Qdrant 1.15.0
    #[serde(alias = "memmap_threshold_kb")]
    #[deprecated(since = "1.15.0", note = "Use `on_disk` flags instead")]
    pub memmap_threshold: Option<usize>,
    /// Maximum size (in kilobytes) of vectors allowed for plain index, exceeding this threshold will enable vector indexing
    ///
    /// Default value is 20,000, based on <https://github.com/google-research/google-research/blob/master/scann/docs/algorithms.md>.
    ///
    /// To disable vector indexing, set to `0`.
    ///
    /// Note: 1kB = 1 vector of size 256.
    #[serde(alias = "indexing_threshold_kb")]
    pub indexing_threshold: Option<usize>,
    /// Minimum interval between forced flushes.
    pub flush_interval_sec: Option<u64>,
    /// Max number of threads (jobs) for running optimizations per shard.
    /// Note: each optimization job will also use `max_indexing_threads` threads by itself for index building.
    /// If "auto" - have no limit and choose dynamically to saturate CPU.
    /// If 0 - no optimization threads, optimizations will be disabled.
    pub max_optimization_threads: Option<MaxOptimizationThreads>,
}

impl std::hash::Hash for OptimizersConfigDiff {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let Self {
            deleted_threshold,
            vacuum_min_vector_number,
            default_segment_number,
            max_segment_size,
            #[expect(deprecated)]
            memmap_threshold,
            indexing_threshold,
            flush_interval_sec,
            max_optimization_threads,
        } = self;

        deleted_threshold.map(f64::to_le_bytes).hash(state);
        vacuum_min_vector_number.hash(state);
        default_segment_number.hash(state);
        max_segment_size.hash(state);
        memmap_threshold.hash(state);
        indexing_threshold.hash(state);
        flush_interval_sec.hash(state);
        max_optimization_threads.hash(state);
    }
}

impl PartialEq for OptimizersConfigDiff {
    fn eq(&self, other: &Self) -> bool {
        #[expect(deprecated)]
        let eq_memmap_threshold = self.memmap_threshold == other.memmap_threshold;
        self.deleted_threshold.map(f64::to_le_bytes)
            == other.deleted_threshold.map(f64::to_le_bytes)
            && self.vacuum_min_vector_number == other.vacuum_min_vector_number
            && self.default_segment_number == other.default_segment_number
            && self.max_segment_size == other.max_segment_size
            && eq_memmap_threshold
            && self.indexing_threshold == other.indexing_threshold
            && self.flush_interval_sec == other.flush_interval_sec
            && self.max_optimization_threads == other.max_optimization_threads
    }
}

impl Eq for OptimizersConfigDiff {}

impl DiffConfig<HnswConfigDiff> for HnswConfig {
    fn update(&self, diff: &HnswConfigDiff) -> Self {
        let HnswConfigDiff {
            m,
            ef_construct,
            full_scan_threshold,
            max_indexing_threads,
            on_disk,
            payload_m,
            inline_storage,
        } = diff;

        HnswConfig {
            m: m.unwrap_or(self.m),
            ef_construct: ef_construct.unwrap_or(self.ef_construct),
            full_scan_threshold: full_scan_threshold.unwrap_or(self.full_scan_threshold),
            max_indexing_threads: max_indexing_threads.unwrap_or(self.max_indexing_threads),
            on_disk: on_disk.or(self.on_disk),
            payload_m: payload_m.or(self.payload_m),
            inline_storage: inline_storage.or(self.inline_storage),
        }
    }
}

impl DiffConfig<HnswConfigDiff> for HnswConfigDiff {
    fn update(&self, diff: &HnswConfigDiff) -> Self {
        let HnswConfigDiff {
            m,
            ef_construct,
            full_scan_threshold,
            max_indexing_threads,
            on_disk,
            payload_m,
            inline_storage,
        } = diff;

        HnswConfigDiff {
            m: m.or(self.m),
            ef_construct: ef_construct.or(self.ef_construct),
            full_scan_threshold: full_scan_threshold.or(self.full_scan_threshold),
            max_indexing_threads: max_indexing_threads.or(self.max_indexing_threads),
            on_disk: on_disk.or(self.on_disk),
            payload_m: payload_m.or(self.payload_m),
            inline_storage: inline_storage.or(self.inline_storage),
        }
    }
}

impl DiffConfig<OptimizersConfigDiff> for OptimizersConfig {
    fn update(&self, diff: &OptimizersConfigDiff) -> Self {
        let OptimizersConfigDiff {
            deleted_threshold,
            vacuum_min_vector_number,
            default_segment_number,
            max_segment_size,
            memmap_threshold,
            indexing_threshold,
            flush_interval_sec,
            max_optimization_threads,
        } = diff;

        OptimizersConfig {
            deleted_threshold: deleted_threshold.unwrap_or(self.deleted_threshold),
            vacuum_min_vector_number: vacuum_min_vector_number
                .unwrap_or(self.vacuum_min_vector_number),
            default_segment_number: default_segment_number.unwrap_or(self.default_segment_number),
            max_segment_size: max_segment_size.or(self.max_segment_size),
            memmap_threshold: memmap_threshold.or(self.memmap_threshold),
            indexing_threshold: indexing_threshold.or(self.indexing_threshold),
            flush_interval_sec: flush_interval_sec.unwrap_or(self.flush_interval_sec),
            max_optimization_threads: max_optimization_threads
                .map_or(self.max_optimization_threads, From::from),
        }
    }
}

impl DiffConfig<WalConfigDiff> for WalConfig {
    fn update(&self, diff: &WalConfigDiff) -> Self {
        let WalConfigDiff {
            wal_capacity_mb,
            wal_segments_ahead,
            wal_retain_closed,
        } = diff;

        WalConfig {
            wal_capacity_mb: wal_capacity_mb.unwrap_or(self.wal_capacity_mb),
            wal_segments_ahead: wal_segments_ahead.unwrap_or(self.wal_segments_ahead),
            wal_retain_closed: wal_retain_closed.unwrap_or(self.wal_retain_closed),
        }
    }
}

impl DiffConfig<CollectionParamsDiff> for CollectionParams {
    fn update(&self, diff: &CollectionParamsDiff) -> Self {
        let CollectionParamsDiff {
            replication_factor,
            write_consistency_factor,
            read_fan_out_factor,
            on_disk_payload,
        } = diff;

        CollectionParams {
            replication_factor: replication_factor.unwrap_or(self.replication_factor),
            write_consistency_factor: write_consistency_factor
                .unwrap_or(self.write_consistency_factor),
            read_fan_out_factor: read_fan_out_factor.or(self.read_fan_out_factor),
            on_disk_payload: on_disk_payload.unwrap_or(self.on_disk_payload),
            shard_number: self.shard_number,
            sharding_method: self.sharding_method,
            sparse_vectors: self.sparse_vectors.clone(),
            vectors: self.vectors.clone(),
        }
    }
}

impl DiffConfig<StrictModeConfig> for StrictModeConfig {
    fn update(&self, diff: &StrictModeConfig) -> Self {
        let StrictModeConfig {
            enabled,
            max_query_limit,
            max_timeout,
            unindexed_filtering_retrieve,
            unindexed_filtering_update,
            search_max_hnsw_ef,
            search_allow_exact,
            search_max_oversampling,
            upsert_max_batchsize,
            max_collection_vector_size_bytes,
            read_rate_limit,
            write_rate_limit,
            max_collection_payload_size_bytes,
            max_points_count,
            filter_max_conditions,
            condition_max_size,
            multivector_config,
            sparse_config,
            max_payload_index_count,
        } = diff;

        StrictModeConfig {
            enabled: enabled.or(self.enabled),
            max_query_limit: max_query_limit.or(self.max_query_limit),
            max_timeout: max_timeout.or(self.max_timeout),
            unindexed_filtering_retrieve: unindexed_filtering_retrieve
                .or(self.unindexed_filtering_retrieve),
            unindexed_filtering_update: unindexed_filtering_update
                .or(self.unindexed_filtering_update),
            search_max_hnsw_ef: search_max_hnsw_ef.or(self.search_max_hnsw_ef),
            search_allow_exact: search_allow_exact.or(self.search_allow_exact),
            search_max_oversampling: search_max_oversampling.or(self.search_max_oversampling),
            upsert_max_batchsize: upsert_max_batchsize.or(self.upsert_max_batchsize),
            max_collection_vector_size_bytes: max_collection_vector_size_bytes
                .or(self.max_collection_vector_size_bytes),
            read_rate_limit: read_rate_limit.or(self.read_rate_limit),
            write_rate_limit: write_rate_limit.or(self.write_rate_limit),
            max_collection_payload_size_bytes: max_collection_payload_size_bytes
                .or(self.max_collection_payload_size_bytes),
            max_points_count: max_points_count.or(self.max_points_count),
            filter_max_conditions: filter_max_conditions.or(self.filter_max_conditions),
            condition_max_size: condition_max_size.or(self.condition_max_size),
            multivector_config: multivector_config
                .as_ref()
                .or(self.multivector_config.as_ref())
                .cloned(),
            sparse_config: sparse_config
                .as_ref()
                .or(self.sparse_config.as_ref())
                .cloned(),
            max_payload_index_count: max_payload_index_count.or(self.max_payload_index_count),
        }
    }
}

impl From<HnswConfig> for HnswConfigDiff {
    fn from(config: HnswConfig) -> Self {
        let HnswConfig {
            m,
            ef_construct,
            full_scan_threshold,
            max_indexing_threads,
            on_disk,
            payload_m,
            inline_storage,
        } = config;

        HnswConfigDiff {
            m: Some(m),
            ef_construct: Some(ef_construct),
            full_scan_threshold: Some(full_scan_threshold),
            max_indexing_threads: Some(max_indexing_threads),
            on_disk,
            payload_m,
            inline_storage,
        }
    }
}

impl From<WalConfig> for WalConfigDiff {
    fn from(config: WalConfig) -> Self {
        let WalConfig {
            wal_capacity_mb,
            wal_segments_ahead,
            wal_retain_closed,
        } = config;

        WalConfigDiff {
            wal_capacity_mb: Some(wal_capacity_mb),
            wal_segments_ahead: Some(wal_segments_ahead),
            wal_retain_closed: Some(wal_retain_closed),
        }
    }
}

impl From<CollectionParams> for CollectionParamsDiff {
    fn from(config: CollectionParams) -> Self {
        let CollectionParams {
            replication_factor,
            write_consistency_factor,
            read_fan_out_factor,
            on_disk_payload,
            shard_number: _,
            sharding_method: _,
            sparse_vectors: _,
            vectors: _,
        } = config;

        CollectionParamsDiff {
            replication_factor: Some(replication_factor),
            write_consistency_factor: Some(write_consistency_factor),
            read_fan_out_factor,
            on_disk_payload: Some(on_disk_payload),
        }
    }
}

impl From<OptimizersConfig> for OptimizersConfigDiff {
    fn from(config: OptimizersConfig) -> Self {
        let OptimizersConfig {
            deleted_threshold,
            vacuum_min_vector_number,
            default_segment_number,
            max_segment_size,
            #[expect(deprecated)]
            memmap_threshold,
            indexing_threshold,
            flush_interval_sec,
            max_optimization_threads,
        } = config;

        Self {
            deleted_threshold: Some(deleted_threshold),
            vacuum_min_vector_number: Some(vacuum_min_vector_number),
            default_segment_number: Some(default_segment_number),
            max_segment_size,
            #[expect(deprecated)]
            memmap_threshold,
            indexing_threshold,
            flush_interval_sec: Some(flush_interval_sec),
            max_optimization_threads: max_optimization_threads.map(MaxOptimizationThreads::Threads),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
pub enum Disabled {
    Disabled,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum QuantizationConfigDiff {
    Scalar(ScalarQuantization),
    Product(ProductQuantization),
    Binary(BinaryQuantization),
    Disabled(Disabled),
}

impl QuantizationConfigDiff {
    pub fn new_disabled() -> Self {
        QuantizationConfigDiff::Disabled(Disabled::Disabled)
    }
}

impl Validate for QuantizationConfigDiff {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            QuantizationConfigDiff::Scalar(scalar) => scalar.validate(),
            QuantizationConfigDiff::Product(product) => product.validate(),
            QuantizationConfigDiff::Binary(binary) => binary.validate(),
            QuantizationConfigDiff::Disabled(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use segment::types::{Distance, HnswConfig};

    use super::*;
    use crate::operations::vector_params_builder::VectorParamsBuilder;
    use crate::optimizers_builder::OptimizersConfig;

    #[test]
    fn test_update_collection_params() {
        let params = CollectionParams {
            vectors: VectorParamsBuilder::new(128, Distance::Cosine)
                .build()
                .into(),
            ..CollectionParams::empty()
        };

        let diff = CollectionParamsDiff {
            replication_factor: None,
            write_consistency_factor: Some(NonZeroU32::new(2).unwrap()),
            read_fan_out_factor: None,
            on_disk_payload: None,
        };

        let new_params = params.update(&diff);

        assert_eq!(new_params.replication_factor.get(), 1);
        assert_eq!(new_params.write_consistency_factor.get(), 2);
        assert!(new_params.on_disk_payload);
    }

    #[test]
    fn test_hnsw_update() {
        let base_config = HnswConfig::default();
        let update: HnswConfigDiff = serde_json::from_str(r#"{ "m": 32 }"#).unwrap();
        let new_config = base_config.update(&update);
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
            indexing_threshold: Some(50_000),
            flush_interval_sec: 30,
            max_optimization_threads: Some(1),
        };
        let update: OptimizersConfigDiff =
            serde_json::from_str(r#"{ "indexing_threshold": 10000 }"#).unwrap();
        let new_config = base_config.update(&update);
        assert_eq!(new_config.indexing_threshold, Some(10000))
    }

    #[rstest]
    #[case::number(r#"{ "max_optimization_threads": 5 }"#, Some(5))]
    #[case::auto(r#"{ "max_optimization_threads": "auto" }"#, None)]
    #[case::null(r#"{ "max_optimization_threads": null }"#, Some(1))] // no effect
    #[case::nothing("{  }", Some(1))] // no effect
    #[should_panic]
    #[case::other(r#"{ "max_optimization_threads": "other" }"#, Some(1))]
    fn test_set_optimizer_threads(#[case] json_diff: &str, #[case] expected: Option<usize>) {
        let base_config = OptimizersConfig {
            deleted_threshold: 0.9,
            vacuum_min_vector_number: 1000,
            default_segment_number: 10,
            max_segment_size: None,
            memmap_threshold: None,
            indexing_threshold: Some(50_000),
            flush_interval_sec: 30,
            max_optimization_threads: Some(1),
        };

        let update: OptimizersConfigDiff = serde_json::from_str(json_diff).unwrap();
        let new_config = base_config.update(&update);

        assert_eq!(new_config.max_optimization_threads, expected);
    }

    #[test]
    fn test_wal_config() {
        let base_config = WalConfig::default();
        let update: WalConfigDiff = serde_json::from_str(r#"{ "wal_segments_ahead": 2 }"#).unwrap();
        let new_config = base_config.update(&update);
        assert_eq!(new_config.wal_segments_ahead, 2)
    }
}
