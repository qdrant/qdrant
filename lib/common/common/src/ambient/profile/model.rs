//! Query profile output schema, version [`PROFILE_SCHEMA_VERSION`].
//!
//! Plain data, shared by REST (serde/schemars) and gRPC (converted in the `api` crate).
//! Changes must be additive within a version: new optional fields only. Removing or
//! renaming a field, or changing its meaning, bumps the version.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Version of the profile schema, reported in every [`QueryProfile`].
pub const PROFILE_SCHEMA_VERSION: u32 = 1;

/// What a profiled request does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProfileMode {
    /// Run the request and report where the time went, like `EXPLAIN ANALYZE`.
    Analyze,
    /// Plan the request without scoring any vector, like `EXPLAIN`.
    /// Returns an empty result.
    Explain,
}

/// Structured breakdown of one request, see the "Query profiling" docs page.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueryProfile {
    /// Schema version, see [`PROFILE_SCHEMA_VERSION`].
    pub version: u32,
    pub mode: ProfileMode,
    /// Wall time on the node that received the request, from the start of profiling.
    pub total_time_ms: f64,
    /// Request-level phases on the receiving node, in the order they started.
    pub phases: Vec<PhaseProfile>,
    /// One entry per shard operation, local or remote.
    pub shards: Vec<ShardProfile>,
    /// Human-readable findings derived from the data above, e.g. a missing payload index.
    pub notes: Vec<String>,
}

/// A timed step that is not a segment search.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PhaseProfile {
    /// e.g. `plan`, `merge`, `rescore`, `fusion`, `payload_retrieval`, `prefetch`.
    pub name: String,
    /// Summed over all calls; may exceed wall time when calls run in parallel.
    pub time_ms: f64,
    pub calls: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ShardLocation {
    Local,
    Remote,
}

/// One shard operation, e.g. `query_batch` on shard 3.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ShardProfile {
    pub shard_id: u32,
    /// Peer that executed the operation. Absent in single-node deployments.
    pub peer_id: Option<u64>,
    pub location: ShardLocation,
    /// `query_batch`, `core_search`, `scroll`, `count`, `facet`, `retrieve`.
    pub operation: String,
    /// Time of the operation on the peer that executed it.
    pub time_ms: f64,
    /// Remote only: round trip seen by the receiving node minus `time_ms`.
    /// Includes serialization, network and queueing on the remote peer.
    pub network_time_ms: Option<f64>,
    /// Shard-level phases, e.g. `rescore`, `fusion`, `payload_retrieval`.
    pub phases: Vec<PhaseProfile>,
    /// One entry per segment per search group (requests sharing vector name and filter).
    pub segments: Vec<SegmentProfile>,
}

/// How a segment answered a vector search or a filtered read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SearchStrategy {
    /// HNSW graph traversal, no filter.
    Hnsw,
    /// HNSW graph traversal, checking the filter on visited points.
    HnswFiltered,
    /// ACORN graph traversal for selective filters.
    Acorn,
    /// Points matching the filter are read from payload indexes and scored by brute force.
    PayloadIndexFirst,
    /// Every point of the segment is scored (and checked against the filter, if any).
    FullScan,
    /// Brute force without quantization, because `exact: true` was requested.
    Exact,
    /// Sparse vector search with the inverted index.
    SparseIndex,
    /// Filtered read (scroll, count, facet) answered from payload indexes.
    IndexedRead,
    /// Filtered read (scroll, count, facet) that checks every point of the segment.
    FullScanRead,
}

/// Why the strategy was chosen.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StrategyReason {
    /// The segment has no HNSW index for this vector (not optimized yet, or indexing disabled).
    NotIndexed,
    /// HNSW is configured with `m = 0` and `payload_m = 0`.
    HnswDisabled,
    /// `exact: true` in search params.
    ExactRequested,
    /// The segment has fewer vectors than `full_scan_threshold`.
    SmallSegment,
    /// The upper bound of the estimated filter cardinality is below `full_scan_threshold`.
    CardinalityBelowThreshold,
    /// The lower bound of the estimated filter cardinality is above `full_scan_threshold`.
    CardinalityAboveThreshold,
    /// The estimate was inconclusive and sampling points decided.
    SampledCardinality,
    /// ACORN is enabled and the filter is selective enough.
    AcornSelectivity,
    /// No filter and the index is usable.
    Unfiltered,
}

/// Bounds of the estimated number of points matching a filter, in one segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CardinalityProfile {
    pub min: u64,
    pub exp: u64,
    pub max: u64,
}

/// How the filter was planned in one segment.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FilterProfile {
    pub estimated_cardinality: Option<CardinalityProfile>,
    /// Exact number of matching points, when the operation computed it (exact count,
    /// full scans). Absent when it would cost extra work to find out.
    pub actual_cardinality: Option<u64>,
    /// Payload keys whose index was used to select candidate points (primary clauses).
    pub indexed_keys: Vec<String>,
    /// Payload keys in the filter that have no payload index in this segment:
    /// their conditions are checked by reading the payload of every candidate point.
    pub unindexed_keys: Vec<String>,
    /// True when no payload index narrowed the candidates down, so all points of the
    /// segment were candidates.
    pub full_payload_scan: bool,
}

/// Counters for one segment search, measured on the thread that ran it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SegmentCounters {
    /// Distance computations against original (full precision) vectors.
    pub vector_scores: u64,
    /// Distance computations against quantized vectors.
    pub quantized_vector_scores: u64,
    /// Bytes of vectors read (original and quantized).
    pub vector_io_read_bytes: u64,
    /// Bytes of payload read, mostly from filter checks and payload retrieval.
    pub payload_io_read_bytes: u64,
    /// Bytes of payload indexes read.
    pub payload_index_io_read_bytes: u64,
    /// Bytes served from the local cache of remote (object storage) data.
    pub remote_io_cached_read_bytes: u64,
    /// Bytes fetched from remote (object storage).
    pub remote_io_non_cached_read_bytes: u64,
}

/// One search group (or filtered read) in one segment.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SegmentProfile {
    pub segment_id: String,
    /// Appendable segments take new writes and are usually not indexed yet.
    pub appendable: bool,
    /// The vector searched has an HNSW (or sparse) index in this segment.
    /// False during indexing: such segments are searched by brute force.
    pub indexed: bool,
    /// Points visible in the segment.
    pub points: u64,
    /// `search` or `read`.
    pub operation: String,
    pub vector_name: Option<String>,
    /// Number of queries answered together.
    pub batch_size: u64,
    /// Requested number of results from this segment (after sampling).
    pub limit: Option<u64>,
    pub strategy: Option<SearchStrategy>,
    pub strategy_reason: Option<StrategyReason>,
    /// `full_scan_threshold` in points, as used to pick the strategy.
    pub full_scan_threshold: Option<u64>,
    pub filter: Option<FilterProfile>,
    pub time_ms: f64,
    pub counters: SegmentCounters,
}
