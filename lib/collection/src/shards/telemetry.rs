use std::collections::HashMap;

use schemars::JsonSchema;
use segment::common::anonymize::{Anonymize, anonymize_collection_with_u64_hashable_key};
use segment::common::operation_time_statistics::OperationDurationStatistics;
use segment::telemetry::SegmentTelemetry;
use segment::types::ShardKey;
use serde::Serialize;

use crate::collection_manager::optimizers::TrackerTelemetry;
use crate::operations::types::{OptimizersStatus, ShardStatus};
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct ReplicaSetTelemetry {
    #[anonymize(false)]
    pub id: ShardId,
    pub key: Option<ShardKey>,
    pub local: Option<LocalShardTelemetry>,
    pub remote: Vec<RemoteShardTelemetry>,
    #[anonymize(with = anonymize_collection_with_u64_hashable_key)]
    pub replicate_states: HashMap<PeerId, ReplicaState>,
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct RemoteShardTelemetry {
    #[anonymize(false)]
    pub shard_id: ShardId,
    #[anonymize(value = None)]
    pub peer_id: Option<PeerId>,
    pub searches: OperationDurationStatistics,
    pub updates: OperationDurationStatistics,
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct LocalShardTelemetry {
    #[anonymize(false)]
    pub variant_name: Option<String>,
    pub status: Option<ShardStatus>,
    /// Total number of optimized points since the last start.
    pub total_optimized_points: usize,
    /// An ESTIMATION of effective amount of bytes used for vectors
    /// Do NOT rely on this number unless you know what you are doing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vectors_size_bytes: Option<usize>,
    /// An estimation of the effective amount of bytes used for payloads
    /// Do NOT rely on this number unless you know what you are doing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payloads_size_bytes: Option<usize>,
    /// Sum of segment points
    /// This is an approximate number
    /// Do NOT rely on this number unless you know what you are doing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_points: Option<usize>,
    /// Sum of number of vectors in all segments
    /// This is an approximate number
    /// Do NOT rely on this number unless you know what you are doing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_vectors: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segments: Option<Vec<SegmentTelemetry>>,
    pub optimizations: OptimizerTelemetry,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub async_scorer: Option<bool>,
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize, Default)]
pub struct OptimizerTelemetry {
    pub status: OptimizersStatus,
    pub optimizations: OperationDurationStatistics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log: Option<Vec<TrackerTelemetry>>,
}
