use std::collections::HashMap;

use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
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
    #[anonymize(value = HashMap::new())]
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
    pub segments: Vec<SegmentTelemetry>,
    pub optimizations: OptimizerTelemetry,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub async_scorer: Option<bool>,
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize, Default)]
pub struct OptimizerTelemetry {
    pub status: OptimizersStatus,
    pub optimizations: OperationDurationStatistics,
    pub log: Vec<TrackerTelemetry>,
}
