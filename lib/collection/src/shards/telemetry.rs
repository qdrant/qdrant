use std::collections::HashMap;

use api::rest;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::common::operation_time_statistics::OperationDurationStatistics;
use serde::Serialize;

use crate::collection_manager::optimizers::TrackerTelemetry;
use crate::operations::types::OptimizersStatus;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct ReplicaSetTelemetry {
    pub id: ShardId,
    pub local: Option<LocalShardTelemetry>,
    pub remote: Vec<RemoteShardTelemetry>,
    pub replicate_states: HashMap<PeerId, ReplicaState>,
}

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct RemoteShardTelemetry {
    pub shard_id: ShardId,
    pub peer_id: Option<PeerId>,
    pub searches: OperationDurationStatistics,
    pub updates: OperationDurationStatistics,
}

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct LocalShardTelemetry {
    pub variant_name: Option<String>,
    pub segments: Vec<rest::schema::SegmentTelemetry>,
    pub optimizations: OptimizerTelemetry,
}

#[derive(Serialize, Clone, Debug, JsonSchema, Default)]
pub struct OptimizerTelemetry {
    pub status: OptimizersStatus,
    pub optimizations: OperationDurationStatistics,
    pub log: Vec<TrackerTelemetry>,
}

impl Anonymize for OptimizerTelemetry {
    fn anonymize(&self) -> Self {
        Self {
            status: self.status.clone(),
            optimizations: self.optimizations.anonymize(),
            log: self.log.anonymize(),
        }
    }
}

impl Anonymize for LocalShardTelemetry {
    fn anonymize(&self) -> Self {
        LocalShardTelemetry {
            variant_name: self.variant_name.clone(),
            segments: self.segments.anonymize(),
            optimizations: self.optimizations.anonymize(),
        }
    }
}

impl Anonymize for TrackerTelemetry {
    fn anonymize(&self) -> Self {
        TrackerTelemetry {
            name: self.name.clone(),
            segment_ids: self.segment_ids.anonymize(),
            status: self.status.clone(),
            start_at: self.start_at.anonymize(),
            end_at: self.end_at.anonymize(),
        }
    }
}

impl Anonymize for RemoteShardTelemetry {
    fn anonymize(&self) -> Self {
        RemoteShardTelemetry {
            shard_id: self.shard_id,
            peer_id: None,
            searches: self.searches.anonymize(),
            updates: self.updates.anonymize(),
        }
    }
}

impl Anonymize for ReplicaSetTelemetry {
    fn anonymize(&self) -> Self {
        ReplicaSetTelemetry {
            id: self.id,
            local: self.local.anonymize(),
            remote: self.remote.anonymize(),
            replicate_states: Default::default(),
        }
    }
}
