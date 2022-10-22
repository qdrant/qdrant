use std::cmp::max;

use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::common::operation_time_statistics::OperationDurationStatistics;
use segment::telemetry::SegmentTelemetry;
use serde::{Deserialize, Serialize};

use crate::operations::types::OptimizersStatus;
use crate::shards::shard::ShardId;

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct ReplicaSetTelemetry {
    pub id: ShardId,
    pub local: Option<LocalShardTelemetry>,
    pub remote: Vec<RemoteShardTelemetry>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct RemoteShardTelemetry {
    pub shard_id: ShardId,
    pub searches: OperationDurationStatistics,
    pub updates: OperationDurationStatistics,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct LocalShardTelemetry {
    pub variant_name: Option<String>,
    pub segments: Vec<SegmentTelemetry>,
    pub optimizations: OptimizerTelemetry,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct OptimizerTelemetry {
    pub status: OptimizersStatus,
    pub optimizations: OperationDurationStatistics,
}

impl std::ops::Add for OptimizerTelemetry {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            status: max(self.status, other.status),
            optimizations: self.optimizations + other.optimizations,
        }
    }
}

impl Anonymize for OptimizerTelemetry {
    fn anonymize(&self) -> Self {
        Self {
            status: self.status.clone(),
            optimizations: self.optimizations.anonymize(),
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

impl Anonymize for RemoteShardTelemetry {
    fn anonymize(&self) -> Self {
        RemoteShardTelemetry {
            shard_id: self.shard_id,
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
        }
    }
}
