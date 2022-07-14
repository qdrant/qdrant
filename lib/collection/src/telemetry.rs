use crate::config::CollectionConfig;
use crate::shard::ShardId;
use segment::telemetry::{
    telemetry_hash, Anonymize, SegmentTelemetry, TelemetryOperationStatistics,
};
use serde::Serialize;

#[derive(Serialize, Clone)]
pub enum ShardTelemetry {
    Remote {
        shard_id: ShardId,
        searches: TelemetryOperationStatistics,
        updates: TelemetryOperationStatistics,
    },
    Local {
        segments: Vec<SegmentTelemetry>,
    },
    Proxy {},
}

#[derive(Serialize, Clone)]
pub struct CollectionTelemetry {
    pub id: String,
    pub config: CollectionConfig,
    pub init_time: std::time::Duration,
    pub shards: Vec<ShardTelemetry>,
}

impl CollectionTelemetry {
    pub fn new(id: String, config: CollectionConfig, init_time: std::time::Duration) -> Self {
        Self {
            id,
            config,
            init_time,
            shards: Vec::new(),
        }
    }
}

impl Anonymize for CollectionTelemetry {
    fn anonymize(&self) -> Self {
        Self {
            id: telemetry_hash(&self.id),
            config: self.config.clone(),
            init_time: self.init_time,
            shards: self.shards.iter().map(|shard| shard.anonymize()).collect(),
        }
    }
}

impl Anonymize for ShardTelemetry {
    fn anonymize(&self) -> Self {
        match self {
            ShardTelemetry::Local { segments } => ShardTelemetry::Local {
                segments: segments.iter().map(|segment| segment.anonymize()).collect(),
            },
            ShardTelemetry::Remote {
                searches,
                updates,
                shard_id,
            } => ShardTelemetry::Remote {
                shard_id: *shard_id,
                searches: searches.anonymize(),
                updates: updates.anonymize(),
            },
            ShardTelemetry::Proxy {} => ShardTelemetry::Proxy {},
        }
    }
}
