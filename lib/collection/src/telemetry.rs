use schemars::JsonSchema;
use segment::telemetry::{
    telemetry_hash, Anonymize, SegmentTelemetry, TelemetryOperationStatistics,
};
use serde::{Deserialize, Serialize};

use crate::config::CollectionConfig;
use crate::shard::ShardId;

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct RemoteShardTelemetry {
    pub shard_id: ShardId,
    pub searches: TelemetryOperationStatistics,
    pub updates: TelemetryOperationStatistics,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct LocalShardTelemetry {
    pub segments: Vec<SegmentTelemetry>,
    pub optimizers: Vec<OptimizerTelemetry>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(untagged)]
pub enum ShardTelemetry {
    Remote(RemoteShardTelemetry),
    Local(LocalShardTelemetry),
    Proxy,
    ForwardProxy,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct CollectionTelemetry {
    pub id: String,
    pub config: CollectionConfig,
    pub init_time: std::time::Duration,
    pub shards: Vec<ShardTelemetry>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(untagged)]
pub enum TelemetryOptimizerType {
    Indexer,
    Merger,
    Vacuum,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct OptimizerTelemetry {
    pub stats: TelemetryOperationStatistics,
    pub optimizer_type: TelemetryOptimizerType,
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
            config: self.config.anonymize(),
            init_time: self.init_time,
            shards: self.shards.iter().map(|shard| shard.anonymize()).collect(),
        }
    }
}

impl Anonymize for OptimizerTelemetry {
    fn anonymize(&self) -> Self {
        Self {
            stats: self.stats.anonymize(),
            optimizer_type: self.optimizer_type.clone(),
        }
    }
}

impl Anonymize for ShardTelemetry {
    fn anonymize(&self) -> Self {
        match self {
            ShardTelemetry::Local(LocalShardTelemetry {
                segments,
                optimizers,
            }) => ShardTelemetry::Local(LocalShardTelemetry {
                segments: segments.iter().map(|segment| segment.anonymize()).collect(),
                optimizers: optimizers
                    .iter()
                    .map(|optimizer| optimizer.anonymize())
                    .collect(),
            }),
            ShardTelemetry::Remote(RemoteShardTelemetry {
                searches,
                updates,
                shard_id,
            }) => ShardTelemetry::Remote(RemoteShardTelemetry {
                shard_id: *shard_id,
                searches: searches.anonymize(),
                updates: updates.anonymize(),
            }),
            ShardTelemetry::Proxy => ShardTelemetry::Proxy,
            ShardTelemetry::ForwardProxy => ShardTelemetry::ForwardProxy,
        }
    }
}

impl Anonymize for CollectionConfig {
    fn anonymize(&self) -> Self {
        CollectionConfig {
            params: self.params.clone(),
            hnsw_config: self.hnsw_config,
            optimizer_config: self.optimizer_config.clone(),
            wal_config: self.wal_config.clone(),
        }
    }
}
