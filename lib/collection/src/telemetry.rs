use schemars::JsonSchema;
use segment::telemetry::{
    telemetry_hash, Anonymize, SegmentTelemetry, TelemetryOperationStatistics,
};
use serde::{Deserialize, Serialize};

use crate::config::CollectionConfig;
use crate::shards::shard::ShardId;

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub enum ShardTelemetry {
    Remote(RemoteShardTelemetry),
    Local(LocalShardTelemetry),
    Proxy,
    ForwardProxy,
    ReplicaSet {
        local: Option<Box<ShardTelemetry>>,
        remote: Vec<ShardTelemetry>,
    },
}

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
pub struct CollectionTelemetry {
    pub id: String,
    pub config: CollectionConfig,
    pub init_time: std::time::Duration,
    pub shards: Vec<ShardTelemetry>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub enum OptimizerTelemetry {
    Indexing {
        optimizations: TelemetryOperationStatistics,
    },
    Merge {
        optimizations: TelemetryOperationStatistics,
    },
    Vacuum {
        optimizations: TelemetryOperationStatistics,
    },
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
        match self {
            OptimizerTelemetry::Indexing { optimizations } => OptimizerTelemetry::Indexing {
                optimizations: optimizations.anonymize(),
            },
            OptimizerTelemetry::Merge { optimizations } => OptimizerTelemetry::Merge {
                optimizations: optimizations.anonymize(),
            },
            OptimizerTelemetry::Vacuum { optimizations } => OptimizerTelemetry::Vacuum {
                optimizations: optimizations.anonymize(),
            },
        }
    }
}

impl Anonymize for ShardTelemetry {
    fn anonymize(&self) -> Self {
        match self {
            ShardTelemetry::Local(local) => ShardTelemetry::Local(local.anonymize()),
            ShardTelemetry::Remote(remote) => ShardTelemetry::Remote(remote.anonymize()),
            ShardTelemetry::Proxy => ShardTelemetry::Proxy,
            ShardTelemetry::ForwardProxy => ShardTelemetry::ForwardProxy,
            ShardTelemetry::ReplicaSet { local, remote } => ShardTelemetry::ReplicaSet {
                local: local.as_ref().map(|local| Box::new(local.anonymize())),
                remote: remote.iter().map(|remote| remote.anonymize()).collect(),
            },
        }
    }
}

impl Anonymize for LocalShardTelemetry {
    fn anonymize(&self) -> Self {
        LocalShardTelemetry {
            segments: self
                .segments
                .iter()
                .map(|segment| segment.anonymize())
                .collect(),
            optimizers: self
                .optimizers
                .iter()
                .map(|optimizer| optimizer.anonymize())
                .collect(),
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
