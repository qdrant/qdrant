use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::common::operation_time_statistics::OperationDurationStatistics;
use segment::telemetry::{SegmentTelemetry, VectorIndexSearchesTelemetry};
use serde::{Deserialize, Serialize};

use crate::config::{CollectionConfig, CollectionParams};
use crate::operations::types::{CollectionStatus, OptimizersStatus};
use crate::shard::ShardId;

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
    pub searches: OperationDurationStatistics,
    pub updates: OperationDurationStatistics,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct LocalShardTelemetry {
    pub segments: Vec<SegmentTelemetry>,
    pub optimizations: OptimizerTelemetry,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct CollectionTelemetry {
    pub id: String,
    pub init_time_micros: u32,
    pub short_info: CollectionShortInfoTelemetry,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub params: Option<CollectionParams>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub config: Option<CollectionConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub shards: Option<Vec<ShardTelemetry>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub vector_index_searches: Option<VectorIndexSearchesTelemetry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub optimizations: Option<OptimizerTelemetry>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct OptimizerTelemetry {
    pub indexing: OperationDurationStatistics,
    pub merge: OperationDurationStatistics,
    pub vacuum: OperationDurationStatistics,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct CollectionShortInfoTelemetry {
    pub status: CollectionStatus,
    pub optimizer_status: OptimizersStatus,
    pub vectors_count: usize,
    pub indexed_vectors_count: usize,
}

impl Default for CollectionShortInfoTelemetry {
    fn default() -> Self {
        Self {
            status: CollectionStatus::Green,
            optimizer_status: OptimizersStatus::Ok,
            vectors_count: 0,
            indexed_vectors_count: 0,
        }
    }
}

impl CollectionTelemetry {
    pub fn new(id: String, config: CollectionConfig, init_time: std::time::Duration) -> Self {
        Self {
            id,
            short_info: CollectionShortInfoTelemetry::default(),
            params: Some(config.params.clone()),
            config: Some(config),
            init_time_micros: init_time.as_micros() as u32,
            shards: Some(Vec::new()),
            vector_index_searches: None,
            optimizations: None,
        }
    }

    pub fn calculate_optimizations_from_shards(&self) -> OptimizerTelemetry {
        let mut result = OptimizerTelemetry::default();
        if let Some(shards) = &self.shards {
            for shard in shards {
                if let ShardTelemetry::Local(local_shard) = shard {
                    result = result + local_shard.optimizations.clone();
                }
            }
        }
        result
    }

    pub fn calculate_vector_index_searches_from_shards(&self) -> VectorIndexSearchesTelemetry {
        let mut result = VectorIndexSearchesTelemetry::default();
        if let Some(shards) = &self.shards {
            for shard in shards {
                if let ShardTelemetry::Local(local_shard) = shard {
                    for segment in &local_shard.segments {
                        result = result + segment.vector_index_searches.clone().unwrap();
                    }
                }
            }
        }
        result
    }
}

impl std::ops::Add for OptimizerTelemetry {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            indexing: self.indexing + other.indexing,
            merge: self.merge + other.merge,
            vacuum: self.vacuum + other.vacuum,
        }
    }
}

impl std::ops::Add for CollectionShortInfoTelemetry {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            status: std::cmp::max(self.status, other.status),
            optimizer_status: std::cmp::max(self.optimizer_status, other.optimizer_status),
            vectors_count: self.vectors_count + other.vectors_count,
            indexed_vectors_count: self.indexed_vectors_count + other.indexed_vectors_count,
        }
    }
}

impl Anonymize for CollectionTelemetry {
    fn anonymize(&self) -> Self {
        Self {
            id: self.id.anonymize(),
            short_info: self.short_info.anonymize(),
            params: self.params.clone(),
            config: self.config.anonymize(),
            init_time_micros: self.init_time_micros,
            shards: self.shards.anonymize(),
            vector_index_searches: self.vector_index_searches.anonymize(),
            optimizations: self.optimizations.anonymize(),
        }
    }
}

impl Anonymize for CollectionShortInfoTelemetry {
    fn anonymize(&self) -> Self {
        Self {
            status: self.status,
            optimizer_status: self.optimizer_status.clone(),
            vectors_count: self.vectors_count.anonymize(),
            indexed_vectors_count: self.indexed_vectors_count.anonymize(),
        }
    }
}

impl Anonymize for OptimizerTelemetry {
    fn anonymize(&self) -> Self {
        Self {
            indexing: self.indexing.anonymize(),
            merge: self.merge.anonymize(),
            vacuum: self.vacuum.anonymize(),
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
                local: local.anonymize(),
                remote: remote.anonymize(),
            },
        }
    }
}

impl Anonymize for LocalShardTelemetry {
    fn anonymize(&self) -> Self {
        LocalShardTelemetry {
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
