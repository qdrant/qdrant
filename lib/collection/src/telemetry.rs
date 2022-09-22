use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::common::operation_time_statistics::TelemetryOperationStatistics;
use segment::telemetry::{CardinalitySearchesTelemetry, SegmentTelemetry};
use serde::{Deserialize, Serialize};

use crate::config::CollectionConfig;
use crate::shard::ShardId;

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub enum ShardTelemetry {
    Remote {
        shard_id: ShardId,
        searches: TelemetryOperationStatistics,
        updates: TelemetryOperationStatistics,
    },
    Local {
        segments: Vec<SegmentTelemetry>,
        optimizers: Vec<OptimizerTelemetry>,
    },
    Proxy {},
    ForwardProxy {},
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

    pub fn get_cardinality_searches(&self) -> CardinalitySearchesTelemetry {
        let mut result = CardinalitySearchesTelemetry::default();
        for shard in &self.shards {
            if let ShardTelemetry::Local { segments, .. } = shard {
                for segment in segments {
                    result = result + segment.cardinality_searches.clone().unwrap();
                }
            }
        }
        result
    }

    pub fn remove_cardinality_searches(&mut self) {
        for shard in &mut self.shards {
            if let ShardTelemetry::Local { segments, .. } = shard {
                for segment in segments {
                    segment.cardinality_searches = None;
                }
            }
        }
    }
}

impl Anonymize for CollectionTelemetry {
    fn anonymize(&self) -> Self {
        Self {
            id: self.id.anonymize(),
            config: self.config.anonymize(),
            init_time: self.init_time,
            shards: self.shards.anonymize(),
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
            ShardTelemetry::Local {
                segments,
                optimizers,
            } => ShardTelemetry::Local {
                segments: segments.anonymize(),
                optimizers: optimizers.anonymize(),
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
            ShardTelemetry::ForwardProxy {} => ShardTelemetry::ForwardProxy {},
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
