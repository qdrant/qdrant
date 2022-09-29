use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::common::operation_time_statistics::OperationDurationStatistics;
use segment::telemetry::{SegmentTelemetry, VectorIndexSearchesTelemetry};
use serde::{Deserialize, Serialize};

use crate::config::CollectionConfig;
use crate::shard::ShardId;

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub enum ShardTelemetry {
    Remote {
        shard_id: ShardId,
        searches: OperationDurationStatistics,
        updates: OperationDurationStatistics,
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
    pub init_time_micros: u32,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub shards: Option<Vec<ShardTelemetry>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub vector_index_searches: Option<VectorIndexSearchesTelemetry>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub enum OptimizerTelemetry {
    Indexing {
        optimizations: OperationDurationStatistics,
    },
    Merge {
        optimizations: OperationDurationStatistics,
    },
    Vacuum {
        optimizations: OperationDurationStatistics,
    },
}

impl CollectionTelemetry {
    pub fn new(id: String, config: CollectionConfig, init_time: std::time::Duration) -> Self {
        Self {
            id,
            config,
            init_time_micros: init_time.as_micros() as u32,
            shards: Some(Vec::new()),
            vector_index_searches: None,
        }
    }

    pub fn calculate_vector_index_searches_from_shards(&self) -> VectorIndexSearchesTelemetry {
        let mut result = VectorIndexSearchesTelemetry::default();
        if let Some(shards) = &self.shards {
            for shard in shards {
                if let ShardTelemetry::Local { segments, .. } = shard {
                    for segment in segments {
                        result = result + segment.vector_index_searches.clone().unwrap();
                    }
                }
            }
        }
        result
    }

    pub fn set_vector_index_searches(&mut self, telemetry: VectorIndexSearchesTelemetry) {
        self.vector_index_searches = Some(telemetry);
    }

    pub fn remove_vector_index_searches(&mut self) {
        if let Some(shards) = &mut self.shards {
            for shard in shards {
                if let ShardTelemetry::Local { segments, .. } = shard {
                    for segment in segments {
                        segment.vector_index_searches = None;
                    }
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
            init_time_micros: self.init_time_micros,
            shards: self.shards.anonymize(),
            vector_index_searches: self.vector_index_searches.anonymize(),
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
