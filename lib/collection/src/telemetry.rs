use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;

use crate::config::CollectionConfig;
use crate::operations::types::ShardTransferInfo;
use crate::shards::telemetry::ReplicaSetTelemetry;

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct CollectionTelemetry {
    pub id: String,
    pub init_time_ms: u64,
    pub config: CollectionConfig,
    pub shards: Vec<ReplicaSetTelemetry>,
    pub transfers: Vec<ShardTransferInfo>,
}

impl CollectionTelemetry {
    pub fn count_vectors(&self) -> usize {
        self.shards
            .iter()
            .filter_map(|shard| shard.local.as_ref())
            .flat_map(|x| x.segments.iter())
            .map(|s| s.info.num_vectors)
            .sum()
    }
}

impl Anonymize for CollectionTelemetry {
    fn anonymize(&self) -> Self {
        Self {
            id: self.id.anonymize(),
            config: self.config.anonymize(),
            init_time_ms: self.init_time_ms,
            shards: self.shards.anonymize(),
            transfers: vec![],
        }
    }
}

impl Anonymize for CollectionConfig {
    fn anonymize(&self) -> Self {
        CollectionConfig {
            params: self.params.clone(),
            hnsw_config: self.hnsw_config.clone(),
            optimizer_config: self.optimizer_config.clone(),
            wal_config: self.wal_config.clone(),
            quantization_config: self.quantization_config.clone(),
            strict_mode_config: self.strict_mode_config.clone(),
        }
    }
}
