use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::telemetry::RocksDBMemoryUsageStats;
use serde::{Deserialize, Serialize};

use crate::config::CollectionConfig;
use crate::operations::types::ShardTransferInfo;
use crate::shards::telemetry::ReplicaSetTelemetry;

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
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
            .flat_map(|shard| shard.local.as_ref())
            .flat_map(|x| x.segments.iter())
            .map(|s| s.info.num_vectors)
            .sum()
    }

    pub fn get_rocksdb_memory_usage_stats(&self) -> RocksDBMemoryUsageStats {
        let rocksdb_memory_usage_stats = self
            .shards
            .iter()
            .flat_map(|shard| shard.local.as_ref())
            .flat_map(|x| x.segments.iter())
            .map(|s| s.rocksdb_memory_usage_stats.clone())
            .collect::<Vec<RocksDBMemoryUsageStats>>();

        let mut acc = RocksDBMemoryUsageStats {
            mem_table_total: 0,
            mem_table_unflushed: 0,
            mem_table_readers_total: 0,
            cache_total: 0,
        };

        for s in rocksdb_memory_usage_stats {
            acc.mem_table_total += s.mem_table_total;
            acc.mem_table_unflushed += s.mem_table_unflushed;
            acc.mem_table_readers_total += s.mem_table_readers_total;
            acc.cache_total += s.cache_total;
        }

        acc
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
        }
    }
}
