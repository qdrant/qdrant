use std::collections::HashMap;

use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::types::{HnswConfig, QuantizationConfig, StrictModeConfigOutput};
use serde::Serialize;
use uuid::Uuid;

use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use crate::operations::types::{ReshardingInfo, ShardTransferInfo};
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::shard::ShardId;
use crate::shards::telemetry::ReplicaSetTelemetry;

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct CollectionTelemetry {
    pub id: String,
    pub init_time_ms: u64,
    pub config: CollectionConfigTelemetry,
    pub shards: Vec<ReplicaSetTelemetry>,
    pub transfers: Vec<ShardTransferInfo>,
    pub resharding: Vec<ReshardingInfo>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub shard_clean_tasks: HashMap<ShardId, ShardCleanStatusTelemetry>,
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
            resharding: vec![],
            shard_clean_tasks: HashMap::new(),
        }
    }
}

impl Anonymize for CollectionConfigTelemetry {
    fn anonymize(&self) -> Self {
        CollectionConfigTelemetry {
            params: self.params.anonymize(),
            hnsw_config: self.hnsw_config.clone(),
            optimizer_config: self.optimizer_config.clone(),
            wal_config: self.wal_config.clone(),
            quantization_config: self.quantization_config.clone(),
            strict_mode_config: self.strict_mode_config.anonymize(),
            uuid: None,
        }
    }
}

#[derive(Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ShardCleanStatusTelemetry {
    Started,
    Progress(ShardCleanStatusProgressTelemetry),
    Done,
    Failed(ShardCleanStatusFailedTelemetry),
    Cancelled,
}

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct ShardCleanStatusProgressTelemetry {
    pub deleted_points: usize,
}

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct ShardCleanStatusFailedTelemetry {
    pub reason: String,
}

#[derive(Debug, Serialize, JsonSchema, Clone, PartialEq)]
pub struct CollectionConfigTelemetry {
    pub params: CollectionParams,
    pub hnsw_config: HnswConfig,
    pub optimizer_config: OptimizersConfig,
    pub wal_config: WalConfig,
    #[serde(default)]
    pub quantization_config: Option<QuantizationConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strict_mode_config: Option<StrictModeConfigOutput>,
    #[serde(default)]
    pub uuid: Option<Uuid>,
}

impl From<CollectionConfigInternal> for CollectionConfigTelemetry {
    fn from(config: CollectionConfigInternal) -> Self {
        let CollectionConfigInternal {
            params,
            hnsw_config,
            optimizer_config,
            wal_config,
            quantization_config,
            strict_mode_config,
            uuid,
        } = config;
        CollectionConfigTelemetry {
            params,
            hnsw_config,
            optimizer_config,
            wal_config,
            quantization_config,
            strict_mode_config: strict_mode_config.map(StrictModeConfigOutput::from),
            uuid,
        }
    }
}
