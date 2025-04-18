use std::collections::HashMap;

use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::types::{HnswConfig, QuantizationConfig, StrictModeConfigOutput};
use serde::Serialize;
use uuid::Uuid;

use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use crate::operations::types::{OptimizersStatus, ReshardingInfo, ShardTransferInfo};
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::shard::ShardId;
use crate::shards::telemetry::ReplicaSetTelemetry;

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct CollectionTelemetry {
    pub id: String,

    #[anonymize(false)]
    pub init_time_ms: u64,

    pub config: CollectionConfigTelemetry,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub shards: Option<Vec<ReplicaSetTelemetry>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfers: Option<Vec<ShardTransferInfo>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub resharding: Option<Vec<ReshardingInfo>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub shard_clean_tasks: Option<HashMap<ShardId, ShardCleanStatusTelemetry>>,
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct CollectionsAggregatedTelemetry {
    pub vectors: usize,
    pub optimizers_status: OptimizersStatus,
    pub params: CollectionParams,
}

impl CollectionTelemetry {
    pub fn count_vectors(&self) -> usize {
        self.shards
            .iter()
            .flatten()
            .filter_map(|shard| shard.local.as_ref())
            .map(|x| x.num_vectors.unwrap_or(0))
            .sum()
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

#[derive(Debug, Serialize, JsonSchema, Anonymize, Clone, PartialEq)]
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
    #[anonymize(value = None)]
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
