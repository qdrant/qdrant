use std::collections::HashMap;

use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::data_types::tiny_map::TinyMap;
use segment::types::{
    HnswConfig, Payload, QuantizationConfig, StrictModeConfigOutput, VectorNameBuf,
};
use serde::Serialize;
use uuid::Uuid;

use crate::collection_manager::optimizers::TrackerStatus;
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

    /// Amount of optimizers currently running.
    ///
    /// Note: A `DetailsLevel` of 4 or setting `telemetry_detail.optimizer_logs` to true is required.
    ///       Otherwise, this function will return 0, which may not be correct.
    pub fn count_optimizers_running(&self) -> usize {
        self.shards
            .iter()
            .flatten()
            .filter_map(|replica_set| replica_set.local.as_ref())
            .flat_map(|local_shard| local_shard.optimizations.log.iter().flatten())
            .filter(|log| log.status == TrackerStatus::Optimizing)
            .count()
    }

    pub fn count_points(&self) -> usize {
        self.shards
            .iter()
            .flatten()
            .filter_map(|shard| shard.local.as_ref())
            .map(|local_shard| local_shard.num_points.unwrap_or(0))
            .sum()
    }

    pub fn count_points_per_vector(&self) -> TinyMap<VectorNameBuf, usize> {
        self.shards
            .iter()
            .flatten()
            .filter_map(|shard| shard.local.as_ref())
            .map(|local_shard| {
                local_shard
                    .num_vectors_by_name
                    .as_ref()
                    .into_iter()
                    .flatten()
            })
            .fold(
                TinyMap::<VectorNameBuf, usize>::new(),
                |mut acc, shard_vectors| {
                    for (name, count) in shard_vectors {
                        *acc.get_or_insert_default(name) += count;
                    }
                    acc
                },
            )
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
    /// Arbitrary JSON metadata for the collection
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[anonymize(value = None)]
    pub metadata: Option<Payload>,
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
            metadata,
        } = config;
        CollectionConfigTelemetry {
            params,
            hnsw_config,
            optimizer_config,
            wal_config,
            quantization_config,
            strict_mode_config: strict_mode_config.map(StrictModeConfigOutput::from),
            uuid,
            metadata,
        }
    }
}
