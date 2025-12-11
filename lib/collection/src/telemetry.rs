use std::collections::HashMap;

use itertools::Itertools;
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
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::shard::ShardId;
use crate::shards::telemetry::{LocalShardTelemetry, ReplicaSetTelemetry};

const MICROSECONDS_PER_SECOND: usize = 1_000_000;

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
pub struct CollectionSnapshotTelemetry {
    pub id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub running_snapshots: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub running_snapshot_recovery: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_snapshot_creations: Option<usize>,
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct CollectionsAggregatedTelemetry {
    pub vectors: usize,
    pub optimizers_status: OptimizersStatus,
    pub params: CollectionParams,
}

impl CollectionTelemetry {
    pub fn local_shard_iter(&self) -> impl Iterator<Item = &LocalShardTelemetry> {
        self.shards
            .iter()
            .flatten()
            .filter_map(|shard| shard.local.as_ref())
    }

    pub fn count_vectors(&self) -> usize {
        self.local_shard_iter()
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
        self.local_shard_iter()
            .map(|local_shard| local_shard.num_points.unwrap_or(0))
            .sum()
    }

    pub fn count_points_per_vector(&self) -> TinyMap<VectorNameBuf, usize> {
        self.local_shard_iter()
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

    pub fn active_replica_min_max(&self) -> Option<(usize, usize)> {
        let min_max_active_replicas = self
            .shards
            .iter()
            .flatten()
            // While resharding up, some (shard) replica sets may still be incomplete during
            // the resharding process. In that case we don't want to consider these replica
            // sets at all in the active replica calculation. This is fine because searches nor
            // updates don't depend on them being available yet.
            //
            // More specifically:
            // - in stage 2 (migrate points) of resharding up we don't rely on the replica
            //   to be available yet. In this stage, these replicas will have the `Resharding`
            //   state.
            // - in stage 3 (replicate) of resharding up we activate the the replica and
            //   replicate to match the configuration replication factor. From this point on we
            //   do rely on the replica to be available. Now one replica will be `Active`, and
            //   the other replicas will be in a transfer state. No replica will have `Resharding`
            //   state.
            //
            // So, during stage 2 of resharding up we don't want to adjust the minimum number
            // of active replicas downwards. During stage 3 we do want it to affect the minimum
            // available replica number. It will be 1 for some time until replication transfers
            // complete.
            //
            // To ignore a (shard) replica set that is in stage 2 of resharding up, we simply
            // check if any of it's replicas is in `Resharding` state.
            .filter(|shard| {
                !shard
                    .replicate_states
                    .values()
                    .any(|i| matches!(i, ReplicaState::Resharding))
            })
            .map(|shard| {
                shard
                    .replicate_states
                    .values()
                    // While resharding down, all the replicas that we keep will get the
                    // `ReshardingScaleDown` state for a period of time. We simply consider
                    // these replicas to be active. The `is_active` function already accounts
                    // this.
                    .filter(|state| state.is_active())
                    .count()
            })
            .minmax();

        match min_max_active_replicas {
            itertools::MinMaxResult::NoElements => None,
            itertools::MinMaxResult::OneElement(one) => Some((one, one)),
            itertools::MinMaxResult::MinMax(min, max) => Some((min, max)),
        }
    }

    /// Returns the amount of non active replicas.
    pub fn dead_replicas(&self) -> usize {
        self.shards
            .iter()
            .flatten()
            .filter(|i| i.replicate_states.values().any(|state| !state.is_active()))
            .count()
    }

    pub fn optimization_time_spent_seconds(&self) -> usize {
        self.local_shard_iter()
            .map(|i| {
                i.optimizations
                    .optimizations
                    .total_duration_micros
                    .unwrap_or(0) as usize
                    / MICROSECONDS_PER_SECOND
            })
            .sum::<usize>()
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
