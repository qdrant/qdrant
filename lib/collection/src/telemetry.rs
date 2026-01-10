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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub init_time_ms: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<CollectionConfigTelemetry>,

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

// Internal telemetry service conversions
mod internal_conversions {
    use api::grpc::conversions::{convert_shard_key_from_grpc_opt, convert_shard_key_to_grpc};
    use api::grpc::qdrant as grpc;
    use tonic::Status;

    use super::*;
    use crate::operations::cluster_ops::ReshardingDirection;
    use crate::shards::resharding::ReshardingStage;
    use crate::shards::transfer::ShardTransferMethod;

    impl TryFrom<grpc::ShardTransferTelemetry> for ShardTransferInfo {
        type Error = Status;

        fn try_from(value: grpc::ShardTransferTelemetry) -> Result<Self, Self::Error> {
            Ok(ShardTransferInfo {
                shard_id: value.shard_id,
                to_shard_id: value.to_shard_id,
                from: value.from,
                to: value.to,
                sync: value.sync,
                method: value
                    .method
                    .map(grpc::ShardTransferMethod::try_from)
                    .transpose()
                    .map_err(|err| {
                        Status::invalid_argument(format!("cannot decode ShardTransferMethod {err}"))
                    })?
                    .map(ShardTransferMethod::from),
                filter: None, // todo: add filter
                comment: (!value.comment.is_empty()).then_some(value.comment),
            })
        }
    }

    impl From<grpc::ReshardingStage> for ReshardingStage {
        fn from(value: grpc::ReshardingStage) -> Self {
            match value {
                grpc::ReshardingStage::MigratingPoints => ReshardingStage::MigratingPoints,
                grpc::ReshardingStage::ReadHashRingCommitted => {
                    ReshardingStage::ReadHashRingCommitted
                }
                grpc::ReshardingStage::WriteHashRingCommitted => {
                    ReshardingStage::WriteHashRingCommitted
                }
            }
        }
    }

    impl From<ReshardingStage> for grpc::ReshardingStage {
        fn from(value: ReshardingStage) -> Self {
            match value {
                ReshardingStage::MigratingPoints => grpc::ReshardingStage::MigratingPoints,
                ReshardingStage::ReadHashRingCommitted => {
                    grpc::ReshardingStage::ReadHashRingCommitted
                }
                ReshardingStage::WriteHashRingCommitted => {
                    grpc::ReshardingStage::WriteHashRingCommitted
                }
            }
        }
    }

    impl From<ShardTransferInfo> for grpc::ShardTransferTelemetry {
        fn from(value: ShardTransferInfo) -> Self {
            grpc::ShardTransferTelemetry {
                shard_id: value.shard_id,
                to_shard_id: value.to_shard_id,
                from: value.from,
                to: value.to,
                sync: value.sync,
                method: value
                    .method
                    .map(|method| grpc::ShardTransferMethod::from(method) as i32),
                comment: value.comment.unwrap_or_default(),
            }
        }
    }

    impl TryFrom<grpc::ReshardingTelemetry> for ReshardingInfo {
        type Error = Status;

        fn try_from(value: grpc::ReshardingTelemetry) -> Result<Self, Self::Error> {
            Ok(ReshardingInfo {
                uuid: Uuid::parse_str(&value.uuid)
                    .map_err(|err| Status::invalid_argument(format!("cannot parse Uuid {err}")))?,
                direction: ReshardingDirection::from(
                    grpc::ReshardingDirection::try_from(value.direction).map_err(|err| {
                        Status::invalid_argument(format!("cannot decode ReshardingDirection {err}"))
                    })?,
                ),
                shard_id: value.shard_id,
                peer_id: value.peer_id,
                shard_key: convert_shard_key_from_grpc_opt(value.shard_key),
                stage: ReshardingStage::from(
                    grpc::ReshardingStage::try_from(value.stage).map_err(|err| {
                        Status::invalid_argument(format!("cannot decode ReshardingStage {err}"))
                    })?,
                ),
            })
        }
    }

    impl From<ReshardingInfo> for grpc::ReshardingTelemetry {
        fn from(value: ReshardingInfo) -> Self {
            grpc::ReshardingTelemetry {
                uuid: value.uuid.to_string(),
                direction: grpc::ReshardingDirection::from(value.direction) as i32,
                shard_id: value.shard_id,
                peer_id: value.peer_id,
                shard_key: value.shard_key.map(convert_shard_key_to_grpc),
                stage: grpc::ReshardingStage::from(value.stage) as i32,
            }
        }
    }

    impl TryFrom<grpc::ShardCleanStatusTelemetry> for ShardCleanStatusTelemetry {
        type Error = Status;

        fn try_from(value: grpc::ShardCleanStatusTelemetry) -> Result<Self, Self::Error> {
            use grpc::shard_clean_status_telemetry::*;

            let Some(variant) = value.variant else {
                return Err(Status::invalid_argument(
                    "ShardCleanStatusTelemetry variant is missing",
                ));
            };

            let out = match variant {
                Variant::Started(Started {}) => ShardCleanStatusTelemetry::Started,
                Variant::Progress(Progress { deleted_points }) => {
                    ShardCleanStatusTelemetry::Progress(ShardCleanStatusProgressTelemetry {
                        deleted_points: deleted_points as usize,
                    })
                }
                Variant::Done(Done {}) => ShardCleanStatusTelemetry::Done,
                Variant::Failed(Failed { reason }) => {
                    ShardCleanStatusTelemetry::Failed(ShardCleanStatusFailedTelemetry { reason })
                }
                Variant::Cancelled(Cancelled {}) => ShardCleanStatusTelemetry::Cancelled,
            };

            Ok(out)
        }
    }

    impl From<ShardCleanStatusTelemetry> for grpc::ShardCleanStatusTelemetry {
        fn from(value: ShardCleanStatusTelemetry) -> Self {
            use grpc::shard_clean_status_telemetry::*;

            let variant = match value {
                ShardCleanStatusTelemetry::Started => Variant::Started(Started {}),
                ShardCleanStatusTelemetry::Progress(ShardCleanStatusProgressTelemetry {
                    deleted_points,
                }) => Variant::Progress(Progress {
                    deleted_points: deleted_points as u64,
                }),
                ShardCleanStatusTelemetry::Done => Variant::Done(Done {}),
                ShardCleanStatusTelemetry::Failed(ShardCleanStatusFailedTelemetry { reason }) => {
                    Variant::Failed(Failed { reason })
                }
                ShardCleanStatusTelemetry::Cancelled => Variant::Cancelled(Cancelled {}),
            };

            grpc::ShardCleanStatusTelemetry {
                variant: Some(variant),
            }
        }
    }

    impl TryFrom<grpc::CollectionTelemetry> for CollectionTelemetry {
        type Error = Status;

        fn try_from(value: grpc::CollectionTelemetry) -> Result<Self, Self::Error> {
            let grpc::CollectionTelemetry {
                id,
                transfers,
                resharding,
                shard_clean_tasks,
            } = value;

            let transfers: Option<Vec<ShardTransferInfo>> = (!transfers.is_empty()).then_some(
                transfers
                    .into_iter()
                    .map(ShardTransferInfo::try_from)
                    .collect::<Result<_, _>>()?,
            );

            let resharding: Option<Vec<ReshardingInfo>> = (!resharding.is_empty()).then_some(
                resharding
                    .into_iter()
                    .map(ReshardingInfo::try_from)
                    .collect::<Result<_, _>>()?,
            );

            let shard_clean_tasks: Option<HashMap<ShardId, ShardCleanStatusTelemetry>> =
                (!shard_clean_tasks.is_empty()).then_some(
                    shard_clean_tasks
                        .into_iter()
                        .map(|(shard_id, telemetry)| {
                            Ok::<_, Status>((
                                shard_id,
                                ShardCleanStatusTelemetry::try_from(telemetry)?,
                            ))
                        })
                        .collect::<Result<_, _>>()?,
                );

            Ok(CollectionTelemetry {
                id,
                init_time_ms: None, // Not provided in internal service
                config: None,       // Not provided in internal service
                shards: None,       // Not provided in internal service
                transfers,
                resharding,
                shard_clean_tasks,
            })
        }
    }

    impl From<CollectionTelemetry> for grpc::CollectionTelemetry {
        fn from(value: CollectionTelemetry) -> Self {
            let CollectionTelemetry {
                id,
                init_time_ms: _,
                config: _,
                shards: _,
                transfers,
                resharding,
                shard_clean_tasks,
            } = value;

            grpc::CollectionTelemetry {
                id,
                transfers: transfers
                    .into_iter()
                    .flatten()
                    .map(grpc::ShardTransferTelemetry::from)
                    .collect(),
                resharding: resharding
                    .into_iter()
                    .flatten()
                    .map(grpc::ReshardingTelemetry::from)
                    .collect(),
                shard_clean_tasks: shard_clean_tasks
                    .into_iter()
                    .flatten()
                    .map(|(shard_id, telemetry)| {
                        (shard_id, grpc::ShardCleanStatusTelemetry::from(telemetry))
                    })
                    .collect(),
            }
        }
    }
}
