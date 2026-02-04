use std::collections::HashMap;

use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::data_types::tiny_map::TinyMap;
use segment::types::{
    HnswConfig, Payload, QuantizationConfig, StrictModeConfigOutput, VectorNameBuf,
};
use serde::Serialize;
use uuid::Uuid;

use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use crate::operations::types::{OptimizersStatus, ReshardingInfo, ShardStatus, ShardTransferInfo};
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::shard::ShardId;
use crate::shards::telemetry::{
    LocalShardTelemetry, PartialSnapshotTelemetry, ReplicaSetTelemetry,
};

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
            .filter_map(|replica_set| replica_set.local.as_ref()?.optimizations.as_ref())
            .flat_map(|optimizer| optimizer.log.iter().flatten())
            .filter(|log| log.status.is_running())
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
    use crate::shards::telemetry::RemoteShardTelemetry;
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
                shards,
                transfers,
                resharding,
                shard_clean_tasks,
            } = value;

            let shards: Option<Vec<ReplicaSetTelemetry>> = (!shards.is_empty()).then_some(
                shards
                    .into_iter()
                    .map(ReplicaSetTelemetry::try_from)
                    .collect::<Result<_, _>>()?,
            );

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
                shards,
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
                shards,
                transfers,
                resharding,
                shard_clean_tasks,
            } = value;

            grpc::CollectionTelemetry {
                id,
                shards: shards
                    .into_iter()
                    .flatten()
                    .map(grpc::ReplicaSetTelemetry::from)
                    .collect(),
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

    impl From<ReplicaSetTelemetry> for grpc::ReplicaSetTelemetry {
        fn from(value: ReplicaSetTelemetry) -> Self {
            use crate::shards::telemetry::ReplicaSetTelemetry;

            let ReplicaSetTelemetry {
                id,
                key,
                local,
                remote,
                replicate_states,
                partial_snapshot,
            } = value;

            grpc::ReplicaSetTelemetry {
                id,
                key: key.map(convert_shard_key_to_grpc),
                local: local.map(grpc::LocalShardTelemetry::from),
                remote: remote
                    .into_iter()
                    .map(grpc::RemoteShardTelemetry::from)
                    .collect(),
                replica_states: replicate_states
                    .into_iter()
                    .map(|(k, v)| (k, grpc::ReplicaState::from(v) as i32))
                    .collect(),
                partial_snapshot: partial_snapshot.map(grpc::PartialSnapshotTelemetry::from),
            }
        }
    }

    impl From<LocalShardTelemetry> for grpc::LocalShardTelemetry {
        fn from(value: LocalShardTelemetry) -> Self {
            let LocalShardTelemetry {
                variant_name: _, // not included in grpc
                status,
                total_optimized_points,
                vectors_size_bytes,
                payloads_size_bytes,
                num_points,
                num_vectors,
                num_vectors_by_name,
                segments: _,      // not included in grpc
                optimizations: _, // not included in grpc
                async_scorer: _,  // not included in grpc
                indexed_only_excluded_vectors,
                update_queue: _, // not included in grpc
            } = value;

            grpc::LocalShardTelemetry {
                status: status.map(|s| grpc::ShardStatus::from(s) as i32),
                total_optimized_points: total_optimized_points as u64,
                vectors_size_bytes: vectors_size_bytes.map(|v| v as u64),
                payloads_size_bytes: payloads_size_bytes.map(|v| v as u64),
                num_points: num_points.map(|v| v as u64),
                num_vectors: num_vectors.map(|v| v as u64),
                num_vectors_by_name: num_vectors_by_name
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(k, v)| (k, v as u64))
                    .collect(),
                indexed_only_excluded_vectors: indexed_only_excluded_vectors
                    .into_iter()
                    .flatten()
                    .map(|(k, v)| (k, v as u64))
                    .collect(),
            }
        }
    }

    impl From<ShardStatus> for grpc::ShardStatus {
        fn from(value: ShardStatus) -> Self {
            match value {
                ShardStatus::Green => grpc::ShardStatus::Green,
                ShardStatus::Yellow => grpc::ShardStatus::Yellow,
                ShardStatus::Grey => grpc::ShardStatus::Grey,
                ShardStatus::Red => grpc::ShardStatus::Red,
            }
        }
    }

    impl From<RemoteShardTelemetry> for grpc::RemoteShardTelemetry {
        fn from(value: RemoteShardTelemetry) -> Self {
            let RemoteShardTelemetry {
                shard_id,
                peer_id,
                searches: _, // not included in grpc
                updates: _,  // not included in grpc
            } = value;
            grpc::RemoteShardTelemetry { shard_id, peer_id }
        }
    }

    impl From<PartialSnapshotTelemetry> for grpc::PartialSnapshotTelemetry {
        fn from(value: PartialSnapshotTelemetry) -> Self {
            let PartialSnapshotTelemetry {
                ongoing_create_snapshot_requests,
                is_recovering,
                recovery_timestamp,
            } = value;

            grpc::PartialSnapshotTelemetry {
                ongoing_create_snapshot_requests: ongoing_create_snapshot_requests as u64,
                is_recovering,
                recovery_timestamp,
            }
        }
    }

    impl From<ShardStatus> for grpc::CollectionStatus {
        fn from(value: ShardStatus) -> Self {
            use crate::operations::types::ShardStatus;

            match value {
                ShardStatus::Green => grpc::CollectionStatus::Green,
                ShardStatus::Yellow => grpc::CollectionStatus::Yellow,
                ShardStatus::Red => grpc::CollectionStatus::Red,
                ShardStatus::Grey => grpc::CollectionStatus::Grey,
            }
        }
    }

    impl TryFrom<grpc::ReplicaSetTelemetry> for ReplicaSetTelemetry {
        type Error = Status;

        fn try_from(value: grpc::ReplicaSetTelemetry) -> Result<Self, Self::Error> {
            use crate::shards::telemetry::ReplicaSetTelemetry;

            let grpc::ReplicaSetTelemetry {
                id,
                key,
                local,
                remote,
                replica_states,
                partial_snapshot,
            } = value;

            let replicate_states = replica_states
                .into_iter()
                .map(|(k, v)| {
                    Ok((
                        k,
                        ReplicaState::from(grpc::ReplicaState::try_from(v).map_err(|err| {
                            Status::invalid_argument(format!(
                                "Failed to decode ReplicaState: {err}"
                            ))
                        })?),
                    ))
                })
                .collect::<Result<_, Status>>()?;

            Ok(ReplicaSetTelemetry {
                id,
                key: key.and_then(|k| match k.key? {
                    grpc::shard_key::Key::Keyword(s) => {
                        Some(segment::types::ShardKey::Keyword(s.into()))
                    }
                    grpc::shard_key::Key::Number(n) => Some(segment::types::ShardKey::Number(n)),
                }),
                local: local.map(LocalShardTelemetry::try_from).transpose()?,
                remote: remote.into_iter().map(RemoteShardTelemetry::from).collect(),
                replicate_states,
                partial_snapshot: partial_snapshot
                    .map(PartialSnapshotTelemetry::try_from)
                    .transpose()?,
            })
        }
    }

    impl TryFrom<grpc::LocalShardTelemetry> for LocalShardTelemetry {
        type Error = Status;

        fn try_from(value: grpc::LocalShardTelemetry) -> Result<Self, Self::Error> {
            let grpc::LocalShardTelemetry {
                status,
                total_optimized_points,
                vectors_size_bytes,
                payloads_size_bytes,
                num_points,
                num_vectors,
                num_vectors_by_name,
                indexed_only_excluded_vectors,
            } = value;

            Ok(LocalShardTelemetry {
                variant_name: None,
                status: status
                    .map(grpc::ShardStatus::try_from)
                    .transpose()
                    .map_err(|err| {
                        Status::invalid_argument(format!("failed to decode ShardStatus: {err}"))
                    })?
                    .map(ShardStatus::from),
                total_optimized_points: total_optimized_points as usize,
                vectors_size_bytes: vectors_size_bytes.map(|v| v as usize),
                payloads_size_bytes: payloads_size_bytes.map(|v| v as usize),
                num_points: num_points.map(|v| v as usize),
                num_vectors: num_vectors.map(|v| v as usize),
                num_vectors_by_name: (!num_vectors_by_name.is_empty()).then(|| {
                    num_vectors_by_name
                        .into_iter()
                        .map(|(k, v)| (k, v as usize))
                        .collect()
                }),
                segments: None,      // Not included in grpc
                async_scorer: None,  // Not included in grpc
                optimizations: None, // Not included in grpc
                indexed_only_excluded_vectors: (!indexed_only_excluded_vectors.is_empty()).then(
                    || {
                        indexed_only_excluded_vectors
                            .into_iter()
                            .map(|(k, v)| (k, v as usize))
                            .collect()
                    },
                ),
                update_queue: None, // Not included in grpc
            })
        }
    }

    impl From<grpc::ShardStatus> for ShardStatus {
        fn from(value: grpc::ShardStatus) -> Self {
            match value {
                grpc::ShardStatus::Green => ShardStatus::Green,
                grpc::ShardStatus::Yellow => ShardStatus::Yellow,
                grpc::ShardStatus::Grey => ShardStatus::Grey,
                grpc::ShardStatus::Red => ShardStatus::Red,
            }
        }
    }

    impl From<grpc::RemoteShardTelemetry> for RemoteShardTelemetry {
        fn from(value: grpc::RemoteShardTelemetry) -> Self {
            let grpc::RemoteShardTelemetry { shard_id, peer_id } = value;

            RemoteShardTelemetry {
                shard_id,
                peer_id,
                searches: None, // not included in grpc
                updates: None,  // not included in grpc
            }
        }
    }

    impl TryFrom<grpc::PartialSnapshotTelemetry> for PartialSnapshotTelemetry {
        type Error = Status;

        fn try_from(value: grpc::PartialSnapshotTelemetry) -> Result<Self, Self::Error> {
            use crate::shards::telemetry::PartialSnapshotTelemetry;

            Ok(PartialSnapshotTelemetry {
                ongoing_create_snapshot_requests: value.ongoing_create_snapshot_requests as usize,
                is_recovering: value.is_recovering,
                recovery_timestamp: value.recovery_timestamp,
            })
        }
    }

    impl TryFrom<grpc::CollectionStatus> for ShardStatus {
        type Error = Status;

        fn try_from(value: grpc::CollectionStatus) -> Result<Self, Self::Error> {
            use crate::operations::types::ShardStatus;

            match value {
                grpc::CollectionStatus::Green => Ok(ShardStatus::Green),
                grpc::CollectionStatus::Yellow => Ok(ShardStatus::Yellow),
                grpc::CollectionStatus::Red => Ok(ShardStatus::Red),
                grpc::CollectionStatus::Grey => Ok(ShardStatus::Grey),
                grpc::CollectionStatus::UnknownCollectionStatus => {
                    Err(Status::invalid_argument("Unknown collection status"))
                }
            }
        }
    }
}
