use std::collections::HashMap;
use std::str::FromStr;

use api::conversions::json;
use api::grpc::qdrant as grpc;
use chrono::{DateTime, Utc};
use collection::operations::config_diff::{
    CollectionParamsDiff, HnswConfigDiff, OptimizersConfigDiff, QuantizationConfigDiff,
};
use collection::operations::conversions::sharding_method_from_proto;
use collection::operations::types::{SparseVectorsConfig, VectorsConfigDiff};
use segment::types::{StrictModeConfig, StrictModeMultivectorConfig, StrictModeSparseConfig};
use tonic::Status;
use tonic::metadata::MetadataValue;

use crate::content_manager::collection_meta_ops::{
    AliasOperations, ChangeAliasesOperation, CollectionMetaOperations, CreateAlias,
    CreateAliasOperation, CreateCollection, CreateCollectionOperation, DeleteAlias,
    DeleteAliasOperation, DeleteCollectionOperation, RenameAlias, RenameAliasOperation,
    UpdateCollection, UpdateCollectionOperation,
};
use crate::content_manager::errors::StorageError;
use crate::types::{ConsensusThreadStatus, StateRole};

impl From<StorageError> for Status {
    fn from(error: StorageError) -> Self {
        let mut metadata_headers = HashMap::new();
        let error_code = match &error {
            StorageError::BadInput { .. } => tonic::Code::InvalidArgument,
            StorageError::NotFound { .. } => tonic::Code::NotFound,
            StorageError::ServiceError { .. } => tonic::Code::Internal,
            StorageError::BadRequest { .. } => tonic::Code::InvalidArgument,
            StorageError::Locked { .. } => tonic::Code::FailedPrecondition,
            StorageError::Timeout { .. } => tonic::Code::DeadlineExceeded,
            StorageError::AlreadyExists { .. } => tonic::Code::AlreadyExists,
            StorageError::ChecksumMismatch { .. } => tonic::Code::DataLoss,
            StorageError::Forbidden { .. } => tonic::Code::PermissionDenied,
            StorageError::PreconditionFailed { .. } => tonic::Code::FailedPrecondition,
            StorageError::InferenceError { .. } => tonic::Code::InvalidArgument,
            StorageError::RateLimitExceeded {
                description: _,
                retry_after,
            } => {
                if let Some(retry_after) = retry_after {
                    // Retry-After is expressed in seconds `https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After`
                    // Ceil the value to the nearest second so clients don't retry too early
                    let retry_after_sec = retry_after.as_secs_f32().ceil() as u32;
                    metadata_headers.insert("retry-after", retry_after_sec.to_string());
                }
                tonic::Code::ResourceExhausted
            }
            StorageError::ShardUnavailable { .. } => tonic::Code::Unavailable,
            StorageError::EmptyPartialSnapshot { .. } => tonic::Code::FailedPrecondition,
        };
        let mut status = Status::new(error_code, format!("{error}"));
        // add metadata headers
        for (header_key, header_value) in metadata_headers {
            if let Ok(metadata) = MetadataValue::from_str(&header_value) {
                status.metadata_mut().insert(header_key, metadata);
            } else {
                log::info!("Failed to parse metadata header value: {header_value}");
            }
        }
        status
    }
}

impl TryFrom<grpc::CreateCollection> for CollectionMetaOperations {
    type Error = Status;

    fn try_from(value: grpc::CreateCollection) -> Result<Self, Self::Error> {
        let grpc::CreateCollection {
            collection_name,
            hnsw_config,
            wal_config,
            optimizers_config,
            shard_number,
            on_disk_payload,
            timeout: _,
            vectors_config,
            replication_factor,
            write_consistency_factor,
            quantization_config,
            sharding_method,
            sparse_vectors_config,
            strict_mode_config,
            metadata,
        } = value;
        let op = CreateCollectionOperation::new(
            collection_name,
            CreateCollection {
                vectors: match vectors_config.and_then(|config| config.config) {
                    Some(vector_config) => vector_config.try_into()?,
                    // TODO(sparse): sparse or dense vectors config is required
                    None => Default::default(),
                },
                sparse_vectors: sparse_vectors_config
                    .map(|v| SparseVectorsConfig::try_from(v).map(|SparseVectorsConfig(x)| x))
                    .transpose()?,
                hnsw_config: hnsw_config.map(|v| v.into()),
                wal_config: wal_config.map(|v| v.into()),
                optimizers_config: optimizers_config.map(TryFrom::try_from).transpose()?,
                shard_number,
                on_disk_payload,
                replication_factor,
                write_consistency_factor,
                quantization_config: quantization_config.map(TryInto::try_into).transpose()?,
                sharding_method: sharding_method
                    .map(sharding_method_from_proto)
                    .transpose()?,
                strict_mode_config: strict_mode_config.map(strict_mode_from_api),
                uuid: None,
                metadata: if metadata.is_empty() {
                    None
                } else {
                    Some(json::proto_to_payloads(metadata)?)
                },
            },
        )?;
        Ok(CollectionMetaOperations::CreateCollection(op))
    }
}

pub fn strict_mode_from_api(value: grpc::StrictModeConfig) -> StrictModeConfig {
    let grpc::StrictModeConfig {
        enabled,
        max_query_limit,
        max_timeout,
        unindexed_filtering_retrieve,
        unindexed_filtering_update,
        search_max_hnsw_ef,
        search_allow_exact,
        search_max_oversampling,
        upsert_max_batchsize,
        max_collection_vector_size_bytes,
        read_rate_limit,
        write_rate_limit,
        max_collection_payload_size_bytes,
        max_points_count,
        filter_max_conditions,
        condition_max_size,
        multivector_config,
        sparse_config,
        max_payload_index_count,
    } = value;
    StrictModeConfig {
        enabled,
        max_query_limit: max_query_limit.map(|i| i as usize),
        max_timeout: max_timeout.map(|i| i as usize),
        unindexed_filtering_retrieve,
        unindexed_filtering_update,
        search_max_hnsw_ef: search_max_hnsw_ef.map(|i| i as usize),
        search_allow_exact,
        search_max_oversampling: search_max_oversampling.map(f64::from),
        upsert_max_batchsize: upsert_max_batchsize.map(|i| i as usize),
        max_collection_vector_size_bytes: max_collection_vector_size_bytes.map(|i| i as usize),
        read_rate_limit: read_rate_limit.map(|i| i as usize),
        write_rate_limit: write_rate_limit.map(|i| i as usize),
        max_collection_payload_size_bytes: max_collection_payload_size_bytes.map(|i| i as usize),
        max_points_count: max_points_count.map(|i| i as usize),
        filter_max_conditions: filter_max_conditions.map(|i| i as usize),
        condition_max_size: condition_max_size.map(|i| i as usize),
        multivector_config: multivector_config.map(StrictModeMultivectorConfig::from),
        sparse_config: sparse_config.map(StrictModeSparseConfig::from),
        max_payload_index_count: max_payload_index_count.map(|i| i as usize),
    }
}

impl TryFrom<grpc::UpdateCollection> for CollectionMetaOperations {
    type Error = Status;

    fn try_from(value: grpc::UpdateCollection) -> Result<Self, Self::Error> {
        let grpc::UpdateCollection {
            collection_name,
            optimizers_config,
            timeout: _,
            params,
            hnsw_config,
            vectors_config,
            quantization_config,
            sparse_vectors_config,
            strict_mode_config,
            metadata,
        } = value;
        Ok(Self::UpdateCollection(UpdateCollectionOperation::new(
            collection_name,
            UpdateCollection {
                vectors: vectors_config
                    .and_then(|config| config.config)
                    .map(VectorsConfigDiff::try_from)
                    .transpose()?,
                hnsw_config: hnsw_config.map(HnswConfigDiff::from),
                params: params.map(CollectionParamsDiff::try_from).transpose()?,
                optimizers_config: optimizers_config
                    .map(OptimizersConfigDiff::try_from)
                    .transpose()?,
                quantization_config: quantization_config
                    .map(QuantizationConfigDiff::try_from)
                    .transpose()?,
                sparse_vectors: sparse_vectors_config
                    .map(SparseVectorsConfig::try_from)
                    .transpose()?,
                strict_mode_config: strict_mode_config.map(StrictModeConfig::from),
                metadata: if metadata.is_empty() {
                    None
                } else {
                    Some(json::proto_to_payloads(metadata)?)
                },
            },
        )))
    }
}

impl TryFrom<grpc::DeleteCollection> for CollectionMetaOperations {
    type Error = Status;

    fn try_from(value: grpc::DeleteCollection) -> Result<Self, Self::Error> {
        let grpc::DeleteCollection {
            collection_name,
            timeout: _,
        } = value;
        Ok(Self::DeleteCollection(DeleteCollectionOperation(
            collection_name,
        )))
    }
}

impl From<grpc::CreateAlias> for AliasOperations {
    fn from(value: grpc::CreateAlias) -> Self {
        let grpc::CreateAlias {
            collection_name,
            alias_name,
        } = value;
        Self::CreateAlias(CreateAliasOperation {
            create_alias: CreateAlias {
                collection_name,
                alias_name,
            },
        })
    }
}

impl From<grpc::DeleteAlias> for AliasOperations {
    fn from(value: grpc::DeleteAlias) -> Self {
        let grpc::DeleteAlias { alias_name } = value;
        Self::DeleteAlias(DeleteAliasOperation {
            delete_alias: DeleteAlias { alias_name },
        })
    }
}

impl From<grpc::RenameAlias> for AliasOperations {
    fn from(value: grpc::RenameAlias) -> Self {
        let grpc::RenameAlias {
            old_alias_name,
            new_alias_name,
        } = value;
        Self::RenameAlias(RenameAliasOperation {
            rename_alias: RenameAlias {
                old_alias_name,
                new_alias_name,
            },
        })
    }
}

impl TryFrom<grpc::AliasOperations> for AliasOperations {
    type Error = Status;

    fn try_from(value: grpc::AliasOperations) -> Result<Self, Self::Error> {
        let grpc::AliasOperations { action } = value;
        match action {
            Some(grpc::alias_operations::Action::CreateAlias(create)) => Ok(create.into()),
            Some(grpc::alias_operations::Action::DeleteAlias(delete)) => Ok(delete.into()),
            Some(grpc::alias_operations::Action::RenameAlias(rename)) => Ok(rename.into()),
            None => Err(Status::invalid_argument("Malformed AliasOperation type")),
        }
    }
}

impl TryFrom<grpc::ChangeAliases> for CollectionMetaOperations {
    type Error = Status;

    fn try_from(value: grpc::ChangeAliases) -> Result<Self, Self::Error> {
        let grpc::ChangeAliases {
            actions,
            timeout: _,
        } = value;
        let actions: Vec<AliasOperations> = actions
            .into_iter()
            .map(|a| a.try_into())
            .collect::<Result<_, _>>()?;
        Ok(Self::ChangeAliases(ChangeAliasesOperation { actions }))
    }
}

impl From<grpc::StateRole> for StateRole {
    fn from(role: grpc::StateRole) -> Self {
        match role {
            grpc::StateRole::Follower => StateRole::Follower,
            grpc::StateRole::Candidate => StateRole::Candidate,
            grpc::StateRole::Leader => StateRole::Leader,
            grpc::StateRole::PreCandidate => StateRole::PreCandidate,
        }
    }
}

impl From<StateRole> for grpc::StateRole {
    fn from(role: StateRole) -> Self {
        match role {
            StateRole::Follower => grpc::StateRole::Follower,
            StateRole::Candidate => grpc::StateRole::Candidate,
            StateRole::Leader => grpc::StateRole::Leader,
            StateRole::PreCandidate => grpc::StateRole::PreCandidate,
        }
    }
}

impl TryFrom<grpc::ConsensusThreadStatus> for ConsensusThreadStatus {
    type Error = Status;

    fn try_from(value: grpc::ConsensusThreadStatus) -> Result<Self, Self::Error> {
        match value.status {
            Some(grpc::consensus_thread_status::Status::Working(working)) => {
                // Convert milliseconds to DateTime<Utc>
                let timestamp = working.last_update_ms;
                let datetime =
                    DateTime::<Utc>::from_timestamp_millis(timestamp).ok_or_else(|| {
                        Status::invalid_argument("Invalid timestamp for consensus thread status")
                    })?;
                Ok(ConsensusThreadStatus::Working {
                    last_update: datetime,
                })
            }
            Some(grpc::consensus_thread_status::Status::Stopped(_)) => {
                Ok(ConsensusThreadStatus::Stopped)
            }
            Some(grpc::consensus_thread_status::Status::StoppedWithErr(err_status)) => {
                Ok(ConsensusThreadStatus::StoppedWithErr {
                    err: err_status.err,
                })
            }
            None => Ok(ConsensusThreadStatus::Stopped),
        }
    }
}

impl From<ConsensusThreadStatus> for grpc::ConsensusThreadStatus {
    fn from(status: ConsensusThreadStatus) -> Self {
        match status {
            ConsensusThreadStatus::Working { last_update } => {
                let timestamp = last_update.timestamp_millis();
                grpc::ConsensusThreadStatus {
                    status: Some(grpc::consensus_thread_status::Status::Working(
                        grpc::consensus_thread_status::Working {
                            last_update_ms: timestamp,
                        },
                    )),
                }
            }
            ConsensusThreadStatus::Stopped => grpc::ConsensusThreadStatus {
                status: Some(grpc::consensus_thread_status::Status::Stopped(
                    grpc::consensus_thread_status::Stopped {},
                )),
            },
            ConsensusThreadStatus::StoppedWithErr { err } => grpc::ConsensusThreadStatus {
                status: Some(grpc::consensus_thread_status::Status::StoppedWithErr(
                    grpc::consensus_thread_status::StoppedWithErr { err },
                )),
            },
        }
    }
}
