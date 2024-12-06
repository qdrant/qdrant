use collection::operations::config_diff::{
    CollectionParamsDiff, HnswConfigDiff, OptimizersConfigDiff, QuantizationConfigDiff,
};
use collection::operations::conversions::sharding_method_from_proto;
use collection::operations::types::{SparseVectorsConfig, VectorsConfigDiff};
use segment::types::StrictModeConfig;
use tonic::Status;

use crate::content_manager::collection_meta_ops::{
    AliasOperations, ChangeAliasesOperation, CollectionMetaOperations, CreateAlias,
    CreateAliasOperation, CreateCollection, CreateCollectionOperation, DeleteAlias,
    DeleteAliasOperation, DeleteCollectionOperation, InitFrom, RenameAlias, RenameAliasOperation,
    UpdateCollection, UpdateCollectionOperation,
};
use crate::content_manager::errors::StorageError;

impl From<StorageError> for tonic::Status {
    fn from(error: StorageError) -> Self {
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
            StorageError::RateLimitExceeded { .. } => tonic::Code::ResourceExhausted,
        };
        Status::new(error_code, format!("{error}"))
    }
}

impl TryFrom<api::grpc::qdrant::CreateCollection> for CollectionMetaOperations {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::CreateCollection) -> Result<Self, Self::Error> {
        Ok(Self::CreateCollection(CreateCollectionOperation::new(
            value.collection_name,
            CreateCollection {
                vectors: match value.vectors_config.and_then(|config| config.config) {
                    Some(vector_config) => vector_config.try_into()?,
                    // TODO(sparse): sparse or dense vectors config is required
                    None => Default::default(),
                },
                sparse_vectors: value
                    .sparse_vectors_config
                    .map(|v| SparseVectorsConfig::try_from(v).map(|SparseVectorsConfig(x)| x))
                    .transpose()?,
                hnsw_config: value.hnsw_config.map(|v| v.into()),
                wal_config: value.wal_config.map(|v| v.into()),
                optimizers_config: value.optimizers_config.map(|v| v.into()),
                shard_number: value.shard_number,
                on_disk_payload: value.on_disk_payload,
                replication_factor: value.replication_factor,
                write_consistency_factor: value.write_consistency_factor,
                init_from: value
                    .init_from_collection
                    .map(|v| InitFrom { collection: v }),
                quantization_config: value
                    .quantization_config
                    .map(TryInto::try_into)
                    .transpose()?,
                sharding_method: value
                    .sharding_method
                    .map(sharding_method_from_proto)
                    .transpose()?,
                strict_mode_config: value.strict_mode_config.map(strict_mode_from_api),
                uuid: None,
            },
        )))
    }
}

pub fn strict_mode_from_api(value: api::grpc::qdrant::StrictModeConfig) -> StrictModeConfig {
    StrictModeConfig {
        enabled: value.enabled,
        max_query_limit: value.max_query_limit.map(|i| i as usize),
        max_timeout: value.max_timeout.map(|i| i as usize),
        unindexed_filtering_retrieve: value.unindexed_filtering_retrieve,
        unindexed_filtering_update: value.unindexed_filtering_update,
        search_max_hnsw_ef: value.search_max_hnsw_ef.map(|i| i as usize),
        search_allow_exact: value.search_allow_exact,
        search_max_oversampling: value.search_max_oversampling.map(f64::from),
        upsert_max_batchsize: value.upsert_max_batchsize.map(|i| i as usize),
        max_collection_vector_size_bytes: value
            .max_collection_vector_size_bytes
            .map(|i| i as usize),
        read_rate_limit_per_sec: value.write_rate_limit_per_sec.map(|i| i as usize),
        write_rate_limit_per_sec: value.write_rate_limit_per_sec.map(|i| i as usize),
    }
}

impl TryFrom<api::grpc::qdrant::UpdateCollection> for CollectionMetaOperations {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::UpdateCollection) -> Result<Self, Self::Error> {
        Ok(Self::UpdateCollection(UpdateCollectionOperation::new(
            value.collection_name,
            UpdateCollection {
                vectors: value
                    .vectors_config
                    .and_then(|config| config.config)
                    .map(VectorsConfigDiff::try_from)
                    .transpose()?,
                hnsw_config: value.hnsw_config.map(HnswConfigDiff::from),
                params: value
                    .params
                    .map(CollectionParamsDiff::try_from)
                    .transpose()?,
                optimizers_config: value.optimizers_config.map(OptimizersConfigDiff::from),
                quantization_config: value
                    .quantization_config
                    .map(QuantizationConfigDiff::try_from)
                    .transpose()?,
                sparse_vectors: value
                    .sparse_vectors_config
                    .map(SparseVectorsConfig::try_from)
                    .transpose()?,
                strict_mode_config: value.strict_mode_config.map(StrictModeConfig::from),
            },
        )))
    }
}

impl TryFrom<api::grpc::qdrant::DeleteCollection> for CollectionMetaOperations {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::DeleteCollection) -> Result<Self, Self::Error> {
        Ok(Self::DeleteCollection(DeleteCollectionOperation(
            value.collection_name,
        )))
    }
}

impl From<api::grpc::qdrant::CreateAlias> for AliasOperations {
    fn from(value: api::grpc::qdrant::CreateAlias) -> Self {
        Self::CreateAlias(CreateAliasOperation {
            create_alias: CreateAlias {
                collection_name: value.collection_name,
                alias_name: value.alias_name,
            },
        })
    }
}

impl From<api::grpc::qdrant::DeleteAlias> for AliasOperations {
    fn from(value: api::grpc::qdrant::DeleteAlias) -> Self {
        Self::DeleteAlias(DeleteAliasOperation {
            delete_alias: DeleteAlias {
                alias_name: value.alias_name,
            },
        })
    }
}

impl From<api::grpc::qdrant::RenameAlias> for AliasOperations {
    fn from(value: api::grpc::qdrant::RenameAlias) -> Self {
        Self::RenameAlias(RenameAliasOperation {
            rename_alias: RenameAlias {
                old_alias_name: value.old_alias_name,
                new_alias_name: value.new_alias_name,
            },
        })
    }
}

impl TryFrom<api::grpc::qdrant::AliasOperations> for AliasOperations {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::AliasOperations) -> Result<Self, Self::Error> {
        match value.action {
            Some(api::grpc::qdrant::alias_operations::Action::CreateAlias(create)) => {
                Ok(create.into())
            }
            Some(api::grpc::qdrant::alias_operations::Action::DeleteAlias(delete)) => {
                Ok(delete.into())
            }
            Some(api::grpc::qdrant::alias_operations::Action::RenameAlias(rename)) => {
                Ok(rename.into())
            }
            _ => Err(Status::invalid_argument("Malformed AliasOperation type")),
        }
    }
}

impl TryFrom<api::grpc::qdrant::ChangeAliases> for CollectionMetaOperations {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::ChangeAliases) -> Result<Self, Self::Error> {
        let actions: Vec<AliasOperations> = value
            .actions
            .into_iter()
            .map(|a| a.try_into())
            .collect::<Result<_, _>>()?;
        Ok(Self::ChangeAliases(ChangeAliasesOperation { actions }))
    }
}
