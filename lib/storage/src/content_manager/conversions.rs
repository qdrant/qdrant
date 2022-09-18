use std::collections::BTreeMap;
use collection::config::VectorsConfig;
use tonic::Status;

use crate::content_manager::collection_meta_ops::{
    AliasOperations, ChangeAliasesOperation, CollectionMetaOperations, CreateAlias,
    CreateAliasOperation, CreateCollection, CreateCollectionOperation, DeleteAlias,
    DeleteAliasOperation, DeleteCollectionOperation, RenameAlias, RenameAliasOperation,
    UpdateCollection, UpdateCollectionOperation,
};
use crate::content_manager::errors::StorageError;

pub fn error_to_status(error: StorageError) -> tonic::Status {
    let error_code = match &error {
        StorageError::BadInput { .. } => tonic::Code::InvalidArgument,
        StorageError::NotFound { .. } => tonic::Code::NotFound,
        StorageError::ServiceError { .. } => tonic::Code::Internal,
        StorageError::BadRequest { .. } => tonic::Code::InvalidArgument,
    };
    tonic::Status::new(error_code, format!("{}", error))
}

impl TryFrom<api::grpc::qdrant::CreateCollection> for CollectionMetaOperations {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::CreateCollection) -> Result<Self, Self::Error> {
        Ok(Self::CreateCollection(CreateCollectionOperation {
            collection_name: value.collection_name,
            create_collection: CreateCollection {
                vectors: match value.vectors_config {
                    Some(vectors) => match vectors.config {
                        None => return Err(Status::invalid_argument("vectors config is required")),
                        Some(params) => match params {
                            api::grpc::qdrant::vectors_config::Config::Params(vector_params) => {
                                VectorsConfig::Single(vector_params.try_into()?)
                            }
                            api::grpc::qdrant::vectors_config::Config::ParamsMap(
                                vectors_params,
                            ) => {
                                let mut params_map = BTreeMap::new();
                                for (name, params) in vectors_params.map {
                                    params_map.insert(name, params.try_into()?);
                                }
                                VectorsConfig::Multi(params_map)
                            }
                        },
                    },
                    None => return Err(Status::invalid_argument("vectors config is required")),
                },
                hnsw_config: value.hnsw_config.map(|v| v.into()),
                wal_config: value.wal_config.map(|v| v.into()),
                optimizers_config: value.optimizers_config.map(|v| v.into()),
                shard_number: value.shard_number,
                on_disk_payload: value.on_disk_payload,
            },
        }))
    }
}

impl TryFrom<api::grpc::qdrant::UpdateCollection> for CollectionMetaOperations {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::UpdateCollection) -> Result<Self, Self::Error> {
        Ok(Self::UpdateCollection(UpdateCollectionOperation {
            collection_name: value.collection_name,
            update_collection: UpdateCollection {
                optimizers_config: value.optimizers_config.map(|v| v.into()),
            },
        }))
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
