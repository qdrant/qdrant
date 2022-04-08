use crate::content_manager::collection_meta_ops::{
    default_shard_number, AliasOperations, ChangeAliasesOperation, CollectionMetaOperations,
    CreateAlias, CreateAliasOperation, CreateCollection, CreateCollectionOperation, DeleteAlias,
    DeleteAliasOperation, DeleteCollectionOperation, RenameAlias, RenameAliasOperation,
    UpdateCollection, UpdateCollectionOperation,
};
use crate::content_manager::errors::StorageError;
use tonic::Status;

pub fn error_to_status(error: StorageError) -> tonic::Status {
    let error_code = match &error {
        StorageError::BadInput { .. } => tonic::Code::InvalidArgument,
        StorageError::NotFound { .. } => tonic::Code::NotFound,
        StorageError::ServiceError { .. } => tonic::Code::Internal,
        StorageError::BadRequest { .. } => tonic::Code::InvalidArgument,
    };
    return tonic::Status::new(error_code, format!("{}", error));
}

impl TryFrom<api::grpc::qdrant::CreateCollection> for CollectionMetaOperations {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::CreateCollection) -> Result<Self, Self::Error> {
        let internal_distance = match api::grpc::qdrant::Distance::from_i32(value.distance) {
            Some(api::grpc::qdrant::Distance::Cosine) => segment::types::Distance::Cosine,
            Some(api::grpc::qdrant::Distance::Euclid) => segment::types::Distance::Euclid,
            Some(api::grpc::qdrant::Distance::Dot) => segment::types::Distance::Dot,
            Some(_) => return Err(Status::failed_precondition("Unknown distance")),
            _ => return Err(Status::failed_precondition("Bad value of distance field!")),
        };

        Ok(Self::CreateCollection(CreateCollectionOperation {
            collection_name: value.collection_name,
            create_collection: CreateCollection {
                vector_size: value.vector_size as usize,
                distance: internal_distance,
                hnsw_config: value.hnsw_config.map(|v| v.into()),
                wal_config: value.wal_config.map(|v| v.into()),
                optimizers_config: value.optimizers_config.map(|v| v.into()),
                shard_number: value.shard_number.unwrap_or_else(default_shard_number),
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
