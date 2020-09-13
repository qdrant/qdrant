use thiserror::Error;
use collection::collection::CollectionError;
use sled::Error;
use sled::transaction::TransactionError;


#[derive(Error, Debug, Clone)]
#[error("{0}")]
pub enum StorageError {
    #[error("Wrong input: {description}")]
    BadInput { description: String },
    #[error("Not found: {description}")]
    NotFound { description: String },
    #[error("Service internal error: {description}")]
    ServiceError { description: String },
    #[error("Bad request: {description}")]
    BadRequest { description: String },
}


impl From<CollectionError> for StorageError {
    fn from(err: CollectionError) -> Self {
        match err {
            CollectionError::BadInput { description } => StorageError::BadInput { description },
            err @ CollectionError::NotFound { .. } => StorageError::NotFound { description: format!("{}", err) },
            CollectionError::ServiceError { error } => StorageError::ServiceError { description: error },
            CollectionError::BadRequest { description } => StorageError::BadRequest { description },
        }
    }
}

impl From<Error> for StorageError {
    fn from(err: Error) -> Self {
        StorageError::ServiceError { description: format!("Persistence error: {:?}", err) }
    }
}

impl From<TransactionError> for StorageError {
    fn from(err: TransactionError) -> Self {
        StorageError::ServiceError { description: format!("Persistence error: {}", err) }
    }
}