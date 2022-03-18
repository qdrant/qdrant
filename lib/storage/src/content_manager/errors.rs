use collection::operations::types::CollectionError;
use sled::transaction::TransactionError;
use sled::Error;
use std::io::Error as IoError;
use thiserror::Error;
use tokio::task::JoinError;

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
            err @ CollectionError::NotFound { .. } => StorageError::NotFound {
                description: format!("{err}"),
            },
            CollectionError::ServiceError { error } => {
                StorageError::ServiceError { description: error }
            }
            CollectionError::BadRequest { description } => StorageError::BadRequest { description },
            CollectionError::Cancelled { description } => StorageError::ServiceError {
                description: format!("Operation cancelled: {description}"),
            },
            err @ CollectionError::InconsistentFailure { .. } => StorageError::ServiceError {
                description: format!("{err}"),
            },
        }
    }
}

impl From<Error> for StorageError {
    fn from(err: Error) -> Self {
        StorageError::ServiceError {
            description: format!("Persistence error: {:?}", err),
        }
    }
}

impl From<TransactionError> for StorageError {
    fn from(err: TransactionError) -> Self {
        StorageError::ServiceError {
            description: format!("Persistence error: {}", err),
        }
    }
}

impl From<IoError> for StorageError {
    fn from(err: IoError) -> Self {
        StorageError::ServiceError {
            description: format!("{}", err),
        }
    }
}

impl From<JoinError> for StorageError {
    fn from(err: JoinError) -> Self {
        StorageError::ServiceError {
            description: format!("{}", err),
        }
    }
}
