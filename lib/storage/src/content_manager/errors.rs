use collection::operations::types::CollectionError;
use segment::common::file_operations::FileStorageError;
use std::io::Error as IoError;
use thiserror::Error;

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

impl StorageError {
    pub fn service_error(description: &str) -> StorageError {
        StorageError::ServiceError {
            description: description.to_string(),
        }
    }
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

impl From<IoError> for StorageError {
    fn from(err: IoError) -> Self {
        StorageError::service_error(&format!("{}", err))
    }
}

impl From<FileStorageError> for StorageError {
    fn from(err: FileStorageError) -> Self {
        match err {
            FileStorageError::IoError { description } => StorageError::service_error(&description),
            FileStorageError::UserAtomicIoError => {
                StorageError::service_error("Unknown atomic write error")
            }
            FileStorageError::GenericError { description } => {
                StorageError::service_error(&description)
            }
        }
    }
}

#[cfg(feature = "consensus")]
impl From<serde_cbor::Error> for StorageError {
    fn from(err: serde_cbor::Error) -> Self {
        StorageError::ServiceError {
            description: format!("cbor (de)serialization error: {}", err),
        }
    }
}
