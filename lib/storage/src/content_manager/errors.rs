use thiserror::Error;
use collection::collection::CollectionError;


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