use thiserror::Error;
use collection::collection::CollectionError;


#[derive(Error, Debug, Clone)]
#[error("{0}")]
pub enum ServiceError {
    #[error("Wrong input: {description}")]
    BadInput { description: String },
    #[error("Not found: {description}")]
    NotFound { description: String },
    #[error("Service internal error: {description}")]
    ServiceError { description: String },
    #[error("Bad request: {description}")]
    BadRequest { description: String },
}


impl From<CollectionError> for ServiceError {
    fn from(err: CollectionError) -> Self {
        match err {
            CollectionError::BadInput { description } => ServiceError::BadInput { description },
            err @ CollectionError::NotFound { .. } => ServiceError::NotFound { description: format!("{}", err) },
            CollectionError::ServiceError { error } => ServiceError::ServiceError { description: error },
            CollectionError::BadRequest { description } => ServiceError::BadRequest { description },
        }
    }
}