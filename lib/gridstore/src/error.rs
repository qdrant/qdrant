use common::mmap;

#[derive(thiserror::Error, Debug)]
pub enum GridstoreError {
    #[error("{0}")]
    Mmap(#[from] mmap::Error),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    SerdeJson(#[from] serde_json::error::Error),
    #[error("Service error: {description}")]
    ServiceError { description: String },
    #[error("Flush was cancelled")]
    FlushCancelled,
    #[error("Validation error: {message}")]
    ValidationError { message: String },
}

impl GridstoreError {
    pub fn service_error(description: impl Into<String>) -> Self {
        GridstoreError::ServiceError {
            description: description.into(),
        }
    }

    pub fn validation_error(message: impl Into<String>) -> Self {
        GridstoreError::ValidationError {
            message: message.into(),
        }
    }
}
