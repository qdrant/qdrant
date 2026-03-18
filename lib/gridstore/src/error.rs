use common::mmap;
use common::universal_io::UniversalIoError;

use crate::tracker::PageId;

#[derive(thiserror::Error, Debug)]
pub enum GridstoreError {
    #[error("{0}")]
    Mmap(#[from] mmap::Error),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    UniversalIo(#[from] UniversalIoError),
    #[error("{0}")]
    SerdeJson(#[from] serde_json::error::Error),
    #[error("Service error: {description}")]
    ServiceError { description: String },
    #[error("Flush was cancelled")]
    FlushCancelled,
    #[error("Validation error: {message}")]
    ValidationError { message: String },
    #[error("Page {page_id} not found")]
    PageNotFound { page_id: PageId },
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
