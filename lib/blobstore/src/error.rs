use std::ops::Range;

use common::mmap;
use common::universal_io::{IsNotFound, UniversalIoError};

use crate::tracker::{PageId, PointOffset};

#[derive(thiserror::Error, Debug)]
pub enum BlobstoreError {
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
    #[error("Operation not supported in append-only mode: {operation}")]
    UnsupportedOperation { operation: String },
    #[error("Page {page_id} not found")]
    PageNotFound { page_id: PageId },
    #[error("Requested pages {page_ids:?}, but we only have {available_pages} pages")]
    PageRangeNotFound {
        page_ids: Range<PageId>,
        available_pages: usize,
    },
    #[error("value {point_offset} not found")]
    ValueNotFound { point_offset: PointOffset },
    /// Stored bytes don't decode into a value, the storage is corrupt
    #[error("Failed to decode value: {description}")]
    DecodeError { description: String },
}

impl BlobstoreError {
    pub fn service_error(description: impl Into<String>) -> Self {
        BlobstoreError::ServiceError {
            description: description.into(),
        }
    }

    pub fn validation_error(message: impl Into<String>) -> Self {
        BlobstoreError::ValidationError {
            message: message.into(),
        }
    }

    pub fn unsupported_operation(operation: impl Into<String>) -> Self {
        BlobstoreError::UnsupportedOperation {
            operation: operation.into(),
        }
    }

    pub fn decode_error(description: impl Into<String>) -> Self {
        BlobstoreError::DecodeError {
            description: description.into(),
        }
    }
}

impl IsNotFound for BlobstoreError {
    fn is_not_found(&self) -> bool {
        match self {
            BlobstoreError::UniversalIo(err) => err.is_not_found(),
            BlobstoreError::Io(err) => err.is_not_found(),
            BlobstoreError::Mmap(err) => err.is_not_found(),
            BlobstoreError::SerdeJson(_)
            | BlobstoreError::ServiceError { .. }
            | BlobstoreError::FlushCancelled
            | BlobstoreError::ValidationError { .. }
            | BlobstoreError::UnsupportedOperation { .. }
            | BlobstoreError::PageNotFound { .. }
            | BlobstoreError::PageRangeNotFound { .. }
            | BlobstoreError::ValueNotFound { .. }
            | BlobstoreError::DecodeError { .. } => false,
        }
    }
}
