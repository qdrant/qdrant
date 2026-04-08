use std::backtrace::Backtrace;
use std::io::Error as IoError;
use std::time::Duration;

use collection::operations::types::CollectionError;
use collection::shards::shard::ShardId;
use common::fs::FileStorageError;
use tempfile::PersistError;
use thiserror::Error;

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Error, Debug, Clone)]
#[error("{0}")]
pub enum StorageError {
    #[error("Wrong input: {description}")]
    BadInput { description: String },
    #[error("Wrong input: {description}")]
    AlreadyExists { description: String },
    #[error("Not found: {description}")]
    NotFound { description: String },
    #[error("Service internal error: {description}")]
    ServiceError {
        description: String,
        backtrace: Option<String>,
    },
    #[error("Bad request: {description}")]
    BadRequest { description: String },
    #[error("Storage locked: {description}")]
    Locked { description: String },
    #[error("Timeout: {description}")]
    Timeout { description: String },
    #[error("Checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },
    #[error("Forbidden: {description}")]
    Forbidden { description: String },
    #[error("Pre-condition failure: {description}")]
    PreconditionFailed { description: String }, // system is not in the state to perform the operation
    #[error("{description}")]
    InferenceError { description: String },
    #[error("Rate limiting exceeded: {description}")]
    RateLimitExceeded {
        description: String,
        retry_after: Option<Duration>,
    },
    #[error("Shard temporarily unavailable: {description}")]
    ShardUnavailable { description: String },
    #[error("Partial snapshot for shard {shard_id} contains no changes")]
    EmptyPartialSnapshot { shard_id: ShardId },
}

impl StorageError {
    pub fn inference_error(description: impl Into<String>) -> Self {
        Self::InferenceError {
            description: description.into(),
        }
    }

    pub fn service_error(description: impl Into<String>) -> Self {
        Self::ServiceError {
            description: description.into(),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }

    pub fn bad_request(description: impl Into<String>) -> Self {
        Self::BadRequest {
            description: description.into(),
        }
    }

    pub fn bad_input(description: impl Into<String>) -> Self {
        Self::BadInput {
            description: description.into(),
        }
    }

    pub fn already_exists(description: impl Into<String>) -> Self {
        Self::AlreadyExists {
            description: description.into(),
        }
    }

    pub fn not_found(description: impl Into<String>) -> Self {
        Self::NotFound {
            description: description.into(),
        }
    }

    pub fn checksum_mismatch(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        Self::ChecksumMismatch {
            expected: expected.into(),
            actual: actual.into(),
        }
    }

    pub fn forbidden(description: impl Into<String>) -> Self {
        Self::Forbidden {
            description: description.into(),
        }
    }

    pub fn timeout(timeout: Duration, operation: impl Into<String>) -> Self {
        Self::Timeout {
            description: format!(
                "Operation '{}' timed out after {timeout:?}",
                operation.into(),
            ),
        }
    }

    pub fn rate_limit_exceeded(
        description: impl Into<String>,
        retry_after: Option<Duration>,
    ) -> StorageError {
        StorageError::RateLimitExceeded {
            description: description.into(),
            retry_after,
        }
    }

    /// Used to override the `description` field of the resulting `StorageError`
    pub fn from_inconsistent_shard_failure(
        err: CollectionError,
        overriding_description: String,
    ) -> StorageError {
        match err {
            CollectionError::BadInput { .. } => StorageError::BadInput {
                description: overriding_description,
            },
            CollectionError::NotFound { .. } => StorageError::NotFound {
                description: overriding_description,
            },
            CollectionError::PointNotFound { .. } => StorageError::NotFound {
                description: overriding_description,
            },
            CollectionError::ServiceError { backtrace, .. } => StorageError::ServiceError {
                description: overriding_description,
                backtrace,
            },
            CollectionError::BadRequest { .. } => StorageError::BadRequest {
                description: overriding_description,
            },
            CollectionError::Cancelled { .. } => StorageError::ServiceError {
                description: format!("Operation cancelled: {overriding_description}"),
                backtrace: None,
            },
            CollectionError::InconsistentShardFailure { ref first_err, .. } => {
                Self::from_inconsistent_shard_failure(*first_err.clone(), overriding_description)
            }
            CollectionError::BadShardSelection { .. } => StorageError::BadRequest {
                description: overriding_description,
            },
            CollectionError::ForwardProxyError { error, .. } => {
                Self::from_inconsistent_shard_failure(*error, overriding_description)
            }
            CollectionError::OutOfMemory { .. } => StorageError::ServiceError {
                description: overriding_description,
                backtrace: None,
            },
            CollectionError::Timeout { .. } => StorageError::Timeout {
                description: overriding_description,
            },
            CollectionError::PreConditionFailed { .. } => StorageError::PreconditionFailed {
                description: overriding_description,
            },
            CollectionError::ObjectStoreError { .. } => StorageError::ServiceError {
                description: overriding_description,
                backtrace: None,
            },
            CollectionError::StrictMode { description } => StorageError::BadRequest { description },
            CollectionError::InferenceError { description } => {
                StorageError::InferenceError { description }
            }
            CollectionError::RateLimitExceeded {
                description,
                retry_after,
            } => StorageError::RateLimitExceeded {
                description,
                retry_after,
            },
            CollectionError::ShardUnavailable { .. } => StorageError::ShardUnavailable {
                description: overriding_description,
            },
        }
    }
}

impl From<CollectionError> for StorageError {
    fn from(err: CollectionError) -> Self {
        match err {
            CollectionError::BadInput { description } => StorageError::BadInput { description },
            CollectionError::NotFound { .. } => StorageError::NotFound {
                description: err.to_string(),
            },
            CollectionError::PointNotFound { .. } => StorageError::NotFound {
                description: err.to_string(),
            },
            CollectionError::ServiceError { error, backtrace } => StorageError::ServiceError {
                description: error,
                backtrace,
            },
            CollectionError::BadRequest { description } => StorageError::BadRequest { description },
            CollectionError::Cancelled { description } => StorageError::ServiceError {
                description: format!("Operation cancelled: {description}"),
                backtrace: None,
            },
            CollectionError::InconsistentShardFailure { ref first_err, .. } => {
                let full_description = err.to_string();
                Self::from_inconsistent_shard_failure(*first_err.clone(), full_description)
            }
            CollectionError::BadShardSelection { description } => {
                StorageError::BadRequest { description }
            }
            CollectionError::ForwardProxyError { error, .. } => {
                let full_description = error.to_string();
                Self::from_inconsistent_shard_failure(*error, full_description)
            }
            CollectionError::OutOfMemory { .. } => StorageError::ServiceError {
                description: err.to_string(),
                backtrace: None,
            },
            CollectionError::Timeout { .. } => StorageError::Timeout {
                description: err.to_string(),
            },
            CollectionError::PreConditionFailed { .. } => StorageError::PreconditionFailed {
                description: err.to_string(),
            },
            CollectionError::ObjectStoreError { .. } => StorageError::ServiceError {
                description: err.to_string(),
                backtrace: None,
            },
            CollectionError::StrictMode { description } => StorageError::BadRequest { description },
            CollectionError::InferenceError { description } => {
                StorageError::InferenceError { description }
            }
            CollectionError::RateLimitExceeded {
                description,
                retry_after,
            } => StorageError::RateLimitExceeded {
                description,
                retry_after,
            },
            CollectionError::ShardUnavailable { description } => {
                StorageError::ShardUnavailable { description }
            }
        }
    }
}

impl From<IoError> for StorageError {
    fn from(err: IoError) -> Self {
        Self::service_error(err.to_string())
    }
}

impl From<FileStorageError> for StorageError {
    fn from(err: FileStorageError) -> Self {
        Self::service_error(err.to_string())
    }
}

impl From<tempfile::PathPersistError> for StorageError {
    fn from(err: tempfile::PathPersistError) -> Self {
        Self::service_error(format!(
            "failed to persist temporary file path {}: {}",
            err.path.display(),
            err.error,
        ))
    }
}

impl<Guard> From<std::sync::PoisonError<Guard>> for StorageError {
    fn from(err: std::sync::PoisonError<Guard>) -> Self {
        Self::service_error(format!("Mutex lock poisoned: {err}"))
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for StorageError {
    fn from(err: std::sync::mpsc::SendError<T>) -> Self {
        Self::service_error(format!("Channel closed: {err}"))
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for StorageError {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::service_error(format!("Oneshot channel sender dropped: {err}"))
    }
}

impl From<tokio::sync::broadcast::error::RecvError> for StorageError {
    fn from(err: tokio::sync::broadcast::error::RecvError) -> Self {
        Self::service_error(format!("Broadcast channel sender dropped: {err}"))
    }
}

impl From<serde_cbor::Error> for StorageError {
    fn from(err: serde_cbor::Error) -> Self {
        Self::service_error(format!("cbor (de)serialization error: {err}"))
    }
}

impl From<serde_json::Error> for StorageError {
    fn from(err: serde_json::Error) -> Self {
        Self::service_error(format!("json (de)serialization error: {err}"))
    }
}

impl From<prost_for_raft::EncodeError> for StorageError {
    fn from(err: prost_for_raft::EncodeError) -> Self {
        Self::service_error(format!("prost encode error: {err}"))
    }
}

impl From<prost_for_raft::DecodeError> for StorageError {
    fn from(err: prost_for_raft::DecodeError) -> Self {
        Self::service_error(format!("prost decode error: {err}"))
    }
}

impl From<raft::Error> for StorageError {
    fn from(err: raft::Error) -> Self {
        Self::service_error(format!("Error in Raft consensus: {err}"))
    }
}

impl<E: std::fmt::Display> From<atomicwrites::Error<E>> for StorageError {
    fn from(err: atomicwrites::Error<E>) -> Self {
        Self::service_error(format!("Failed to write file: {err}"))
    }
}

impl From<tonic::transport::Error> for StorageError {
    fn from(err: tonic::transport::Error) -> Self {
        Self::service_error(format!("Tonic transport error: {err}"))
    }
}

impl From<reqwest::Error> for StorageError {
    fn from(err: reqwest::Error) -> Self {
        Self::service_error(format!("Http request error: {err}"))
    }
}

impl From<tokio::task::JoinError> for StorageError {
    fn from(err: tokio::task::JoinError) -> Self {
        Self::service_error(format!("Tokio task join error: {err}"))
    }
}

impl From<PersistError> for StorageError {
    fn from(err: PersistError) -> Self {
        Self::service_error(format!("Persist error: {err}"))
    }
}

impl From<cancel::Error> for StorageError {
    fn from(err: cancel::Error) -> Self {
        CollectionError::from(err).into()
    }
}
