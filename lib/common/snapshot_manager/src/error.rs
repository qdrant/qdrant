use std::backtrace::Backtrace;
use std::io::Error as IoError;
use std::str::Utf8Error;

use s3::error::S3Error;
use tempfile::PersistError;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
#[error("{0}")]
pub enum SnapshotManagerError {
    #[error("Wrong input: {description}")]
    BadInput { description: String },
    #[error("Not found: {description}")]
    NotFound { description: String },
    #[error("Service internal error: {description}")]
    ServiceError {
        description: String,
        backtrace: Option<String>,
    },
    #[error("Bad request: {description}")]
    BadRequest { description: String },
    #[error("Timeout: {description}")]
    Timeout { description: String },
}

impl SnapshotManagerError {
    pub fn service_error(description: impl Into<String>) -> SnapshotManagerError {
        SnapshotManagerError::ServiceError {
            description: description.into(),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }

    pub fn bad_request(description: impl Into<String>) -> SnapshotManagerError {
        SnapshotManagerError::BadRequest {
            description: description.into(),
        }
    }

    pub fn bad_input(description: impl Into<String>) -> SnapshotManagerError {
        SnapshotManagerError::BadInput {
            description: description.into(),
        }
    }
}

impl From<S3Error> for SnapshotManagerError {
    fn from(value: S3Error) -> Self {
        match value {
            S3Error::Http(code, msg) => {
                if code == 404 {
                    SnapshotManagerError::NotFound {
                        description: format!("File not found on S3 ({:?})", msg),
                    }
                } else {
                    SnapshotManagerError::ServiceError {
                        description: format!("S3 returned {} {:?}", code, msg),
                        backtrace: Some(Backtrace::force_capture().to_string()),
                    }
                }
            }
            _ => SnapshotManagerError::ServiceError {
                description: format!("S3 error: {:?}", value),
                backtrace: Some(Backtrace::force_capture().to_string()),
            },
        }
    }
}

impl From<Utf8Error> for SnapshotManagerError {
    fn from(value: Utf8Error) -> Self {
        SnapshotManagerError::BadInput {
            description: format!("UTF-8 error: {:#?}", value),
        }
    }
}

impl From<IoError> for SnapshotManagerError {
    fn from(err: IoError) -> Self {
        SnapshotManagerError::service_error(format!("{err}"))
    }
}

impl From<tempfile::PathPersistError> for SnapshotManagerError {
    fn from(err: tempfile::PathPersistError) -> Self {
        Self::service_error(format!(
            "failed to persist temporary file path {}: {}",
            err.path.display(),
            err.error,
        ))
    }
}

impl<Guard> From<std::sync::PoisonError<Guard>> for SnapshotManagerError {
    fn from(err: std::sync::PoisonError<Guard>) -> Self {
        SnapshotManagerError::ServiceError {
            description: format!("Mutex lock poisoned: {err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for SnapshotManagerError {
    fn from(err: std::sync::mpsc::SendError<T>) -> Self {
        SnapshotManagerError::ServiceError {
            description: format!("Channel closed: {err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for SnapshotManagerError {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        SnapshotManagerError::ServiceError {
            description: format!("Oneshot channel sender dropped: {err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<tokio::sync::broadcast::error::RecvError> for SnapshotManagerError {
    fn from(err: tokio::sync::broadcast::error::RecvError) -> Self {
        SnapshotManagerError::ServiceError {
            description: format!("Broadcast channel sender dropped: {err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<tokio::task::JoinError> for SnapshotManagerError {
    fn from(err: tokio::task::JoinError) -> Self {
        SnapshotManagerError::ServiceError {
            description: format!("Tokio task join error: {err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<PersistError> for SnapshotManagerError {
    fn from(err: PersistError) -> Self {
        SnapshotManagerError::ServiceError {
            description: format!("Persist error: {err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}
