use collection::operations::types::CollectionError;
use sled::transaction::TransactionError;
use sled::Error;
use std::io::Error as IoError;
use std::rc::Rc;
use std::str::Utf8Error;
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
    #[error("IO error: {description}")]
    IO {
        description: String,
        source: Option<Rc<anyhow::Error>>,
    },
    #[error("parsing error: {description}")]
    Parse {
        description: String,
        source: Option<Rc<anyhow::Error>>,
    },
}
impl StorageError {
    pub(crate) fn from_std_io_error_with_msg(
        msg: impl Into<String>,
        source: std::io::Error,
    ) -> Self {
        StorageError::IO {
            description: msg.into(),
            source: Some(Rc::new(source.into())),
        }
    }
    pub(crate) fn from_any_io_error_with_msg(
        msg: impl Into<String>,
        source: anyhow::Error,
    ) -> Self {
        StorageError::IO {
            description: msg.into(),
            source: Some(Rc::new(source)),
        }
    }
    pub(crate) fn from_utf8_error_with_msg(msg: impl Into<String>, source: Utf8Error) -> Self {
        StorageError::Parse {
            description: msg.into(),
            source: Some(Rc::new(source.into())),
        }
    }
    pub(crate) fn from_io_msg(msg: impl Into<String>) -> Self {
        StorageError::IO {
            description: msg.into(),
            source: None,
        }
    }
    pub fn description(&self) -> String {
        match self {
            StorageError::BadInput { description } => description,
            StorageError::NotFound { description } => description,
            StorageError::ServiceError { description } => description,
            StorageError::BadRequest { description } => description,
            StorageError::IO { description, .. } => description,
            StorageError::Parse { description, .. } => description,
        }
        .to_owned()
    }
}
impl From<CollectionError> for StorageError {
    fn from(err: CollectionError) -> Self {
        match err {
            CollectionError::BadInput { description } => StorageError::BadInput { description },
            err @ CollectionError::NotFound { .. } => StorageError::NotFound {
                description: format!("{}", err),
            },
            CollectionError::ServiceError { error } => {
                StorageError::ServiceError { description: error }
            }
            CollectionError::BadRequest { description } => StorageError::BadRequest { description },
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
