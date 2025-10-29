use std::backtrace::Backtrace;
use std::collections::TryReserveError;
use std::io::{Error as IoError, ErrorKind};
use std::sync::atomic::{AtomicBool, Ordering};

use atomicwrites::Error as AtomicIoError;
use io::file_operations::FileStorageError;
use memory::mmap_type::Error as MmapError;
use rayon::ThreadPoolBuildError;
use thiserror::Error;

use crate::types::{PayloadKeyType, PointIdType, SeqNumberType, VectorNameBuf};
use crate::utils::mem::Mem;

pub const PROCESS_CANCELLED_BY_SERVICE_MESSAGE: &str = "process cancelled by service";

#[derive(Error, Debug, Clone, PartialEq)]
#[error("{0}")]
pub enum OperationError {
    #[error("Vector dimension error: expected dim: {expected_dim}, got {received_dim}")]
    WrongVectorDimension {
        expected_dim: usize,
        received_dim: usize,
    },
    #[error("Not existing vector name error: {received_name}")]
    VectorNameNotExists { received_name: VectorNameBuf },
    #[error("No point with id {missed_point_id}")]
    PointIdError { missed_point_id: PointIdType },
    #[error(
        "Payload type does not match with previously given for field {field_name}. Expected: {expected_type}"
    )]
    TypeError {
        field_name: PayloadKeyType,
        expected_type: String,
    },
    #[error("Unable to infer type for the field '{field_name}'. Please specify `field_type`")]
    TypeInferenceError { field_name: PayloadKeyType },
    /// Service Error prevents further update of the collection until it is fixed.
    /// Should only be used for hardware, data corruption, IO, or other unexpected internal errors.
    #[error("Service runtime error: {description}")]
    ServiceError {
        description: String,
        backtrace: Option<String>,
    },
    #[error("Inconsistent storage: {description}")]
    InconsistentStorage { description: String },
    #[error("Out of memory, free: {free}, {description}")]
    OutOfMemory { description: String, free: u64 },
    #[error("Operation cancelled: {description}")]
    Cancelled { description: String },
    #[error("Validation failed: {description}")]
    ValidationError { description: String },
    #[error("Wrong usage of sparse vectors")]
    WrongSparse,
    #[error("Wrong usage of multi vectors")]
    WrongMulti,
    #[error(
        "No range index for `order_by` key: `{key}`. Please create one to use `order_by`. Check https://qdrant.tech/documentation/concepts/indexing/#payload-index to see which payload schemas support Range conditions"
    )]
    MissingRangeIndexForOrderBy { key: String },
    #[error(
        "No appropriate index for faceting: `{key}`. Please create one to facet on this field. Check https://qdrant.tech/documentation/concepts/indexing/#payload-index to see which payload schemas support Match conditions"
    )]
    MissingMapIndexForFacet { key: String },
    #[error(
        "Expected {expected_type} value for {field_name} in the payload and/or in the formula defaults. Error: {description}"
    )]
    VariableTypeError {
        field_name: PayloadKeyType,
        expected_type: String,
        description: String,
    },
    #[error("The expression {expression} produced a non-finite number")]
    NonFiniteNumber { expression: String },

    // ToDo: Remove after RocksDB is deprecated
    #[error("RocksDB column family {name} not found")]
    RocksDbColumnFamilyNotFound { name: String },
}

impl OperationError {
    /// Create a new service error with a description and a backtrace
    /// Warning: capturing a backtrace can be an expensive operation on some platforms, so this should be used with caution in performance-sensitive parts of code.
    pub fn service_error(description: impl Into<String>) -> OperationError {
        OperationError::ServiceError {
            description: description.into(),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }

    /// Create a new service error with a description and no backtrace
    pub fn service_error_light(description: impl Into<String>) -> OperationError {
        OperationError::ServiceError {
            description: description.into(),
            backtrace: None,
        }
    }

    pub fn validation_error(description: impl Into<String>) -> OperationError {
        OperationError::ValidationError {
            description: description.into(),
        }
    }

    pub fn inconsistent_storage(description: impl Into<String>) -> OperationError {
        OperationError::InconsistentStorage {
            description: description.into(),
        }
    }

    pub fn vector_name_not_exists(vector_name: impl Into<String>) -> OperationError {
        OperationError::VectorNameNotExists {
            received_name: vector_name.into(),
        }
    }
}

/// Contains information regarding last operation error, which should be fixed before next operation could be processed
#[derive(Debug, Clone)]
pub struct SegmentFailedState {
    pub version: SeqNumberType,
    pub point_id: Option<PointIdType>,
    pub error: OperationError,
}

impl From<ThreadPoolBuildError> for OperationError {
    fn from(error: ThreadPoolBuildError) -> Self {
        OperationError::ServiceError {
            description: format!("{error}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<FileStorageError> for OperationError {
    fn from(err: FileStorageError) -> Self {
        Self::service_error(err.to_string())
    }
}

impl From<MmapError> for OperationError {
    fn from(err: MmapError) -> Self {
        Self::service_error(err.to_string())
    }
}

impl From<serde_cbor::Error> for OperationError {
    fn from(err: serde_cbor::Error) -> Self {
        OperationError::service_error(format!("Failed to parse data: {err}"))
    }
}

impl<E> From<AtomicIoError<E>> for OperationError {
    fn from(err: AtomicIoError<E>) -> Self {
        match err {
            AtomicIoError::Internal(io_err) => OperationError::from(io_err),
            AtomicIoError::User(_user_err) => {
                OperationError::service_error("Unknown atomic write error")
            }
        }
    }
}

impl From<IoError> for OperationError {
    fn from(err: IoError) -> Self {
        match err.kind() {
            ErrorKind::OutOfMemory => {
                let free_memory = Mem::new().available_memory_bytes();
                OperationError::OutOfMemory {
                    description: format!("IO Error: {err}"),
                    free: free_memory,
                }
            }
            _ => OperationError::service_error(format!("IO Error: {err}")),
        }
    }
}

impl From<serde_json::Error> for OperationError {
    fn from(err: serde_json::Error) -> Self {
        OperationError::service_error(format!("Json error: {err}"))
    }
}

impl From<fs_extra::error::Error> for OperationError {
    fn from(err: fs_extra::error::Error) -> Self {
        OperationError::service_error(format!("File system error: {err}"))
    }
}

impl From<geohash::GeohashError> for OperationError {
    fn from(err: geohash::GeohashError) -> Self {
        OperationError::service_error(format!("Geohash error: {err}"))
    }
}

impl From<quantization::EncodingError> for OperationError {
    fn from(err: quantization::EncodingError) -> Self {
        match err {
            quantization::EncodingError::IOError(err)
            | quantization::EncodingError::EncodingError(err)
            | quantization::EncodingError::ArgumentsError(err) => {
                OperationError::service_error(format!("Quantization encoding error: {err}"))
            }
            quantization::EncodingError::Stopped => OperationError::Cancelled {
                description: PROCESS_CANCELLED_BY_SERVICE_MESSAGE.to_string(),
            },
        }
    }
}

impl From<TryReserveError> for OperationError {
    fn from(err: TryReserveError) -> Self {
        let free_memory = Mem::new().available_memory_bytes();
        OperationError::OutOfMemory {
            description: format!("Failed to reserve memory: {err}"),
            free: free_memory,
        }
    }
}

#[cfg(feature = "gpu")]
impl From<gpu::GpuError> for OperationError {
    fn from(err: gpu::GpuError) -> Self {
        Self::service_error(format!("GPU error: {err:?}"))
    }
}

pub type OperationResult<T> = Result<T, OperationError>;

pub fn get_service_error<T>(err: &OperationResult<T>) -> Option<OperationError> {
    match err {
        Ok(_) => None,
        Err(error) => match error {
            OperationError::ServiceError { .. } => Some(error.clone()),
            _ => None,
        },
    }
}

#[derive(Debug, Copy, Clone)]
pub struct CancelledError;

pub type CancellableResult<T> = Result<T, CancelledError>;

impl From<CancelledError> for OperationError {
    fn from(CancelledError: CancelledError) -> Self {
        OperationError::Cancelled {
            description: PROCESS_CANCELLED_BY_SERVICE_MESSAGE.to_string(),
        }
    }
}

pub fn check_process_stopped(stopped: &AtomicBool) -> CancellableResult<()> {
    if stopped.load(Ordering::Relaxed) {
        return Err(CancelledError);
    }
    Ok(())
}
