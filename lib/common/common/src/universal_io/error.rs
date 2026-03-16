use std::path::PathBuf;

#[derive(thiserror::Error, Debug)]
pub enum UniversalIoError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Mmap(#[from] crate::mmap::Error),
    #[error("Data range {start}..{end} is out of bounds (data size: {data_length} elements)")]
    OutOfBounds {
        start: u64,
        end: u64,
        data_length: usize,
    },
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    /// Path does not exist or is not accessible; backends may use this instead of
    /// `Io(NotFound)` so callers can match without relying on a specific io::ErrorKind.
    #[error("Not found: {path:?}")]
    NotFound { path: PathBuf },
    /// Source id is not valid for this multi-source storage.
    #[error("Invalid source id {file_index} (num sources: {num_files})")]
    InvalidFileIndex { file_index: usize, num_files: usize },
    #[error("IoUring not supported: {0}")]
    IoUringNotSupported(String),
}
