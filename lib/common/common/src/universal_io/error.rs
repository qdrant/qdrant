use std::io;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum UniversalIoError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Mmap(#[from] crate::mmap::Error),

    #[error(transparent)]
    IoUringNotSupported(io::Error),

    /// Path does not exist or is not accessible; backends may use this instead of
    /// `Io(NotFound)` so callers can match without relying on a specific io::ErrorKind.
    #[error("path {path} not found")]
    NotFound { path: PathBuf },

    #[error("elements range {start}..{end} is out of bounds, file contains {elements} elements")]
    OutOfBounds {
        start: u64,
        end: u64,
        elements: usize,
    },

    /// Source id is not valid for this multi-source storage.
    #[error("invalid file index {file_index} during multi-file operation, {files} files provided")]
    InvalidFileIndex { file_index: usize, files: usize },
}

impl From<serde_json::Error> for UniversalIoError {
    fn from(err: serde_json::Error) -> Self {
        Self::from(io::Error::from(err))
    }
}
