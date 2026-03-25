use std::io;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum UniversalIoError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Mmap(#[from] crate::mmap::Error),

    #[error("Bytemuck cast error: {0:?}")]
    BytemuckCast(bytemuck::PodCastError),

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
    #[error("Resource was not initialized: {description}")]
    Uninitialized { description: String },
}

impl UniversalIoError {
    pub fn extract_not_found(err: io::Error, path: impl Into<PathBuf>) -> Self {
        match err.kind() {
            io::ErrorKind::NotFound => Self::NotFound { path: path.into() },
            _ => Self::Io(err),
        }
    }
    pub fn uninitialized(description: impl Into<String>) -> Self {
        Self::Uninitialized {
            description: description.into(),
        }
    }
}

impl From<serde_json::Error> for UniversalIoError {
    fn from(err: serde_json::Error) -> Self {
        Self::from(io::Error::from(err))
    }
}

impl From<bytemuck::PodCastError> for UniversalIoError {
    fn from(err: bytemuck::PodCastError) -> Self {
        Self::BytemuckCast(err)
    }
}
