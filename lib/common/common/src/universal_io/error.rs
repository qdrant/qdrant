use std::io;
use std::path::PathBuf;

/// `Result` extension for treating `NotFound` as `Ok(None)`
pub trait OkNotFound {
    type Ok;

    type Error;

    /// Treat the not found error as `Ok(None)`
    fn ok_not_found(self) -> Result<Option<Self::Ok>, Self::Error>;
}

pub trait IsNotFound {
    fn is_not_found(&self) -> bool;
}

impl IsNotFound for io::Error {
    fn is_not_found(&self) -> bool {
        self.kind() == io::ErrorKind::NotFound
    }
}

impl<T, E: IsNotFound> OkNotFound for Result<T, E> {
    type Ok = T;

    type Error = E;

    fn ok_not_found(self) -> Result<Option<T>, E> {
        match self {
            Ok(t) => Ok(Some(t)),
            Err(err) if err.is_not_found() => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl IsNotFound for UniversalIoError {
    fn is_not_found(&self) -> bool {
        match self {
            Self::NotFound { .. } => true,
            Self::Io(err) | Self::IoUringNotSupported(err) => err.is_not_found(),
            Self::Mmap(err) => err.is_not_found(),
            Self::Bincode(_)
            | Self::BytemuckCast(_)
            | Self::ZerocopySize(_)
            | Self::OutOfBounds { .. }
            | Self::InvalidFileIndex { .. }
            | Self::Uninitialized { .. }
            | Self::QueueIsFull
            | Self::S3(_)
            | Self::S3Config { .. }
            | Self::TaskPanicked(_) => false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UniversalIoError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Mmap(#[from] crate::mmap::Error),

    #[error(transparent)]
    Bincode(#[from] bincode::Error),

    #[error("Bytemuck cast error: {0:?}")]
    BytemuckCast(bytemuck::PodCastError),

    #[error("Zerocopy size error: {0:?}")]
    ZerocopySize(String),

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

    #[error("Request queue is full")]
    QueueIsFull,

    #[error("S3 object store error: {0}")]
    S3(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("S3 configuration missing or invalid: {description}")]
    S3Config { description: String },

    #[error("Background read task panicked: {0}")]
    TaskPanicked(String),
}

impl UniversalIoError {
    pub fn extract_not_found(err: io::Error, path: impl Into<PathBuf>) -> Self {
        #[expect(clippy::wildcard_enum_match_arm, reason = "error handling")]
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

    pub fn s3<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::S3(Box::new(err))
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

impl<Src, Dst: ?Sized> From<zerocopy::SizeError<Src, Dst>> for UniversalIoError {
    fn from(err: zerocopy::SizeError<Src, Dst>) -> Self {
        Self::ZerocopySize(format!("{err:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::universal_io::UniversalKind;

    #[test]
    fn s3_kind_variant_exists() {
        let k = UniversalKind::S3;
        assert_ne!(k, UniversalKind::Mmap);
        assert_ne!(k, UniversalKind::IoUring);
        assert_ne!(k, UniversalKind::DiskCache);
    }

    #[test]
    fn s3_error_variants_format() {
        let e = UniversalIoError::S3Config {
            description: "missing bucket".into(),
        };
        assert!(e.to_string().contains("missing bucket"));
    }
}
