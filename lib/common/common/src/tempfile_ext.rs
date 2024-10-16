use std::ops::Deref;
use std::path::{Path, PathBuf};

use tempfile::{PathPersistError, TempPath};

/// Either a temporary or a persistent path.
#[must_use = "returns a TempPath, if dropped the downloaded file is deleted"]
pub enum MaybeTempPath {
    Temporary(TempPath),
    Persistent(PathBuf),
}

impl MaybeTempPath {
    /// Keep the temporary file from being deleted.
    /// No-op if the path is persistent.
    pub fn keep(self) -> Result<PathBuf, PathPersistError> {
        match self {
            MaybeTempPath::Temporary(path) => path.keep(),
            MaybeTempPath::Persistent(path) => Ok(path),
        }
    }

    /// Close the temporary file, deleting it.
    /// No-op if the path is persistent.
    pub fn close(self) -> Result<(), std::io::Error> {
        match self {
            MaybeTempPath::Temporary(path) => path.close(),
            MaybeTempPath::Persistent(_) => Ok(()),
        }
    }
}

impl Deref for MaybeTempPath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        match self {
            MaybeTempPath::Temporary(path) => path,
            MaybeTempPath::Persistent(path) => path,
        }
    }
}
