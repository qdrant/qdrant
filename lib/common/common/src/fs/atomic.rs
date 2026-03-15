use std::io::{self};
use std::path::PathBuf;

use fs_err as fs;

pub struct AtomicFile {
    path: PathBuf,
}

#[derive(Clone, Copy, Debug)]
pub enum OverwriteBehavior {
    AllowOverwrite,
}

pub const ALLOW_OVERWRITE: OverwriteBehavior = OverwriteBehavior::AllowOverwrite;
/// Alias for backward compatibility with atomicwrites
pub use ALLOW_OVERWRITE as AllowOverwrite;

impl AtomicFile {
    pub fn new(path: impl Into<PathBuf>, _behavior: OverwriteBehavior) -> Self {
        Self { path: path.into() }
    }

    pub fn write<F, T, E>(&self, f: F) -> std::result::Result<T, Error<E>>
    where
        F: FnOnce(&mut std::fs::File) -> std::result::Result<T, E>,
    {
        let tmp_path = self.path.with_extension("tmp_atomic");
        let mut file = std::fs::File::create(&tmp_path).map_err(Error::Internal)?;
        let result = f(&mut file).map_err(Error::User)?;
        file.sync_all().map_err(Error::Internal)?;
        drop(file);
        fs::rename(&tmp_path, &self.path).map_err(Error::Internal)?;
        Ok(result)
    }
}

#[derive(Debug)]
pub enum Error<E> {
    Internal(io::Error),
    User(E),
}

impl<E> std::fmt::Display for Error<E>
where
    E: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Internal(e) => write!(f, "Internal IO error: {}", e),
            Error::User(e) => write!(f, "User error: {}", e),
        }
    }
}

impl<E> std::error::Error for Error<E> where E: std::error::Error {}

impl<E> From<io::Error> for Error<E> {
    fn from(e: io::Error) -> Self {
        Error::Internal(e)
    }
}
