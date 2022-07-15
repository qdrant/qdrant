use std::fs::File;
use std::io::{BufWriter, Error as IoError, Read, Write};
use std::path::Path;
use std::result;

use atomicwrites::OverwriteBehavior::AllowOverwrite;
use atomicwrites::{AtomicFile, Error as AtomicIoError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
#[error("{0}")]
pub enum FileStorageError {
    #[error("File storage IO error {description} found")]
    IoError { description: String },
    #[error("File storage AtomicIO user error found")]
    UserAtomicIoError,
    #[error("Generic file storage error {description} found")]
    GenericError { description: String },
}

impl FileStorageError {
    pub fn generic_error(description: &str) -> FileStorageError {
        FileStorageError::GenericError {
            description: description.to_string(),
        }
    }
}

impl<E> From<AtomicIoError<E>> for FileStorageError {
    fn from(err: AtomicIoError<E>) -> Self {
        match err {
            AtomicIoError::Internal(io_err) => FileStorageError::IoError {
                description: format!("{}", io_err),
            },
            AtomicIoError::User(_atomic_io_err) => FileStorageError::UserAtomicIoError,
        }
    }
}

impl From<IoError> for FileStorageError {
    fn from(io_err: IoError) -> Self {
        FileStorageError::IoError {
            description: io_err.to_string(),
        }
    }
}

pub type FileOperationResult<T> = result::Result<T, FileStorageError>;

pub fn atomic_save_bin<N: DeserializeOwned + Serialize>(
    path: &Path,
    object: &N,
) -> FileOperationResult<()> {
    let af = AtomicFile::new(path, AllowOverwrite);
    af.write(|f| {
        let mut writer = BufWriter::new(f);
        bincode::serialize_into(&mut writer, object)
    })?;
    Ok(())
}

pub fn atomic_save_json<N: DeserializeOwned + Serialize>(
    path: &Path,
    object: &N,
) -> FileOperationResult<()> {
    let af = AtomicFile::new(path, AllowOverwrite);
    let state_bytes = serde_json::to_vec(object).unwrap();
    af.write(|f| f.write_all(&state_bytes))?;
    Ok(())
}

pub fn read_json<N: DeserializeOwned + Serialize>(path: &Path) -> FileOperationResult<N> {
    let mut contents = String::new();

    let mut file = File::open(path)?;
    file.read_to_string(&mut contents)?;

    let result: N = serde_json::from_str(&contents).map_err(|err| {
        FileStorageError::generic_error(&format!(
            "Failed to read data {}. Error: {}",
            path.to_str().unwrap(),
            err
        ))
    })?;

    Ok(result)
}

pub fn read_bin<N: DeserializeOwned + Serialize>(path: &Path) -> FileOperationResult<N> {
    let mut file = File::open(path)?;

    let result: N = bincode::deserialize_from(&mut file).map_err(|err| {
        FileStorageError::generic_error(&format!(
            "Failed to read data {}. Error: {}",
            path.to_str().unwrap(),
            err
        ))
    })?;

    Ok(result)
}
