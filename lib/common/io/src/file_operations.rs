use std::fs::File;
use std::io::{self, BufReader, BufWriter};
use std::path::Path;
use std::result;

use atomicwrites::{AtomicFile, OverwriteBehavior};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub fn atomic_save_bin<T: Serialize>(path: &Path, object: &T) -> Result<()> {
    let af = AtomicFile::new(path, OverwriteBehavior::AllowOverwrite);
    af.write(|f| bincode::serialize_into(BufWriter::new(f), object))?;
    Ok(())
}

pub fn atomic_save_json<T: Serialize>(path: &Path, object: &T) -> Result<()> {
    let af = AtomicFile::new(path, OverwriteBehavior::AllowOverwrite);
    af.write(|f| serde_json::to_writer(BufWriter::new(f), object))?;
    Ok(())
}

pub fn read_bin<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let file = File::open(path)?;
    let value = bincode::deserialize_from(BufReader::new(file))?;
    Ok(value)
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let file = File::open(path)?;
    let value = serde_json::from_reader(BufReader::new(file))?;
    Ok(value)
}

/// Advise the operating system that the file is no longer needed to be in the page cache.
pub fn advise_dontneed(path: &Path) -> Result<()> {
    // https://github.com/nix-rust/nix/blob/v0.29.0/src/fcntl.rs#L35-L42
    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "android",
        target_os = "fuchsia",
        target_os = "emscripten",
        target_os = "wasi",
        target_env = "uclibc",
    ))]
    {
        use std::os::fd::AsRawFd as _;

        use nix::fcntl;

        let file = File::open(path)?;
        let fd = file.as_raw_fd();

        fcntl::posix_fadvise(fd, 0, 0, fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED)
            .map_err(io::Error::from)?;
    }

    _ = path;

    Ok(())
}

pub type FileOperationResult<T> = Result<T>;
pub type FileStorageError = Error;

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] io::Error),

    #[error("{0}")]
    Bincode(#[from] bincode::ErrorKind),

    #[error("{0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("{0}")]
    Generic(String),
}

impl Error {
    pub fn generic(msg: impl Into<String>) -> Self {
        Self::Generic(msg.into())
    }
}

impl<E> From<atomicwrites::Error<E>> for Error
where
    Self: From<E>,
{
    fn from(err: atomicwrites::Error<E>) -> Self {
        match err {
            atomicwrites::Error::Internal(err) => err.into(),
            atomicwrites::Error::User(err) => err.into(),
        }
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Self::Bincode(*err)
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(err) => err,
            Error::Bincode(err) => Self::other(err),
            Error::SerdeJson(err) => Self::other(err),
            Error::Generic(msg) => Self::other(msg),
        }
    }
}
