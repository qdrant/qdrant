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

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    Ok(serde_json::from_reader(BufReader::new(File::open(path)?))?)
}

pub fn read_bin<T: DeserializeOwned>(path: &Path) -> Result<T> {
    Ok(bincode::deserialize_from(BufReader::new(File::open(
        path,
    )?))?)
}

/// Advise the operating system that the file is no longer needed to be in the page cache.
pub fn advise_dontneed(path: &Path) -> Result<()> {
    _ = path;

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "emscripten",
        target_os = "fuchsia",
        target_os = "wasi",
        target_env = "uclibc",
        target_os = "freebsd",
    ))] // https://github.com/nix-rust/nix/blob/v0.29.0/src/fcntl.rs#L35-L42
    {
        nix::fcntl::posix_fadvise(
            std::os::fd::AsRawFd::as_raw_fd(&File::open(path)?),
            0,
            0,
            nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED,
        )?;
    }

    Ok(())
}

pub type FileOperationResult<T> = Result<T>;
pub type FileStorageError = Error;

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] io::Error),

    #[cfg(unix)]
    #[error("{0}")]
    Errno(#[from] nix::errno::Errno),

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
        io::Error::new(io::ErrorKind::Other, err.to_string())
    }
}
