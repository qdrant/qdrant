mod file_ops;
#[cfg(target_os = "linux")]
pub mod io_uring;
mod local_file_ops;
pub mod mmap;
pub mod read;
pub mod write;

use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

pub use self::file_ops::UniversalReadFileOps;
pub use self::read::UniversalRead;
pub use self::write::UniversalWrite;
use crate::mmap::{Advice, AdviceSetting};

pub type FileIndex = usize;

#[derive(Copy, Clone, Debug)]
pub struct OpenOptions {
    pub need_sequential: bool,
    /// How many parallel requests to the disk we can do.
    /// If `None`, then use implementation-specific default.
    pub disk_parallel: Option<usize>,
    /// Populate RAM cache on open, if applicable for this implementation.
    pub populate: Option<bool>,
    /// Use specific mmap advice.
    pub advice: Option<AdviceSetting>,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            need_sequential: true,
            disk_parallel: None,
            populate: None,
            advice: None,
        }
    }
}

pub type ElementOffset = u64;

#[derive(Copy, Clone, Debug)]
pub struct ElementsRange {
    pub start: ElementOffset,
    pub length: u64,
}
pub type Flusher = Box<dyn FnOnce() -> Result<()> + Send>;

pub type Result<T, E = UniversalIoError> = std::result::Result<T, E>;

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
}

/// Open a file via universal io, read it as a whole, and deserialize as JSON.
///
/// Uses a single logical read when the backend overrides [`UniversalRead::read_whole`].
pub fn read_json_via<S, T>(path: impl AsRef<Path>) -> Result<T>
where
    S: UniversalRead<u8> + Sized,
    T: DeserializeOwned,
{
    let options = OpenOptions {
        need_sequential: false,
        disk_parallel: None,
        populate: Some(false),
        advice: Some(AdviceSetting::Advice(Advice::Sequential)),
    };

    let storage = S::open(path, options)?;
    let bytes = storage.read_whole()?;
    serde_json::from_slice(&bytes).map_err(UniversalIoError::from)
}
