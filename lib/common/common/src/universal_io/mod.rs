pub mod mmap;
pub mod multi_universal_read;
pub mod multi_universal_write;
pub mod single_file;

use std::path::{Path, PathBuf};

pub use multi_universal_read::{SourceId, StorageRead};
pub use multi_universal_write::StorageWrite;
use serde::de::DeserializeOwned;
pub use single_file::SingleFile;

use crate::mmap::{Advice, AdviceSetting};

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
    #[error("Invalid source id {source_id} (num sources: {num_sources})")]
    InvalidSourceId {
        source_id: usize,
        num_sources: usize,
    },
}

/// Open a file via universal io, read it as a whole, and deserialize as JSON.
pub fn read_json_via<S, T>(path: impl AsRef<Path>) -> Result<T>
where
    S: StorageRead<u8> + Sized,
    T: DeserializeOwned,
{
    let options = OpenOptions {
        need_sequential: false,
        disk_parallel: None,
        populate: Some(false),
        advice: Some(AdviceSetting::Advice(Advice::Sequential)),
    };

    let storage = SingleFile::<u8, S>::open(path, options)?;
    let bytes = storage.read_whole()?;
    serde_json::from_slice(&bytes).map_err(UniversalIoError::from)
}
