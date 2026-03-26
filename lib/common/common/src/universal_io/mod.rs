#[cfg(not(target_os = "windows"))]
pub mod disk_cache;
pub mod error;
pub mod file_ops;
#[cfg(target_os = "linux")]
pub mod io_uring;
pub mod local_file_ops;
pub mod mmap;
pub mod read;
mod wrappers;
pub mod write;

use std::path::Path;

use serde::de::DeserializeOwned;

pub use self::error::UniversalIoError;
pub use self::file_ops::UniversalReadFileOps;
#[cfg(target_os = "linux")]
pub use self::io_uring::*;
pub use self::mmap::*;
pub use self::read::UniversalRead;
pub use self::wrappers::*;
pub use self::write::UniversalWrite;
use crate::mmap::{Advice, AdviceSetting};

#[derive(Copy, Clone, Debug)]
pub struct OpenOptions {
    pub writeable: bool,
    pub need_sequential: bool,
    /// How many parallel requests to the disk we can do.
    /// If `None`, then use implementation-specific default.
    pub disk_parallel: Option<usize>,
    /// Populate RAM cache on open, if applicable for this implementation.
    pub populate: Option<bool>,
    /// Use specific mmap advice.
    pub advice: Option<AdviceSetting>,
    /// Whether to try to prevent caching for reads.
    pub prevent_caching: Option<bool>,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            writeable: true,
            need_sequential: true,
            disk_parallel: None,
            populate: None,
            advice: None,
            prevent_caching: None,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ReadRange {
    /// Start position in bytes from the beginning of the file/storage.
    pub byte_offset: u64,
    /// Number of elements to read.
    pub length: u64,
}

pub type ByteOffset = u64;

pub type FileIndex = usize;

pub type Flusher = Box<dyn FnOnce() -> Result<()> + Send>;

pub type Result<T, E = UniversalIoError> = std::result::Result<T, E>;

/// Open a file via universal io, read it as a whole, and deserialize as JSON.
///
/// Uses a single logical read when the backend overrides [`UniversalRead::read_whole`].
pub fn read_json_via<S, T>(path: impl AsRef<Path>) -> Result<T>
where
    S: UniversalRead<u8>,
    T: DeserializeOwned,
{
    let options = OpenOptions {
        writeable: false,
        need_sequential: false,
        disk_parallel: None,
        populate: Some(false),
        advice: Some(AdviceSetting::Advice(Advice::Sequential)),
        prevent_caching: Some(false),
    };

    let storage = S::open(path, options)?;
    let bytes = storage.read_whole()?;
    serde_json::from_slice(&bytes).map_err(UniversalIoError::from)
}
