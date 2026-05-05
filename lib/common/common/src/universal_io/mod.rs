#[cfg(not(target_os = "windows"))]
pub mod disk_cache;
pub mod error;
pub mod family;
pub mod file_ops;
#[cfg(target_os = "linux")]
pub mod io_uring;
pub mod local_file_ops;
pub mod mmap;
pub mod read;
pub mod wrappers;
pub mod write;

use std::path::Path;

use serde::de::DeserializeOwned;

pub use self::error::UniversalIoError;
pub use self::family::UniversalReadFamily;
pub use self::file_ops::UniversalReadFileOps;
#[cfg(target_os = "linux")]
pub use self::io_uring::*;
pub use self::mmap::*;
pub use self::read::*;
pub use self::wrappers::*;
pub use self::write::UniversalWrite;
use crate::mmap::{Advice, AdviceSetting};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum UniversalKind {
    Mmap,
    IoUring,
    DiskCache,
}

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

impl ReadRange {
    pub fn new(byte_offset: u64, length: u64) -> ReadRange {
        ReadRange {
            byte_offset,
            length,
        }
    }

    pub fn one(byte_offset: u64) -> ReadRange {
        ReadRange {
            byte_offset,
            length: 1,
        }
    }

    /// Split the range into a consecutive sequence of smaller ranges of
    /// reasonable size. Takes `T` as a hint for element size in bytes.
    pub fn iter_autochunks<T>(self) -> impl Iterator<Item = ReadRange> {
        // TODO: align chunks. Perhaps this method and `blocks_for_range_in_file`
        // can be unified.
        const MAX_CHUNK_BYTES: u64 = 16 * 1024;
        let chunk_len = (MAX_CHUNK_BYTES / size_of::<T>() as u64).max(1);
        let Self {
            byte_offset,
            length,
        } = self;
        (0..)
            .map(move |i| i * chunk_len)
            .take_while(move |&start| start < length)
            .map(move |start| ReadRange {
                byte_offset: byte_offset + start * size_of::<T>() as u64,
                length: std::cmp::min(chunk_len, length - start),
            })
    }
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
