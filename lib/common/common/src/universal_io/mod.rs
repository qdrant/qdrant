pub mod error;
pub mod file_ops;
#[cfg(target_os = "linux")]
pub mod io_uring;
mod iterator;
pub mod local_file_ops;
pub mod mmap;
pub mod read;
mod reordering_queue;
pub mod write;

use std::path::Path;

use reordering_queue::ReorderingQueue;
use serde::de::DeserializeOwned;

pub use self::error::UniversalIoError;
pub use self::file_ops::UniversalReadFileOps;
#[cfg(target_os = "linux")]
pub use self::io_uring::*;
pub use self::iterator::UniversalSlice;
pub use self::mmap::*;
pub use self::read::UniversalRead;
pub use self::write::UniversalWrite;
use crate::generic_consts::Random;
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

#[derive(Copy, Clone, Debug)]
pub struct ReadRange {
    /// Start position in bytes from the beginning of the file/storage.
    pub byte_offset: u64,
    /// Number of elements to read.
    pub length: u64,
}

pub type ByteOffset = u64;

pub type FileIndex = usize;

impl ReadRange {
    fn iter_chunks(&self, max_size: u64) -> impl Iterator<Item = ReadRange> {
        let Self {
            byte_offset,
            length,
        } = *self;
        (0..length.div_ceil(max_size)).map(move |i| {
            let offset = i * max_size;
            ReadRange {
                byte_offset: byte_offset + offset,
                length: max_size.min(length - offset),
            }
        })
    }
}
pub type Flusher = Box<dyn FnOnce() -> Result<()> + Send>;

pub type Result<T, E = UniversalIoError> = std::result::Result<T, E>;

/// Read a single element at `index` via a one-element batch read,
/// passing it to the closure `f`.
pub fn read_one<T: Copy + 'static, R>(
    storage: &impl UniversalRead<T>,
    index: usize,
    f: impl FnOnce(&T) -> R,
) -> Result<R> {
    let mut f = Some(f);
    let mut result = None;
    storage.read_batch::<Random, UniversalIoError>(
        [ReadRange {
            byte_offset: index as u64,
            length: 1,
        }],
        |_, data| {
            result = Some((f.take().unwrap())(&data[0]));
            Ok(())
        },
    )?;
    Ok(result.unwrap())
}

/// Open a file via universal io, read it as a whole, and deserialize as JSON.
///
/// Uses a single logical read when the backend overrides [`UniversalRead::read_whole`].
pub fn read_json_via<S, T>(path: impl AsRef<Path>) -> Result<T>
where
    S: UniversalRead<u8>,
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
