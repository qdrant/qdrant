use std::path::Path;

use serde::de::DeserializeOwned;

use super::UniversalIoError;
use super::traits::UniversalRead;
use crate::mmap::{Advice, AdviceSetting};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum UniversalKind {
    Mmap,
    IoUring,
    DiskCache,
}

#[derive(Copy, Clone, Debug, Default)]
pub enum Populate {
    /// Let backend choose
    #[default]
    Auto,
    /// Do not populate
    No,
    /// Populate on foreground
    Blocking,
    /// Populate, but prefer to do it in the background
    PreferBackground,
}

impl From<bool> for Populate {
    fn from(populate: bool) -> Self {
        if populate {
            Populate::Blocking
        } else {
            Populate::No
        }
    }
}

/// Options for [`UniversalRead::open`].
///
/// No `#[derive(Default)]`. Prefer specifying all options explicitly. (except
/// for tests and [`OpenOptions::extra`]).
#[derive(Copy, Clone, Debug)]
pub struct OpenOptions {
    pub writeable: bool,
    pub need_sequential: bool,
    /// Populate RAM cache on open, if applicable for this implementation.
    pub populate: Populate,
    /// Use specific mmap advice.
    pub advice: Option<AdviceSetting>,
    /// Whether to try to prevent caching for reads.
    pub prevent_caching: Option<bool>,
}

impl OpenOptions {
    /// Default values for [`OpenOptions`].
    #[cfg(any(test, feature = "testing"))]
    pub fn new_for_test() -> Self {
        Self {
            writeable: true,
            need_sequential: true,
            populate: Populate::Auto,
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

    /// If the last element is beyond `max_end_offset`, then shorten the length
    /// to fit into the `0..max_end_offset` range.
    pub fn clamp<T>(mut self, max_end_offset: u64) -> ReadRange {
        self.byte_offset = self.byte_offset.min(max_end_offset);
        let max_len = max_end_offset.saturating_sub(self.byte_offset) / size_of::<T>() as u64;
        self.length = self.length.min(max_len);
        self
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
    S: UniversalRead,
    T: DeserializeOwned,
{
    let options = OpenOptions {
        writeable: false,
        need_sequential: false,
        populate: Populate::No,
        advice: Some(AdviceSetting::Advice(Advice::Sequential)),
        prevent_caching: Some(false),
    };

    let storage = S::open(path, options)?;
    let bytes = storage.read_whole::<u8>()?;
    serde_json::from_slice(&bytes).map_err(UniversalIoError::from)
}
