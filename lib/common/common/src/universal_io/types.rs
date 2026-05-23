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

/// Universal access-pattern hint passed at open time. Each backend honors or
/// ignores it independently (mmap maps to `madvise`; io_uring ignores).
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub enum AccessHint {
    /// Backend picks a default (mmap: process-global `madvise` setting).
    #[default]
    Default,
    /// Standard access; no specific pattern (mmap: `MADV_NORMAL`).
    Normal,
    /// Sequential access (mmap: `MADV_SEQUENTIAL`).
    Sequential,
    /// Random access (mmap: `MADV_RANDOM`).
    Random,
}

impl From<AdviceSetting> for AccessHint {
    fn from(advice: AdviceSetting) -> Self {
        match advice {
            AdviceSetting::Global => AccessHint::Default,
            AdviceSetting::Advice(Advice::Normal) => AccessHint::Normal,
            AdviceSetting::Advice(Advice::Sequential) => AccessHint::Sequential,
            AdviceSetting::Advice(Advice::Random) => AccessHint::Random,
        }
    }
}

/// Universal page-cache hint passed at open time. Each backend honors or
/// ignores it independently (io_uring uses `O_DIRECT` for `Bypass`; mmap
/// can't bypass the page cache and ignores).
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub enum CacheHint {
    /// Use the backend's default caching behavior.
    #[default]
    Default,
    /// Bypass the OS page cache where possible (io_uring: `O_DIRECT`).
    Bypass,
}

/// Options for [`UniversalRead::open`].
///
/// No `#[derive(Default)]`. Callers must explicitly decide
/// [`writeable`](Self::writeable), [`populate`](Self::populate),
/// [`access_hint`](Self::access_hint), and
/// [`need_sequential`](Self::need_sequential) at every call site —
/// these are the load-shape decisions an opener has to think about.
/// Rarely-tweaked knobs live in [`extra`](Self::extra) and can be
/// defaulted via [`OpenOptionsExtra::default()`] or constructed with
/// builder methods.
#[derive(Copy, Clone, Debug)]
pub struct OpenOptions {
    pub writeable: bool,
    pub populate: Populate,
    pub access_hint: AccessHint,
    /// Request a secondary sequential read path *in addition to* the primary.
    ///
    /// Mmap honors by creating a second mmap with `MADV_SEQUENTIAL` for bulk
    /// reads while the primary serves the access pattern in `access_hint`.
    /// Other backends ignore.
    pub need_sequential: bool,
    /// Rarely-tweaked knobs. `OpenOptionsExtra::default()` for the common case.
    pub extra: OpenOptionsExtra,
}

/// Rarely-tweaked open-time knobs.
///
/// Defaults are correct for the vast majority of opens; tweak via builder
/// methods (`.with_*`) when a specific backend behavior is required.
#[derive(Copy, Clone, Debug, Default)]
pub struct OpenOptionsExtra {
    pub cache_hint: CacheHint,
}

impl OpenOptionsExtra {
    pub fn with_cache_hint(mut self, cache_hint: CacheHint) -> Self {
        self.cache_hint = cache_hint;
        self
    }
}

impl OpenOptions {
    /// Default values for [`OpenOptions`].
    #[cfg(any(test, feature = "testing"))]
    pub fn new_for_test() -> Self {
        Self {
            writeable: true,
            populate: Populate::Auto,
            access_hint: AccessHint::Default,
            need_sequential: true,
            extra: OpenOptionsExtra::default(),
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
        populate: Populate::No,
        access_hint: AccessHint::Sequential,
        need_sequential: false,
        extra: OpenOptionsExtra::default(),
    };
    let storage = S::open(path, options)?;
    let bytes = storage.read_whole::<u8>()?;
    serde_json::from_slice(&bytes).map_err(UniversalIoError::from)
}
