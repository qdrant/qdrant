use std::borrow::Cow;
use std::ops::Range;
use std::path::Path;

use serde::de::DeserializeOwned;

use super::UniversalIoError;
use super::traits::UniversalReadFs;
use crate::mmap::{Advice, AdviceSetting};
use crate::universal_io::{UniversalRead, UserData};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum UniversalKind {
    Mmap,
    IoUring,
    DiskCache,
    SimpleDiskCache,
    S3,
    Gcs,
    Azure,
    /// Direct gRPC connection to a Qdrant peer's `StorageRead` service
    /// (see the `uio-grpc-client` / `io_bridge_uio_grpc` crates).
    UioGrpc,
}

impl UniversalKind {
    /// Whether data backed by this kind is fully resident in RAM or mapped into
    /// the address space (mmap), as opposed to being fetched on demand from disk
    /// or a remote object store.
    pub fn is_in_ram_or_mmap(self) -> bool {
        match self {
            UniversalKind::Mmap | UniversalKind::SimpleDiskCache => true,
            UniversalKind::IoUring
            | UniversalKind::DiskCache
            | UniversalKind::S3
            | UniversalKind::Gcs
            | UniversalKind::Azure
            | UniversalKind::UioGrpc => false,
        }
    }
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

impl Populate {
    pub fn to_bool<S: UniversalRead>(self) -> bool {
        match self {
            Populate::Auto => S::populate_auto(),
            Populate::No => false,
            Populate::Blocking | Populate::PreferBackground => true,
        }
    }
}

/// Options for [`UniversalReadFs::open`].
///
/// No `#[derive(Default)]`. Prefer specifying all options explicitly (except
/// for tests). Knobs in this struct are universal across backends —
/// backend-specific per-open knobs (e.g. `io_uring`'s `prevent_caching`)
/// live on [`UniversalReadFs::OpenExtra`](super::UniversalReadFs::OpenExtra)
/// passed alongside this struct.
#[derive(Copy, Clone, Debug)]
pub struct OpenOptions {
    pub writeable: bool,
    pub need_sequential: bool,
    /// Populate RAM cache on open, if applicable for this implementation.
    pub populate: Populate,
    /// Use specific mmap advice.
    pub advice: AdviceSetting,
}

impl OpenOptions {
    /// Default values for [`OpenOptions`].
    #[cfg(any(test, feature = "testing"))]
    pub fn new_for_test() -> Self {
        Self {
            writeable: true,
            need_sequential: true,
            populate: Populate::Auto,
            advice: AdviceSetting::Global,
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

    /// Turn into a range of bytes, considering the read length, and size of T
    pub fn into_byte_range<T: bytemuck::Pod>(self) -> std::ops::Range<u64> {
        let ReadRange {
            byte_offset,
            length,
        } = self;

        let t_size = size_of::<T>() as u64;
        let byte_len = length.checked_mul(t_size).expect("length overflow");
        let byte_end = byte_offset
            .checked_add(byte_len)
            .expect("byte offset overflow");

        byte_offset..byte_end
    }
}

pub struct ReadBytesItem<U: UserData> {
    pub user_data: U,
    pub range: Range<u64>,
    pub align: usize,
}

/// A single file matched by [`UniversalReadFileOps::list_files`]: its path
/// and size in bytes.
///
/// [`UniversalReadFileOps::list_files`]: super::UniversalReadFileOps::list_files
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ListedFile {
    pub path: std::path::PathBuf,
    pub size: u64,
}

pub type ByteOffset = u64;

pub type FileIndex = usize;

pub type Flusher = Box<dyn FnOnce() -> Result<()> + Send>;

pub type Result<T, E = UniversalIoError> = std::result::Result<T, E>;

pub fn read_whole_via<Fs, T>(
    fs: &Fs,
    path: impl AsRef<Path>,
    callback: impl FnOnce(Cow<'_, [u8]>) -> Result<T, UniversalIoError>,
) -> Result<T, UniversalIoError>
where
    Fs: UniversalReadFs,
{
    use super::UniversalRead;
    let options = OpenOptions {
        writeable: false,
        need_sequential: false,
        populate: Populate::No,
        advice: AdviceSetting::Advice(Advice::Sequential),
    };
    let storage = fs.open(path, options, Default::default())?;
    let bytes = storage.read_whole::<u8>()?;

    let callback_t = callback(bytes)?;

    storage.clear_ram_cache()?;

    Ok(callback_t)
}

/// Open a file via universal io, read it as a whole, and deserialize as JSON.
///
/// Uses a single logical read when the backend overrides
/// [`UniversalRead::read_whole`](super::UniversalRead::read_whole).
pub fn read_json_via<Fs, T>(fs: &Fs, path: impl AsRef<Path>) -> Result<T>
where
    Fs: UniversalReadFs,
    T: DeserializeOwned,
{
    read_whole_via(fs, path, |bytes| {
        serde_json::from_slice(&bytes).map_err(UniversalIoError::from)
    })
}

/// Open a file via universal io, read it as a whole, and deserialize as bincode.
///
/// Uses a single logical read when the backend overrides
/// [`UniversalRead::read_whole`](super::UniversalRead::read_whole).
pub fn read_bin_via<Fs, T>(fs: &Fs, path: impl AsRef<Path>) -> Result<T>
where
    Fs: UniversalReadFs,
    T: DeserializeOwned,
{
    read_whole_via(fs, path, |bytes| {
        bincode::deserialize(&bytes).map_err(UniversalIoError::from)
    })
}
