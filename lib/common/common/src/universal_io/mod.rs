pub mod mmap;

use std::borrow::Cow;
use std::path::Path;

/// Interface for accessing files in a universal way, abstracting away possible
/// implementations, such as memory map, io_uring, DIRECTIO, S3, etc.
pub trait UniversalRead<T: Copy + 'static> {
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self>
    where
        Self: Sized;

    /// Prefer [`read_batch`] if you need high performance.
    fn read<const SEQUENTIAL: bool>(&self, range: BytesRange) -> Result<Cow<'_, [T]>>;

    fn read_batch<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = BytesRange>,
        callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()>;

    fn len(&self) -> Result<u64>;

    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Fill RAM cache with related data, if applicable for this implementation.
    ///
    /// For example in MMAP-based files we do `madvise` with `MADV_POPULATE_READ`.
    fn populate(&self) -> Result<()>;

    /// Ask to evict related data from RAM cache, if applicable for this implementation.
    ///
    /// For example in MMAP-based files we do `fadvise` with `POSIX_FADV_DONTNEED`.
    fn clear_ram_cache(&self) -> Result<()>;
}

pub trait UniversalWrite<T: Copy + 'static>: UniversalRead<T> {
    fn write(&mut self, offset: ByteOffset, data: &[T]) -> Result<()>;

    fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ByteOffset, &'a [T])>,
    ) -> Result<()>;

    fn flusher(&self) -> Flusher;
}

#[derive(Copy, Clone, Debug)]
pub struct OpenOptions {
    pub need_sequential: bool,
    /// How many parallel requests to the disk we can do.
    /// If `None`, then use implementation-specific default.
    pub disk_parallel: Option<usize>,
    /// Populate RAM cache on open, if applicable for this implementation.
    pub populate: Option<bool>,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            need_sequential: true,
            disk_parallel: None,
            populate: None,
        }
    }
}

pub type ByteOffset = u64;

#[derive(Copy, Clone, Debug)]
pub struct BytesRange {
    pub start: ByteOffset,
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
}
