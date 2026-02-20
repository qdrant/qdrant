use std::borrow::Cow;

/// Interface for accessing files in a universal way, abstracting away possible
/// implementations, such as memory map, io_uring, DIRECTIO, S3, etc.
pub trait UniversalRead {
    fn open(path: &str, options: OpenOptions) -> Result<Self>
    where
        Self: Sized;

    /// Prefer [`read_batch`] if you need high performance.
    fn read(&self, range: BytesRange) -> Result<Cow<'_, [u8]>>;

    fn read_batch<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = BytesRange>,
        callback: impl FnMut(usize, &[u8]) -> Result<()>,
    ) -> Result<()>;

    /// Fill RAM cache with related data, if applicable for this implementation.
    ///
    /// For example in MMAP-based files we do `madvise` with `MADV_POPULATE_READ`.
    fn populate(&self) -> Result<()>;

    /// Ask to evict related data from RAM cache, if applicable for this implementation.
    ///
    /// For example in MMAP-based files we do `fadvise` with `POSIX_FADV_DONTNEED`.
    fn clear_ram_cache(&self) -> Result<()>;
}

pub trait UniversalWrite {
    fn write(&mut self, offset: ByteOffset, data: &[u8]) -> Result<()>;

    fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ByteOffset, &'a [u8])>,
    ) -> Result<()>;

    fn flusher(&self) -> Flusher;
}

#[derive(Copy, Clone, Debug)]
pub struct OpenOptions {
    pub need_sequential: bool,
    /// How many parallel requests to the disk we can do.
    /// If `None`, the use implementation-specific default.
    pub disk_parallel: Option<usize>,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            need_sequential: true,
            disk_parallel: None,
        }
    }
}

pub type ByteOffset = u64;
pub type BytesRange = (u64, u64);
pub type Flusher = Box<dyn FnOnce() -> Result<()> + Send>;

pub type Result<T, E = UniversalIoError> = std::result::Result<T, E>;
pub type UniversalIoError = std::io::Error;
