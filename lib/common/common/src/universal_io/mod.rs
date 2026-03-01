pub mod mmap;

use std::borrow::Cow;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

use crate::mmap::AdviceSetting;

/// Identifier for a source in a multi-source storage (e.g. a file, an S3 object).
/// Each multi-source storage assigns source ids to its constituent backends.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceId(pub usize);

/// Interface for accessing files in a universal way, abstracting away possible
/// implementations, such as memory map, io_uring, DIRECTIO, S3, etc.
pub trait UniversalRead<T: Copy + 'static> {
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self>
    where
        Self: Sized;

    /// Prefer [`read_batch`] if you need high performance.
    fn read<const SEQUENTIAL: bool>(&self, range: ElementsRange) -> Result<Cow<'_, [T]>>;

    /// Read the entire file in one logical access.
    ///
    /// Implementations may override this to avoid the two accesses that would result from
    /// `len()` followed by `read(0..len())`. Default implementation does exactly that.
    fn read_whole(&self) -> Result<Cow<'_, [T]>> {
        let n = self.len()?;
        self.read::<false>(ElementsRange {
            start: 0,
            length: n,
        })
    }

    fn read_batch<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = ElementsRange>,
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
    fn write(&mut self, offset: ElementOffset, data: &[T]) -> Result<()>;

    fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ElementOffset, &'a [T])>,
    ) -> Result<()>;

    fn flusher(&self) -> Flusher;
}

/// Interface for batched read access across multiple sources (files, S3 objects, etc.).
/// Complements [`UniversalRead`], which is single-source.
pub trait MultiUniversalRead<T: Copy + 'static> {
    /// Batch read across sources. Each request is `(SourceId, ElementsRange)`.
    /// Invokes `callback(request_index, slice)` for each range in order of `requests`.
    fn read_batch_multi<const SEQUENTIAL: bool>(
        &self,
        requests: impl IntoIterator<Item = (SourceId, ElementsRange)>,
        callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()>;

    /// Length in elements of the given source. Optional; default returns unsupported error.
    fn source_len(&self, source_id: SourceId) -> Result<u64> {
        let _ = source_id;
        Err(UniversalIoError::Io(std::io::Error::new(
            ErrorKind::Unsupported,
            "source_len not implemented",
        )))
    }

    /// Fill RAM cache for all sources, if applicable. Default is a no-op.
    fn populate(&self) -> Result<()> {
        Ok(())
    }

    /// Evict RAM cache for all sources, if applicable. Default is a no-op.
    fn clear_ram_cache(&self) -> Result<()> {
        Ok(())
    }
}

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
///
/// Uses a single logical read when the backend overrides [`UniversalRead::read_whole`].
pub fn read_json_via<S, T>(path: impl AsRef<Path>, options: OpenOptions) -> Result<T>
where
    S: UniversalRead<u8> + Sized,
    T: DeserializeOwned,
{
    let storage = S::open(path, options)?;
    let bytes = storage.read_whole()?;
    serde_json::from_slice(&bytes).map_err(UniversalIoError::from)
}

// ========== Minimal multi-source implementation ==========

/// Minimal multi-source read implementation: a collection of [`UniversalRead`] backends
/// addressable by [`SourceId`] (index). Supports attaching more sources at runtime.
#[derive(Debug)]
pub struct VecMultiUniversalRead<T, S> {
    sources: Vec<S>,
    _element: std::marker::PhantomData<T>,
}

impl<T: Copy + 'static, S: UniversalRead<T>> VecMultiUniversalRead<T, S> {
    /// Create an empty multi-source view. Use [`Self::attach`] to add sources.
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            _element: std::marker::PhantomData,
        }
    }

    /// Create from an initial set of sources. Source ids will be 0, 1, 2, ...
    pub fn from_sources(sources: Vec<S>) -> Self {
        Self {
            sources,
            _element: std::marker::PhantomData,
        }
    }

    /// Attach another source and return its [`SourceId`]. The new id is the current
    /// number of sources (so existing ids remain valid).
    pub fn attach(&mut self, source: S) -> SourceId {
        let id = SourceId(self.sources.len());
        self.sources.push(source);
        id
    }

    /// Number of sources currently attached.
    pub fn len(&self) -> usize {
        self.sources.len()
    }

    /// True if there are no sources.
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }
}

impl<T: Copy + 'static, S: UniversalRead<T>> Default for VecMultiUniversalRead<T, S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Copy + 'static, S: UniversalRead<T>> MultiUniversalRead<T> for VecMultiUniversalRead<T, S> {
    fn read_batch_multi<const SEQUENTIAL: bool>(
        &self,
        requests: impl IntoIterator<Item = (SourceId, ElementsRange)>,
        mut callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()> {
        for (idx, (source_id, range)) in requests.into_iter().enumerate() {
            let source =
                self.sources
                    .get(source_id.0)
                    .ok_or(UniversalIoError::InvalidSourceId {
                        source_id: source_id.0,
                        num_sources: self.sources.len(),
                    })?;
            let data = source.read::<SEQUENTIAL>(range)?;
            callback(idx, &data)?;
        }
        Ok(())
    }

    fn source_len(&self, source_id: SourceId) -> Result<u64> {
        let source = self
            .sources
            .get(source_id.0)
            .ok_or(UniversalIoError::InvalidSourceId {
                source_id: source_id.0,
                num_sources: self.sources.len(),
            })?;
        source.len()
    }

    fn populate(&self) -> Result<()> {
        for source in &self.sources {
            source.populate()?;
        }
        Ok(())
    }

    fn clear_ram_cache(&self) -> Result<()> {
        for source in &self.sources {
            source.clear_ram_cache()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use fs_err::OpenOptions as FsOpenOptions;
    use tempfile::Builder;

    use super::mmap::MmapUniversal;
    use super::*;
    use crate::mmap::create_and_ensure_length;

    #[test]
    fn vec_multi_universal_read_batch_and_attach() {
        let dir = Builder::new()
            .prefix("multi_universal_io")
            .tempdir()
            .unwrap();
        let path0 = dir.path().join("source0");
        let path1 = dir.path().join("source1");

        // Create two files with known content
        let data0 = b"hello ";
        let data1 = b"world";
        create_and_ensure_length(&path0, data0.len()).unwrap();
        create_and_ensure_length(&path1, data1.len()).unwrap();
        FsOpenOptions::new()
            .write(true)
            .open(&path0)
            .unwrap()
            .write_all(data0)
            .unwrap();
        FsOpenOptions::new()
            .write(true)
            .open(&path1)
            .unwrap()
            .write_all(data1)
            .unwrap();

        let opts = OpenOptions::default();
        let s0 = MmapUniversal::<u8>::open(&path0, opts).unwrap();
        let s1 = MmapUniversal::<u8>::open(&path1, opts).unwrap();

        let mut multi = VecMultiUniversalRead::new();
        let id0 = multi.attach(s0);
        let id1 = multi.attach(s1);

        assert_eq!(multi.len(), 2);
        assert_eq!(id0.0, 0);
        assert_eq!(id1.0, 1);

        let requests = [
            (
                id0,
                ElementsRange {
                    start: 0,
                    length: 5,
                },
            ),
            (
                id1,
                ElementsRange {
                    start: 1,
                    length: 3,
                },
            ),
        ];
        let mut results = Vec::new();
        multi
            .read_batch_multi::<false>(requests, |_idx, slice| {
                results.push(slice.to_vec());
                Ok(())
            })
            .unwrap();

        assert_eq!(results[0], b"hello");
        assert_eq!(results[1], b"orl");

        assert_eq!(multi.source_len(id0).unwrap(), 6);
        assert_eq!(multi.source_len(id1).unwrap(), 5);
    }
}
