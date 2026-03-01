//! Multi-source universal read interface and minimal implementation.
//!
//! See [`MultiUniversalRead`] and [`VecMultiUniversalRead`].

use std::io::ErrorKind;

use crate::universal_io::{ElementsRange, Result, UniversalIoError, UniversalRead};

/// Identifier for a source in a multi-source storage (e.g. a file, an S3 object).
/// Each multi-source storage assigns source ids to its constituent backends.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceId(pub usize);

/// Interface for batched read access across multiple sources (files, S3 objects, etc.).
/// Complements [`UniversalRead`], which is single-source.
/// All implementations must support attaching sources dynamically.
pub trait MultiUniversalRead<T: Copy + 'static> {
    /// Type of source that can be attached.
    type Source: UniversalRead<T> + Send;

    /// Create an empty multi-source view. Use [`Self::attach`] to add sources.
    fn new() -> Self
    where
        Self: Sized;

    /// Number of sources currently attached.
    fn len(&self) -> usize;

    /// True if there are no sources.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Attach another source and return its [`SourceId`]. The new id is the current
    /// number of sources (so existing ids remain valid).
    fn attach(&mut self, source: Self::Source) -> Result<SourceId>;

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

    /// Fill RAM cache for all sources, if applicable.
    fn populate(&self) -> Result<()>;

    /// Evict RAM cache for all sources, if applicable.
    fn clear_ram_cache(&self) -> Result<()>;
}

/// Minimal multi-source read implementation: a collection of [`UniversalRead`] backends
/// addressable by [`SourceId`] (index). Supports attaching more sources at runtime.
#[derive(Debug)]
pub struct VecMultiUniversalRead<T, S> {
    sources: Vec<S>,
    _element: std::marker::PhantomData<T>,
}

impl<T: Copy + 'static, S: UniversalRead<T> + Send> VecMultiUniversalRead<T, S> {
    /// Create from an initial set of sources. Source ids will be 0, 1, 2, ...
    pub fn from_sources(sources: Vec<S>) -> Self {
        Self {
            sources,
            _element: std::marker::PhantomData,
        }
    }
}

impl<T: Copy + 'static, S: UniversalRead<T> + Send> Default for VecMultiUniversalRead<T, S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Copy + 'static, S: UniversalRead<T> + Send> MultiUniversalRead<T>
    for VecMultiUniversalRead<T, S>
{
    type Source = S;

    fn new() -> Self {
        Self {
            sources: Vec::new(),
            _element: std::marker::PhantomData,
        }
    }

    fn len(&self) -> usize {
        self.sources.len()
    }

    fn attach(&mut self, source: S) -> Result<SourceId> {
        let id = SourceId(self.sources.len());
        self.sources.push(source);
        Ok(id)
    }

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

    use super::*;
    use crate::mmap::create_and_ensure_length;
    use crate::universal_io::OpenOptions;
    use crate::universal_io::mmap::MmapUniversal;

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
        let id0 = multi.attach(s0).unwrap();
        let id1 = multi.attach(s1).unwrap();

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
