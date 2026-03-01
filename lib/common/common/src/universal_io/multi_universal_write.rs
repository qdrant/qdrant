//! Multi-source universal write interface.
//!
//! See [`MultiUniversalWrite`].

use std::io::ErrorKind;
use std::path::Path;

use crate::universal_io::multi_universal_read::SourceId;
use crate::universal_io::{ElementOffset, Flusher, OpenOptions, Result, UniversalIoError};

/// Interface for batched write access across multiple sources (files, S3 objects, etc.).
/// Complements [`UniversalWrite`], which is single-source.
/// All implementations must support attaching sources by path.
pub trait MultiUniversalWrite<T: Copy + 'static> {
    /// Create an empty multi-source view with the given options (used when attaching paths).
    fn new(options: OpenOptions) -> Self
    where
        Self: Sized;

    /// Number of sources currently attached.
    fn len(&self) -> usize;

    /// True if there are no sources.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Attach a source by path. Opens it with the given options and returns its [`SourceId`].
    fn attach(&mut self, path: &Path, options: OpenOptions) -> Result<SourceId>;

    /// Batch write across sources. Each request is `(SourceId, offset, data)`.
    fn write_batch_multi<'a>(
        &mut self,
        requests: impl IntoIterator<Item = (SourceId, ElementOffset, &'a [T])>,
    ) -> Result<()>;

    /// Length in elements of the given source. Optional; default returns unsupported error.
    fn source_len(&self, source_id: SourceId) -> Result<u64> {
        let _ = source_id;
        Err(UniversalIoError::Io(std::io::Error::new(
            ErrorKind::Unsupported,
            "source_len not implemented",
        )))
    }

    /// Flush all sources. Returns a flusher that flushes every attached source.
    fn flusher(&self) -> Flusher;

    /// Fill RAM cache for all sources, if applicable.
    fn populate(&self) -> Result<()>;

    /// Evict RAM cache for all sources, if applicable.
    fn clear_ram_cache(&self) -> Result<()>;
}
