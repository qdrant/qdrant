//! Multi-source storage read interface.
//!
//! See [`StorageRead`].

use std::borrow::Cow;
use std::path::Path;

use crate::universal_io::{ElementsRange, OpenOptions, Result};

/// Identifier for a source in a multi-source storage (e.g. a file, an S3 object).
/// Each multi-source storage assigns source ids to its constituent backends.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceId(pub usize);

/// Interface for batched read access across multiple sources (files, S3 objects, etc.).
/// All implementations must support attaching sources by path.
pub trait StorageRead<T: Copy + 'static> {
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
    fn attach(&mut self, path: &Path) -> Result<SourceId>;

    /// Batch read across sources. Each request is `(SourceId, ElementsRange)`.
    /// Invokes `callback(request_index, data)` for each range in order of `requests`.
    fn read_batch_multi<'a, const SEQUENTIAL: bool>(
        &'a self,
        requests: impl IntoIterator<Item = (SourceId, ElementsRange)>,
        callback: impl FnMut(usize, Cow<'a, [T]>) -> Result<()>,
    ) -> Result<()>;

    /// Read a single range from a single source.
    /// Default implementation delegates to [`read_batch_multi`](Self::read_batch_multi).
    fn read<const SEQUENTIAL: bool>(
        &self,
        source_id: SourceId,
        range: ElementsRange,
    ) -> Result<Cow<'_, [T]>> {
        let mut result = None;
        self.read_batch_multi::<SEQUENTIAL>(std::iter::once((source_id, range)), |_idx, data| {
            result = Some(data);
            Ok(())
        })?;
        Ok(result.unwrap())
    }

    fn read_whole(&self, source_id: SourceId) -> Result<Cow<'_, [T]>>;

    /// Length in elements of the given source.
    fn source_len(&self, source_id: SourceId) -> Result<u64>;

    /// Fill RAM cache for all sources, if applicable.
    fn populate(&self) -> Result<()>;

    /// Evict RAM cache for all sources, if applicable.
    fn clear_ram_cache(&self) -> Result<()>;
}
