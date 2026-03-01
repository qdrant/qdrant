//! Multi-source universal write interface.
//!
//! See [`MultiUniversalWrite`].

use crate::universal_io::multi_universal_read::{MultiUniversalRead, SourceId};
use crate::universal_io::{ElementOffset, Flusher, Result};

/// Interface for batched write access across multiple sources (files, S3 objects, etc.).
/// Complements [`MultiUniversalRead`]; like [`UniversalWrite`] extends [`UniversalRead`].
pub trait MultiUniversalWrite<T: Copy + 'static>: MultiUniversalRead<T> {
    /// Batch write across sources. Each request is `(SourceId, offset, data)`.
    fn write_batch_multi<'a>(
        &mut self,
        requests: impl IntoIterator<Item = (SourceId, ElementOffset, &'a [T])>,
    ) -> Result<()>;

    /// Flush all sources. Returns a flusher that flushes every attached source.
    fn flusher(&self) -> Flusher;
}
