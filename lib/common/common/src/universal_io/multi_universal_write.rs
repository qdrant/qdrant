//! Multi-source storage write interface.
//!
//! See [`StorageWrite`].

use crate::universal_io::multi_universal_read::{SourceId, StorageRead};
use crate::universal_io::{ElementOffset, Flusher, Result};

/// Interface for batched write access across multiple sources (files, S3 objects, etc.).
/// Extends [`StorageRead`] with write capabilities.
pub trait StorageWrite<T: Copy + 'static>: StorageRead<T> {
    /// Batch write across sources. Each request is `(SourceId, offset, data)`.
    fn write_batch_multi<'a>(
        &mut self,
        requests: impl IntoIterator<Item = (SourceId, ElementOffset, &'a [T])>,
    ) -> Result<()>;

    /// Flush all sources. Returns a flusher that flushes every attached source.
    fn flusher(&self) -> Flusher;
}
