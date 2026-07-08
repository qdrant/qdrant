use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::{CachedReadFs, OkNotFound, Populate, UniversalRead, UniversalReadFs};
use gridstore::error::GridstoreError;
use gridstore::{Blob, GridstoreReader};

use super::super::MapIndexKey;
use super::super::in_memory::InMemoryMapIndex;
use super::ReadOnlyAppendableMapIndex;
use crate::common::operation_error::OperationResult;

impl<N: MapIndexKey + ?Sized, S: UniversalRead> ReadOnlyAppendableMapIndex<N, S>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub fn preopen(fs: &impl CachedReadFs<File = S>, dir: PathBuf) -> OperationResult<bool> {
        // Gridstore reader
        Ok(
            GridstoreReader::<Vec<<N as MapIndexKey>::Owned>, S>::preopen(
                fs,
                dir,
                Populate::PreferBackground,
            )
            .ok_not_found()?
            .is_some(),
        )
    }

    /// Open the appendable (Gridstore) map index read-only, threading every
    /// file open through the filesystem handle `fs`.
    ///
    /// Opens a [`GridstoreReader`] over the generic filesystem object, then
    /// rebuilds the in-memory state by feeding every stored value through
    /// [`MutableMapIndexInner::ingest`] — the exact reconstruction the writable
    /// [`MutableMapIndex::open_gridstore`][1] performs over a writable
    /// [`gridstore::Gridstore`]. No write path; the reader is retained for
    /// later `files` / `clear_cache` use.
    ///
    /// Returns [`Ok(None)`] when the on-disk directory doesn't exist, matching
    /// the `create_if_missing == false` branch of the writable counterpart —
    /// the read path never creates.
    ///
    /// [1]: super::super::MutableMapIndex::open_gridstore
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: PathBuf,
    ) -> OperationResult<Option<Self>> {
        let Some(storage) = GridstoreReader::<Vec<<N as MapIndexKey>::Owned>, S>::open(
            fs,
            path,
            Populate::Blocking,
        )
        .ok_not_found()?
        else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        // Prefix support is not wired for the read-only appendable variant:
        // the gridstore carries no prefix marker, and this open path has no
        // schema access. Prefix conditions fall back to slower checks.
        let mut in_memory_index = InMemoryMapIndex::<N>::empty(false);
        let hw_counter = HardwareCounterCell::disposable();
        storage.iter::<_, GridstoreError>(
            storage.max_point_offset(),
            |idx, values: Vec<_>| {
                in_memory_index.add_many_to_map(idx, values);
                Ok(true)
            },
            hw_counter.ref_payload_index_io_write_counter(),
        )?;

        Ok(Some(Self {
            in_memory_index,
            storage,
        }))
    }
}
