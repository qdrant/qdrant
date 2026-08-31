use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalReadFs};
use futures::future::BoxFuture;

use super::ReadOnlyImmutableTurboVectorStorage;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;
use crate::vector_storage::turbo::turbo_vectors::TurboVectorBlob;

impl<B: TurboVectorBlob> LiveReload for ReadOnlyImmutableTurboVectorStorage<B> {
    type File = B::File;

    fn live_preload<Fs: CachedReadFs<File = B::File>>(
        &self,
        _fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        // No new data to fetch, deletions are applied directly from arguments
        Ok(Vec::new())
    }

    /// Vector data is immutable, so only the in-memory deletion flags are patched
    /// from the authoritative `deleted_points`; `fs` and `new_points` are unused.
    fn live_reload<Fs: UniversalReadFs<File = B::File>>(
        &mut self,
        _fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        _new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.deleted.insert_all(deleted_points);
        Ok(())
    }
}
