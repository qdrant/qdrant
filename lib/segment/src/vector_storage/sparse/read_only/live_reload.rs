use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};

use super::ReadOnlySparseVectorStorage;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

impl<S: UniversalRead> LiveReload for ReadOnlySparseVectorStorage<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(&self, fs: &Fs) -> OperationResult<()> {
        self.storage.live_preload(fs)?;
        // TODO(uio): use upcoming bitmap structure
        self.deleted.live_preload(fs)?;
        Ok(())
    }

    /// Reload the Blobstore, apply `deleted_points`, fold in the persisted
    /// deletion of each appended offset, and recompute `next_point_offset`.
    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage.live_reload(fs)?;
        self.deleted.insert_all(deleted_points);
        self.deleted.reload_appended::<S>(fs, new_points)?;

        self.next_point_offset = self
            .deleted
            .as_bitslice()
            .last_one()
            .map(|i| i + 1)
            .max(Some(self.storage.max_point_offset()? as usize))
            .unwrap_or_default();

        Ok(())
    }
}
