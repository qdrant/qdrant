use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalReadFs};
use futures::future::BoxFuture;

use super::ReadOnlyBoolIndex;
use crate::common::operation_error::OperationResult;
use crate::index::UniversalReadExt;
use crate::index::field_index::LiveReload;

impl<S: UniversalReadExt> LiveReload for ReadOnlyBoolIndex<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        cached_fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        let mut futs = self.storage.trues_flags.live_preload(cached_fs)?;
        futs.extend(self.storage.falses_flags.live_preload(cached_fs)?);
        Ok(futs)
    }

    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        _deleted_points: &SortedSlice<'_, PointOffsetType>,
        _new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // Resync each flag set from its on-disk state; the point deltas are
        // irrelevant, the flag files are the source of truth.
        self.storage.trues_flags.live_reload(fs)?;
        self.storage.falses_flags.live_reload(fs)?;

        // Re-derive the counts from the just-resynced bitmaps, but only if they
        // were computed before: an untouched index must not pay for the bitmap
        // scan that deriving them would force.
        self.refresh_counts()?;

        Ok(())
    }
}
