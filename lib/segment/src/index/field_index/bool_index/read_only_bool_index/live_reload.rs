use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, PreloadFuture, UniversalReadFs};

use super::ReadOnlyBoolIndex;
use crate::common::operation_error::OperationResult;
use crate::index::UniversalReadExt;
use crate::index::field_index::LiveReload;

impl<S: UniversalReadExt> LiveReload for ReadOnlyBoolIndex<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        cached_fs: &Fs,
    ) -> OperationResult<Vec<PreloadFuture>> {
        let mut futs = self.storage.trues_flags.live_preload(cached_fs)?;
        futs.extend(self.storage.falses_flags.live_preload(cached_fs)?);
        Ok(futs)
    }

    fn apply_deletions(
        &mut self,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        // Clear the flags of the newly deleted points: the files keep them
        self.storage
            .trues_flags
            .remove(deleted_points.iter().copied());
        self.storage
            .falses_flags
            .remove(deleted_points.iter().copied());

        // Re-derive the counts from the bitmaps, but only if they were computed
        // before: an untouched index must not pay for the bitmap scan that
        // deriving them would force.
        self.refresh_counts()
    }

    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        _new_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        // Resync each flag set from its on-disk state
        self.storage.trues_flags.live_reload(fs)?;
        self.storage.falses_flags.live_reload(fs)?;
        self.apply_deletions(deleted_points)
    }
}
