use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, PreloadFuture, UniversalRead, UniversalReadFs};

use super::VectorStorageReadEnum;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

impl<S: UniversalRead> LiveReload for VectorStorageReadEnum<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<PreloadFuture>> {
        for_all_vector_storage_read!(self, s => s.live_preload(fs))
    }

    fn apply_deletions(
        &mut self,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        for_all_vector_storage_read!(self, s => s.apply_deletions(deleted_points))
    }

    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        for_all_vector_storage_read!(self, s => s.live_reload(fs, deleted_points, new_points))
    }
}
