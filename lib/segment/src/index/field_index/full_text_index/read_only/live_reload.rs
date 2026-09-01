use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};
use futures::future::BoxFuture;

use super::ReadOnlyFullTextIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;

impl<S: UniversalRead> LiveReload for ReadOnlyFullTextIndex<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        match self {
            Self::Appendable(index) => index.live_preload(fs),
            Self::Immutable(index) => index.live_preload(fs),
            Self::OnDisk(index) => index.live_preload(fs),
        }
    }

    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            ReadOnlyFullTextIndex::Immutable(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            ReadOnlyFullTextIndex::OnDisk(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
        }
    }
}
