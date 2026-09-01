use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UniversalReadFs};
use futures::future::BoxFuture;

use super::ReadOnlyPayloadStorage;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

impl<S: UniversalRead> LiveReload for ReadOnlyPayloadStorage<S> {
    type File = S;

    fn live_preload<Fs: common::universal_io::CachedReadFs<File = Self::File>>(
        &self,
        cached_fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        Ok(self.storage.live_preload(cached_fs)?)
    }

    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        _deleted_points: &SortedSlice<'_, PointOffsetType>,
        _new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Ok(self.storage.live_reload(fs)?)
    }
}
