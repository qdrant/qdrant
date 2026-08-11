use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};

use super::MultivectorOffsetsStorageChunkedRead;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;

impl<S: UniversalRead> LiveReload for MultivectorOffsetsStorageChunkedRead<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(&self, fs: &Fs) -> OperationResult<()> {
        self.data.live_preload(fs)
    }

    /// Pick up offsets a writer appended to the chunked backing.
    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.data
            .live_reload(fs, deleted_points, new_points, hw_counter)
    }
}
