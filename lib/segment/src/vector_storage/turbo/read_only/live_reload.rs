use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::{ReadOnlyTurboEncoded, ReadOnlyTurboMultiVectorStorage, ReadOnlyTurboVectorStorage};
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

impl<S: UniversalRead> LiveReload for ReadOnlyTurboVectorStorage<S> {
    type Fs = S::Fs;

    /// Pick up vectors a writer appended (chunked backend only; the single-file
    /// layout is immutable) and patch the in-memory deletion flags.
    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if let ReadOnlyTurboEncoded::Chunked(storage) = &mut self.storage {
            storage.live_reload(fs, deleted_points, new_points, hw_counter)?;
        }
        self.deleted.insert_all(deleted_points);
        Ok(())
    }
}

impl<S: UniversalRead> LiveReload for ReadOnlyTurboMultiVectorStorage<S> {
    type Fs = S::Fs;

    /// Pick up multivectors a writer appended (records + offsets) and patch the
    /// in-memory deletion flags.
    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage
            .live_reload(fs, deleted_points, new_points, hw_counter)?;
        self.offsets
            .live_reload(fs, deleted_points, new_points, hw_counter)?;
        self.deleted.insert_all(deleted_points);
        Ok(())
    }
}
