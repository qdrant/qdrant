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
    /// layout is immutable), patch the in-memory deletion flags, and fold in the
    /// persisted deletion of each appended offset — a live point may have a
    /// deleted vector slot recorded only on disk.
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
        self.deleted.reload_appended::<S>(fs, new_points)?;
        Ok(())
    }
}

impl<S: UniversalRead> LiveReload for ReadOnlyTurboMultiVectorStorage<S> {
    type Fs = S::Fs;

    /// Pick up multivectors a writer appended (records + offsets), patch the
    /// in-memory deletion flags, and fold in the persisted deletion of each
    /// appended offset — a live point may have a deleted vector slot recorded
    /// only on disk.
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
        self.deleted.reload_appended::<S>(fs, new_points)?;
        Ok(())
    }
}
