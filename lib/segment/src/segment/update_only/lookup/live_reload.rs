use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::universal_io::{UniversalRead, UniversalReadFs};

use super::LookupSegment;
use crate::common::live_reload::LiveReload as _;
use crate::common::operation_error::OperationResult;

impl<S: UniversalRead + 'static> LookupSegment<S> {
    /// Refresh every component to the current on-disk state (id-tracker
    /// delta → payload storage and vector storages) without re-opening the
    /// segment — the mirror of [`ReadOnlySegment::live_reload`], over the
    /// components a lookup holds.
    ///
    /// Unlike the read-only segment there is no retained delta: an error
    /// leaves the components out of step with the changes already drained
    /// from the id tracker, and the segment must be discarded rather than
    /// reloaded again.
    ///
    /// [`ReadOnlySegment::live_reload`]: crate::segment::read_only::ReadOnlySegment::live_reload
    pub fn live_reload(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let Self {
            segment_path: _,
            id_tracker,
            payload_storage,
            vector_data,
            segment_config: _,
            appendable: _,
        } = self;

        let delta = id_tracker.borrow_mut().live_reload(fs)?;

        // SAFETY: `LiveReloadResult` keeps both lists sorted ascending.
        let deleted = unsafe { SortedSlice::new_unchecked(&delta.deleted) };
        let inserted = unsafe { SortedSlice::new_unchecked(&delta.inserted) };

        payload_storage
            .borrow_mut()
            .live_reload(fs, &deleted, &inserted, hw_counter)?;
        for vector_storage in vector_data.values() {
            vector_storage
                .borrow_mut()
                .live_reload(fs, &deleted, &inserted, hw_counter)?;
        }

        Ok(())
    }
}
