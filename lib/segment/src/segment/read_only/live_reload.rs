use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::universal_io::UniversalRead;

use super::ReadOnlySegment;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;

impl<S: UniversalRead + 'static> ReadOnlySegment<S> {
    /// Refresh every component to the current on-disk state (id-tracker delta → all components).
    pub fn live_reload(&self, fs: &S::Fs, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        let Self {
            id_tracker,
            initial_version: _,
            payload_index,
            payload_storage,
            segment_config: _,
            segment_path: _,
            segment_type: _,
            uuid: _,
            vector_data,
            version: _,
        } = &self;
        let LiveReloadResult { inserted, deleted } = id_tracker.borrow_mut().live_reload()?;

        // SAFETY: id-tracker live_reload returns sorted offsets
        let deleted = unsafe { SortedSlice::new_unchecked(&deleted) };
        let inserted = unsafe { SortedSlice::new_unchecked(&inserted) };

        payload_storage
            .borrow_mut()
            .live_reload(fs, &deleted, &inserted, hw_counter)?;
        payload_index
            .borrow_mut()
            .live_reload(fs, &deleted, &inserted, hw_counter)?;

        for vector_data in vector_data.values() {
            vector_data
                .vector_storage
                .borrow_mut()
                .live_reload(fs, &deleted, &inserted, hw_counter)?;
            vector_data
                .vector_index
                .borrow_mut()
                .live_reload(fs, &deleted, &inserted, hw_counter)?;
            if let Some(quantized_vectors) = vector_data.quantized_vectors.borrow_mut().as_mut() {
                quantized_vectors.live_reload(fs, &deleted, &inserted, hw_counter)?;
            }
        }

        Ok(())
    }
}
