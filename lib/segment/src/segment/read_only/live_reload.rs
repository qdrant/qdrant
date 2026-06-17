use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::universal_io::UniversalRead;

use super::ReadOnlySegment;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

impl<S: UniversalRead + 'static> ReadOnlySegment<S> {
    /// Refresh every component to the current on-disk state (id-tracker delta → all components).
    pub fn live_reload(&self, fs: &S::Fs, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        let result = self.id_tracker.borrow_mut().live_reload()?;

        let deleted = SortedSlice::new(&result.deleted)
            .expect("id-tracker live_reload returns sorted deleted offsets");
        let inserted = SortedSlice::new(&result.inserted)
            .expect("id-tracker live_reload returns sorted inserted offsets");

        self.payload_storage
            .borrow_mut()
            .live_reload(fs, &deleted, &inserted, hw_counter)?;
        self.payload_index
            .borrow_mut()
            .live_reload(fs, &deleted, &inserted, hw_counter)?;

        for vector_data in self.vector_data.values() {
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
