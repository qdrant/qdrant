use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::{ReadOnlySegment, ReadOnlyVectorData};
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;

impl<S: UniversalRead + 'static> ReadOnlySegment<S> {
    /// Refresh every component to the current on-disk state (id-tracker delta → all components).
    pub fn live_reload(&self, fs: &S::Fs, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        let Self {
            uuid: _,
            initial_version: _,
            version: _,
            segment_path: _,
            id_tracker,
            vector_data,
            payload_index,
            payload_storage,
            segment_type: _,
            segment_config: _,
        } = self;

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
            vector_data.live_reload(fs, &deleted, &inserted, hw_counter)?;
        }

        Ok(())
    }
}

impl<S: UniversalRead + 'static> ReadOnlyVectorData<S> {
    /// Refresh this vector's storage, index and quantized vectors to the current
    /// on-disk state.
    ///
    /// `Self` is destructured so that every component is covered: adding a field
    /// without reloading it won't compile. Each component mutates through its own
    /// `Arc<AtomicRefCell<_>>`, so `&self` is enough — no `&mut` is needed.
    fn live_reload(
        &self,
        fs: &S::Fs,
        deleted: &SortedSlice<'_, PointOffsetType>,
        inserted: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let Self {
            vector_index,
            vector_storage,
            quantized_vectors,
        } = self;

        vector_storage
            .borrow_mut()
            .live_reload(fs, deleted, inserted, hw_counter)?;
        vector_index
            .borrow_mut()
            .live_reload(fs, deleted, inserted, hw_counter)?;
        if let Some(quantized_vectors) = quantized_vectors.borrow_mut().as_mut() {
            quantized_vectors.live_reload(fs, deleted, inserted, hw_counter)?;
        }

        Ok(())
    }
}
