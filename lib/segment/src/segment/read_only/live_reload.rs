use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;

use super::{ReadOnlySegment, ReadOnlyVectorData};
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;
use crate::index::UniversalReadExt;

impl<S: UniversalReadExt + 'static> ReadOnlySegment<S> {
    /// Refresh every component to the current on-disk state (id-tracker delta → all components).
    ///
    /// Draining the id-tracker advances its internal state and cannot be replayed,
    /// so the delta is accumulated into `pending_reload` and only cleared once
    /// every component has reloaded successfully. If a component fails mid-way the
    /// delta is retained, and a later reload folds in the tracker's new changes and
    /// replays the union — no component is left drifting on a partial reload.
    pub fn live_reload(
        &mut self,
        fs: &S::Fs,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let Self {
            uuid: _,
            segment_path: _,
            id_tracker,
            vector_data,
            payload_index,
            payload_storage,
            pending_reload,
            segment_type: _,
            segment_config: _,
        } = self;

        // Drain the tracker delta and fold it into whatever a previous reload left
        // unapplied. This must happen before any component reload can fail, so the
        // accumulated delta survives an error and is replayed on the next call.
        let fresh = id_tracker.borrow_mut().live_reload()?;
        let mut pending = pending_reload.borrow_mut();
        pending.merge(fresh);

        // Replay the full accumulated delta to every component. Bail on the first
        // error without clearing `pending`, so the next reload retries the union.
        {
            // SAFETY: `merge` keeps both lists sorted ascending.
            let deleted = unsafe { SortedSlice::new_unchecked(&pending.deleted) };
            let inserted = unsafe { SortedSlice::new_unchecked(&pending.inserted) };

            payload_storage
                .borrow_mut()
                .live_reload(fs, &deleted, &inserted, hw_counter)?;
            payload_index
                .borrow_mut()
                .live_reload(fs, &deleted, &inserted, hw_counter)?;

            for vector_data in vector_data.values() {
                vector_data.live_reload(fs, &deleted, &inserted, hw_counter)?;
            }
        }

        // Every component is now in sync; discard the applied delta.
        *pending = LiveReloadResult::default();

        Ok(())
    }
}

impl<S: UniversalReadExt + 'static> ReadOnlyVectorData<S> {
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
