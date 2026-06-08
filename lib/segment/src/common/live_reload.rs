use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;

use crate::common::operation_error::OperationResult;

/// Common live-reload surface shared by the read-only, gridstore-backed stores —
/// the read-only field-index variants and the read-only payload storage.
///
/// A read-only store is opened over a [`UniversalRead`] backend while a writer
/// keeps appending to the same files. `live_reload` refreshes the in-memory
/// view to the current on-disk state without a full re-open. Implementers fall
/// into a few shapes:
///
/// - immutable mmap field-index variants only re-apply the authoritative
///   `deleted_points` to their in-memory deletion bitmap — `fs` and
///   `new_points` are unused because no on-disk state changes after build;
/// - appendable gridstore field-index variants reload the backing storage
///   through `fs`, drop `deleted_points` from the in-memory index, then ingest
///   `new_points` from the refreshed storage view;
/// - stores with no separate in-memory index (e.g. the payload storage) only
///   reload the backing storage through `fs`; deletions and newly written
///   points are served straight from the refreshed gridstore, so
///   `deleted_points` / `new_points` are unused.
///
/// `deleted_points` / `new_points` are supplied by the caller (typically the
/// segment's id-tracker diff accumulated since the previous reload).
///
/// [`UniversalRead`]: common::universal_io::UniversalRead
#[allow(dead_code)]
pub(crate) trait LiveReload {
    /// Filesystem context (`S::Fs` of the backing [`UniversalRead`]) used to
    /// re-read on-disk state during a reload.
    ///
    /// [`UniversalRead`]: common::universal_io::UniversalRead
    type Fs;

    fn live_reload(
        &mut self,
        fs: &Self::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;
}
