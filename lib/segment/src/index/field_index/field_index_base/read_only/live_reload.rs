use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use crate::common::operation_error::OperationResult;

/// Common live-reload surface shared by the read-only field-index variants.
///
/// A read-only index is opened over a [`UniversalRead`] backend while a writer
/// keeps appending to the same files. `live_reload` refreshes the in-memory
/// view to the current on-disk state without a full re-open. Implementors fall
/// into two shapes:
///
/// - immutable mmap variants only re-apply the authoritative `deleted_points`
///   to their in-memory deletion bitmap — `fs` and `new_points` are unused
///   because no on-disk state changes after build;
/// - appendable gridstore variants reload the backing storage through `fs`,
///   drop `deleted_points` from the in-memory index, then ingest `new_points`
///   from the refreshed storage view.
///
/// `deleted_points` / `new_points` are supplied by the caller (typically the
/// segment's id-tracker diff accumulated since the previous reload).
///
/// [`UniversalRead`]: common::universal_io::UniversalRead
// No in-crate implementor on the trait-only branch; the per-index impls land
// on the sibling branches that fork from here.
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
        deleted_points: &[PointOffsetType],
        new_points: &[PointOffsetType],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;
}
